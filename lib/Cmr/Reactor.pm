#
#   Copyright (C) 2014 Chitika Inc.
#
#   This file is a part of Cmr
#
#   Cmr is free software: you can redistribute it and/or modify
#   it under the terms of the GNU General Public License as published by
#   the Free Software Foundation, either version 3 of the License, or
#   (at your option) any later version.
#
#   This program is distributed in the hope that it will be useful,
#   but WITHOUT ANY WARRANTY; without even the implied warranty of
#   MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
#
#   GNU General Public License for more details.
#
#   You should have received a copy of the GNU General Public License
#   along with this program.  If not, see <http://www.gnu.org/licenses/>.
#

package Cmr::Reactor;

## Threaded event reactor

our $VERSION = '0.1';

use strict;
use warnings;

use threads ();
use threads::shared;

use Thread::Queue ();
use POSIX ();
use Time::HiRes ();

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__));

use Cmr::StartupUtils ();

use constant {
    ID              => 0,
    TYPE            => 1,
};

use constant {
    TASK_FINISHED   => 0,
    THREAD_FINISHED => 1,
};

use constant {
    MAGIC => 1768623,
};

use constant {
    TASK => 1,
    CONFIG_CHANGED => 2,
    END_THREAD => 12983,
};

our $parent_handlers = {
    &TASK_FINISHED   => \&task_finished,
    &THREAD_FINISHED => \&thread_finished,
};

sub init;
sub dispatch;
sub resize;
sub pending;
sub schedule_tasks;
sub task_finished;
sub thread_finished;
sub stop_threads;
sub start_threads;

sub thread_main {
    my ($args) = @_;

    my $backlog = $args->{'backlog'};
    my $parent_queue = $args->{'parent_queue'};
    my $queue = $args->{'queue'};
    my $id = $args->{'id'};
    my $handlers = $args->{'handlers'};
    my $config = $args->{'config'};

    my $self = {
        'id'            => $id,
        'backlog'       => $backlog,
    };

    $args->{'thread_init'}->($self, $config) if $args->{'thread_init'};

    while(1) {
        my $task = $queue->dequeue;
        if ( !$task || !$task->{&MAGIC} ) {
           $parent_queue->enqueue({ID=>$id, TYPE=>TASK_FINISHED});
           next;
        }

        if ( $task->{&MAGIC} == TASK ) {
            $task->{'backlog'} = $backlog;
            $handlers->{$task->{'task'}}->($task, $config, $self) if exists($handlers->{$task->{'task'}});
            $parent_queue->enqueue({ID=>$id, TYPE=>TASK_FINISHED});
        }
        elsif ( $task->{&MAGIC} == CONFIG_CHANGED ) {
            $config = $task->{'config'};
            next;
        }
        elsif ( $task->{&MAGIC} == END_THREAD  ) {
            last;
        }
    }
    $parent_queue->enqueue({ID=>$id, TYPE=>THREAD_FINISHED});
}

sub init {
    my ($handlers, $configref, $thread_init, $backlog) = @_;

    my $running_tasks : shared = 0;

    my $worker_ctx = bless {
        'initialized'        => 0,
        'backlog'            => $backlog // Thread::Queue->new,
        'parent_queues'      => [],
        'handlers'           => $handlers,
        'running_tasks'      => \$running_tasks,
        'running_threads'    => 0,
        'config'             => $configref,
        'thread_init'        => $thread_init,
        'max_threads'        => 4,
        'tasks_per_thread'   => 10,
        'max_thread_backlog' => 20,
        'stopping'           => 0,
        'finished'           => 0,
        'scram'              => 0,
        'threads'            => {},
        'dead_workers'       => 0,
        'rr_index'           => 0,
    };

    $worker_ctx->resize();

    return $worker_ctx;
}

our $evil_config;
sub no_config_init {
    my ($handlers, $max_threads, $tasks_per_thread, $max_thread_backlog, $thread_init) = @_;

    $evil_config = {
        'max_threads'        => $max_threads || 4,
        'tasks_per_thread'   => $tasks_per_thread || 10,
        'max_thread_backlog' => $max_thread_backlog || 20,
    };

    my $running_tasks : shared = 0;

    my $worker_ctx = bless {
        'initialized'        => 0,
        'backlog'            => Thread::Queue->new,
        'parent_queues'      => [],
        'handlers'           => $handlers,
        'running_tasks'      => \$running_tasks,
        'running_threads'    => 0,
        'config'             => \$evil_config,
        'thread_init'        => $thread_init,
        'max_threads'        => 4,
        'tasks_per_thread'   => 10,
        'max_thread_backlog' => 20,
        'stopping'           => 0,
        'finished'           => 0,
        'scram'              => 0,
        'threads'            => {},
        'dead_workers'       => 0,
        'rr_index'           => 0,
    };

    $worker_ctx->resize();

    return $worker_ctx;
}


sub sync_config {
    my ($self, $config) = @_;
    $self->{'config'} = $config;

    my $max_threads = $self->{'max_threads'};
    for my $id ( keys %{$self->{'threads'}} )
    {
        my $thread = $self->{'threads'}->{$id};
        next if ( $id >= $max_threads || $thread->{'stopping'} );
        $thread->{'queue'}->insert(0, { &MAGIC => CONFIG_CHANGED, 'config' => $$config } );
    }
}

sub resize {
    my ($self, $max_threads, $tasks_per_thread, $max_thread_backlog) = @_;

    if ($self->{'scram'}) { return; }

    my $config = ${$self->{'config'}};

    $config->{'max_threads'}        ||= 4;
    $config->{'tasks_per_thread'}   ||= 10;
    $config->{'max_thread_backlog'} ||= 20;

    if ( $max_threads )        { $config->{'max_threads'} = $max_threads; }
    if ( $tasks_per_thread )   { $config->{'tasks_per_thread'} = $tasks_per_thread; }
    if ( $max_thread_backlog ) { $config->{'max_thread_backlog'} = $max_thread_backlog; }

    $self->{'max_threads'}        = $config->{'max_threads'};
    $self->{'tasks_per_thread'}   = $config->{'tasks_per_thread'};
    $self->{'max_thread_backlog'} = $config->{'max_thread_backlog'};

    if ( $config->{'tasks_per_thread'} > $config->{'max_thread_backlog'} ) {
        warn "Tasks per thread exceeds maximum allowed thread backlog, defaulting to a max backlog of ".($config->{'tasks_per_thread'}+5);
    }

    $max_threads =  $self->{'max_threads'};

    if ($self->{'running_threads'} > $max_threads) {
        stop_threads($self, {
            'max_threads'  => $max_threads,
        });
    } elsif ( $self->{'running_threads'} < $max_threads ) {
        start_threads(
            $self,
            {
                'parent_queue' => $self->{'parent_queue'},
                'max_threads'  => $max_threads,
                'handlers'     => $self->{'handlers'},
                'thread_init'  => $self->{'thread_init'},
                'config'       => $config,
            }
        );
    }

    $self->sync_config($self->{'config'});
}

sub dispatch {
    my ($self) = @_;
    foreach my $parent_queue (@{$self->{'parent_queues'}}) {
        while ( my @new_events = $parent_queue->dequeue_nb(100) ) {
            foreach (@new_events) {
                $parent_handlers->{$_->{TYPE}}->($self,$_);
            }
        }
    }

    my $resize_required;
    for my $thread_id (keys %{$self->{'threads'}}) {
        if ( $self->{'threads'}->{$thread_id}->{'thread'}->error() ) {
            $self->{'threads'}->{$thread_id}->{'thread'}->join;
            my $pending = $self->{'threads'}->{$thread_id}->{'queue'}->pending();
            if ( $pending )
            { lock ${$self->{'running_tasks'}};
              ${$self->{'running_tasks'}} -= $pending;
              $self->{'backlog'}->insert(0, $self->{'threads'}->{$thread_id}->{'queue'}->dequeue( $pending ));
            } # unlock ${$self->{'running_tasks'}}
            delete $self->{'threads'}->{$thread_id};
            $self->{'running_threads'}--;
            $self->{'dead_workers'}++;
            if (!$self->{'scram'} && $self->{'dead_workers'} >= $self->{'max_threads'}) {
                print STDERR "Reactor deteonated! Too many task failures...\n";
                $self->scram();
                return -1;
            }
            $resize_required = 1;
        }
    }
    if ($resize_required && !$self->{'finished'} ) { $self->resize(); }

    return schedule_tasks($self);
}

sub schedule_tasks {
    my ($self) = @_;
    if ( $self->{'backlog'}->pending() == 0 ) { return 0; }
    my $num_threads = $self->{'running_threads'};

    if ($num_threads == 0) {
        return 0;
    }

    my $max_tasks   = $num_threads * $self->{'tasks_per_thread'};
    my $scheduled = 0;

    if ( ${$self->{'running_tasks'}} < $max_tasks ) {
         
        my $max_dequeue = $max_tasks - ${$self->{'running_tasks'}};
        my $num_tasks = $max_dequeue > $self->{'backlog'}->pending() ? $self->{'backlog'}->pending() : $max_dequeue;
        if ( !$num_tasks ) {
            return 0; # short circuit on nothing to schedule
        }

        my @new_tasks;
        { lock $self->{'running_tasks'};
          ${$self->{'running_tasks'}} += $num_tasks;
          @new_tasks = $self->{'backlog'}->dequeue_nb($num_tasks);
        } # unlock $self->{'running_tasks'};

        my $left = $num_tasks;
        my $pending;
        my $lower_bound = $self->{'max_thread_backlog'};

        # Establish a lower bound on the queue sizes for each thread
        for my $index (keys %{$self->{'threads'}}) {
            $pending->{$index} = $self->{'threads'}->{$index}->{'queue'}->pending();
            $lower_bound = $pending->{$index} < $lower_bound ? $pending->{$index} : $lower_bound;
        }

        # Calculate a value for even distribution across all threads
        my $even = POSIX::ceil($num_tasks / $num_threads) + $lower_bound;


        # Distribute evenly (this will skip over threads that are already overloaded with work)
        for my $index (keys %{$self->{'threads'}}) {
            my $used = $even - $pending->{$index};
            $used = ($used < $left) ? $used : $left;
            if ( $used > 0 ) {
                $self->{'threads'}->{$index}->{'queue'}->enqueue( @new_tasks[$scheduled..($scheduled+$used-1)] );
                $scheduled += $used;
                $pending->{$index} += $used;
                $left -= $used;
            }
        }

        # Distribute any remaining tasks round robin
        while( $left ) {

            $self->{'rr_index'} = ($self->{'rr_index'}) % $num_threads;
            do {
                $self->{'rr_index'} = ($self->{'rr_index'}+1) % $num_threads;
            } while ( $pending->{$self->{'rr_index'}} >= $self->{'max_thread_backlog'} );
            
            $self->{'threads'}->{$self->{'rr_index'}}->{'queue'}->enqueue( $new_tasks[$scheduled] );
            $pending->{$self->{'rr_index'}} += 1;
            $scheduled            += 1;
            $left                 -= 1;
        }
    }

    return $scheduled;
}

sub push {
    my ($self, $task) = @_;
    $task->{&MAGIC} = TASK;
    $self->{'backlog'}->enqueue( $task );
}

sub pending {
    my ($self) = @_;
    my $pending = 0;
    { lock ${$self->{'running_tasks'}};
      $pending = $self->{'backlog'}->pending() + ${$self->{'running_tasks'}};
    } # unlock ${$self->{'running_tasks'}}
    return $pending;
}

sub finish {
    my ($self) = @_;
    if ($self->{'stopping'} && $self->{'finished'} || $self->{'scram'}) { return; }
    $self->{'finished'} = 1;
    while($self->pending() > 0 && !$self->{'scram'} == 1) {
        $self->dispatch();
        Time::HiRes::usleep(100000);
    }
    scram($self);
}

sub scram {
    my ($self) = @_;
    if ($self->{'scram'}) { return; }
    $self->{'stopping'} = 1;
    $self->{'scram'} = 1;
    stop_threads($self, {'max_threads'=>0});
    while($self->{'running_threads'} > 0) {
        $self->dispatch();
        Time::HiRes::usleep(100000);
    }
    $self->{'finished'} = 1;
}

sub task_finished {
    my ($self) = @_;
    { lock ${$self->{'running_tasks'}};
      ${$self->{'running_tasks'}}--;
    } # unlock ${$self->{'running_tasks'}}
}

sub thread_finished {
    my ($self, $event) = @_;

    $self->{'threads'}->{$event->{ID}}->{'thread'}->join;
    my $pending = $self->{'threads'}->{$event->{ID}}->{'queue'}->pending();
    if ( $pending )
    { lock ${$self->{'running_tasks'}};
        ${$self->{'running_tasks'}} -= $pending;
        $self->{'backlog'}->insert(0, $self->{'threads'}->{$event->{ID}}->{'queue'}->dequeue( $pending ));
    } # unlock ${$self->{'running_tasks'}}
    delete $self->{'threads'}->{$event->{ID}};
    $self->{'running_threads'}--;
}

sub start_threads {
    my ($self, $args) = @_;

    next if $self->{'stopping'};

    my $max_threads     = $args->{'max_threads'};
    my $handlers        = $args->{'handlers'};
    my $thread_init     = $args->{'thread_init'};

    for (my $id = $self->{'running_threads'}; $id < $max_threads; $id++)
    {
        my $thread_queue = Thread::Queue->new;
        my $parent_queue = Thread::Queue->new;
        CORE::push @{$self->{'parent_queues'}}, $parent_queue;

        $self->{'threads'}->{$id} = {
            'thread' => threads->create(\&thread_main, {
                'backlog'       => $self->{'backlog'},
                'parent_queue'  => $parent_queue,
                'queue'         => $thread_queue,
                'id'            => $id,
                'handlers'      => $handlers,
                'thread_init'   => $thread_init,
                'config'        => ${$self->{'config'}},
            }),
            'queue'     => $thread_queue,
            'stopping'  => 0,
        };

        if ($self->{'backlog'}->pending() > 0)
        { lock ${$self->{'running_tasks'}};
            my @new_tasks = $self->{'backlog'}->dequeue_nb($self->{tasks_per_thread});
            $self->{'threads'}->{$id}->{'queue'}->enqueue( @new_tasks );
            ${$self->{'running_tasks'}} += scalar (@new_tasks);
        } # unlock ${$self->{'running_tasks'}}
    }
    $self->{'running_threads'} = $max_threads;
}

sub stop_threads {
    my ($self, $args) = @_;

    my $max_threads = $args->{'max_threads'};

    for my $id ( keys %{$self->{'threads'}} )
    {
        my $thread = $self->{'threads'}->{$id};
        next if ( $id < $max_threads || $thread->{'stopping'} );
        $thread->{'stopping'} = 1;
        my $task =  { &MAGIC => END_THREAD };
        $thread->{'queue'}->insert(0, $task);
    }
}

1;
