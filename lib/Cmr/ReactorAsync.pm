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

package Cmr::ReactorAsync;

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
use Cmr::Reactor ();

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
    FINISH => 3,
    SCRAM => 4,
    END_THREAD => 12983,
};

our $parent_handlers = {
    &TASK_FINISHED   => \&_task_finished,
    &THREAD_FINISHED => \&_thread_finished,
};

sub init;
sub push;
sub finish;
sub scram;

sub init {
    my ($handlers, $config, $thread_init) = @_;
    my $backlog  = Thread::Queue->new;
    my $configref = \$config;
    my $ev = Cmr::Reactor::init( $handlers, $configref, $thread_init, $backlog );

    my $dispatcher = bless {
        'reactor'       => $ev,
        'queue'         => Thread::Queue->new,
        'backlog'       => $backlog,
    };

    $dispatcher->{'dispatch'} = threads->create(\&dispatch_main, {
        'reactor'       => $ev,
        'configref'     => $configref,
        'queue'         => $dispatcher->{'queue'},
        'thread_init'   => $thread_init,
    });

    return $dispatcher;
}

sub push {
    my ($self, $task) = @_;
    $task->{&MAGIC} = TASK;
    $self->{'backlog'}->enqueue($task);
}

sub finish {
    my ($self) = @_;
    $self->{'queue'}->enqueue({ &MAGIC => FINISH });
    $self->{'dispatch'}->join();
}

sub scram {
    my ($self) = @_;
    $self->{'queue'}->enqueue({ &MAGIC => SCRAM });
    $self->{'dispatch'}->join();
}

sub pending {
    my ($self) = @_;
    return $self->{'reactor'}->pending();
}

sub dispatch_main {
    my ($args) = @_;

    my $configref      = $args->{'configref'};
    my $reactor     = $args->{'reactor'};
    my $queue       = $args->{'queue'};
    my $thread_init = $args->{'thread_init'};

    my $nano_interval = ($$configref->{'dispatch_interval'} // 0.1) * 1e9;

my $log = Cmr::StartupUtils::get_logger();
    while ( !$reactor->{'finished'} ) {
        if (Cmr::StartupUtils::load_config($configref)) {
            $nano_interval = ($$configref->{'dispatch_interval'} // 0.1) * 1e9;
            $reactor->resize();            
        }
       
        my $cmd = $queue->dequeue_nb;
        if ($cmd) {
            if      ( $cmd->{&MAGIC} == FINISH ) {
                $reactor->finish();
            } elsif ( $cmd->{&MAGIC} == SCRAM ) {
                $reactor->scram();
                last;
            }
        }
        $reactor->dispatch();
        Time::HiRes::nanosleep($nano_interval);
    }
}

1;
