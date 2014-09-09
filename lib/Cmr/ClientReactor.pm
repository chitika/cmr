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

package Cmr::ClientReactor;

use strict;
use warnings;

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__));

use Cmr::StartupUtils ();
use Cmr::Types ();

use JSON::XS;
use UUID ();
use NanoMsg::Raw;
use threads ();
use threads::shared;
use Thread::Queue;
use Time::HiRes ();
use Digest::MD5 qw(md5_hex);

select(STDERR);
$| = 1;
select(STDOUT);
$| = 1;

sub finish_task;
sub resubmit_task;

my $log;
my $task_timeout = 300;
my $resubmit_check_interval = 20;

my $completion_handlers = {
    &Cmr::Types::CMR_RESULT_SUCCESS          => \&finish_task,
    &Cmr::Types::CMR_RESULT_ACCEPT           => \&accept_task,
    &Cmr::Types::CMR_RESULT_TIMEOUT          => \&resubmit_task,
    &Cmr::Types::CMR_RESULT_FAILURE          => \&resubmit_task,
    &Cmr::Types::CMR_RESULT_WORKER_REJECT    => \&resubmit_task,
    &Cmr::Types::CMR_RESULT_SERVER_REJECT    => \&resubmit_task,
    &Cmr::Types::CMR_RESULT_CONNECT          => \&connect,
    &Cmr::Types::CMR_RESULT_DISCONNECT       => \&disconnect,
    &Cmr::Types::CMR_RESULT_STATUS           => \&status,
    &Cmr::Types::CMR_RESULT_SERVER_STATUS    => \&server_status,
    &Cmr::Types::CMR_RESULT_JOB_FAILURE      => \&fail_job,
    &Cmr::Types::CMR_RESULT_MISSING_OUT_PATH => \&fail_job,
    &Cmr::Types::CMR_RESULT_MISSING_ERR_PATH => \&fail_job,
};

sub new {
    my ($config) = @_;

    $log = Cmr::StartupUtils::get_logger();

    my ($user, undef, undef, undef, undef, undef, undef, undef, undef) = getpwuid($<);
    if (!$user) { die "failed to get user - LDAP lookup failed? *shrug*"; }
    $config->{'USER'} = $user;

    my $pid = $$;
    $config->{'PID'} = $pid;

    UUID::generate(my $uuid);
    UUID::unparse($uuid, my $jid);
    $config->{'JOB_ID'} = $jid;

    unless ($config->{'basepath'}) { die "No basepath configured"; }
    my $rc = opendir(my $dir, $config->{'basepath'});
    if (!$rc) { die "basepath check failed"; }
    my @contents =  grep (!/^\./o, readdir($dir));
    closedir($dir);
    die "basepath is empty... datapocalypse?" unless @contents;

    my $basepath      = $config->{'basepath'};
    my $re_basepath   = qr/$basepath/;
    my $destination   = $config->{'output'};

    for my $path ( ('scratch_path','output_path','error_path','bundle_path') ) {
        unless ($config->{$path}) { die "No $path configured"; }
        my @replace_fields = $config->{$path} =~ /\$\{([^\}]*)\}/g;
        for my $field (@replace_fields) {
            if ( exists $config->{$field} ) {
                $config->{$path} =~ s/\${${field}}/$config->{$field}/g;
            }
        }
    }

    if ( $destination and not $config->{'stdout'} ) {
        if ( not $config->{'base-relative-path'} and not $destination =~ /^$re_basepath/ ) {
            # base-relative-path is off, and the user specified a destination
            # outside of the basepath...
            # It can't be assumed that this path is mounted on the workers
            # So we're going to write to the default output path, and afterwards
            # copy in to the specified path.

            $config->{'output_hackery'} = 1;
            $config->{'final_destination'} = $destination;
            if ( -e $config->{'final_destination'} and $config->{'force'} ) {
                system("rm -rf $config->{'final_destination'}");
                sleep(1);
            }
            elsif ( -e $config->{'final_destination'} ) {
                print STDERR "output path $config->{'final_destination'} already exists\n";
                exit(1);
            }
        }

        if ( !$config->{'output_hackery'} ) {
            # path might be absolute, or relative... doesn't matter, strip the basepath off
            # and create a relative path

            $destination =~ s/^$re_basepath//;
            my $output_path = "${basepath}/${destination}";
            $output_path =~ s/\/+/\//g;
            $config->{'output_path'} = $output_path;

            if ( -e $config->{'output_path'} and $config->{'force'} ) {
                system("rm -rf $config->{'output_path'}");
                sleep(1);
            }
            elsif ( -e $config->{'output_path'} ) {
                print STDERR "output path $config->{'output_path'} already exists\n";
                exit(1);
            }
        }

    }

    unless ( $config->{'no_output_dir'} ) {
        system("mkdir -p $config->{'output_path'}");
        system("mkdir -p $config->{'error_path'}");
    }

    if ( $config->{'bundle'} ) {
        system("mkdir -p $config->{'bundle_path'}");

        for my $file (@{$config->{'bundle'}}) {
            unless (-e $file) {
                print STDERR "Failed to locate bundled file: ${file}\n";
                exit(1);
            }

            my ($filename) = $file =~ /([^\/]*)$/o;
            system("cp -p ${file} $config->{'bundle_path'}/${filename}");

            $config->{'mapper'}  =~ s|${file}|$config->{'bundle_path'}/${filename}|g if $config->{'mapper'};
            $config->{'reducer'} =~ s|${file}|$config->{'bundle_path'}/${filename}|g if $config->{'reducer'};
        }
    }


    print STDERR "Job id is $jid\n" if $config->{'verbose'};

    $config->{'max_task_errors'} //= 16;

    my $obj = {
        'config'                => $config,
        'finished'              => 0,
        'failed'                => 0,
        'scratch_path'          => $config->{'scratch_path'},
        'output_path'           => $config->{'output_path'},
        'error_path'            => $config->{'error_path'},
        'bundle_path'           => $config->{'bundle_path'},
        're_basepath'           => $re_basepath,
        'num_task_errors'       => 0,
        'num_tasks_submitted'   => 0,
        'num_tasks_completed'   => 0,
        'lock'                  => 0,
        'job_output'            => [],
        'submitted'             => {},
        'user'                  => $user,
        'jid'                   => $jid,
        'pid'                   => $pid,
        'backlog'               => Thread::Queue->new,
        'warnings'              => 0,
    };

    share($obj->{'lock'});
    share($obj->{'finished'});
    share($obj->{'failed'});
    share(@{$obj->{'job_output'}});
    share($obj->{'num_task_errors'});
    share($obj->{'num_tasks_submitted'});
    share($obj->{'num_tasks_completed'});
    share($obj->{'warnings'});

    $obj->{'thread'} = threads->create(\&thread_main, $obj),

    return bless($obj);
}


sub failed {
    my ($self) = @_;
    return $self->{'failed'};
}


sub sync {
    my ($self) = @_;
    if ($self->{'finished'} or $self->{'failed'}) {
        my $log = Cmr::StartupUtils::get_logger();
        $log->debug("failed sync - cmr instance is finished");
        return;
    }

    while ( ! &failed($self) ) {
        { lock($self->{'backlog'});
            return if $self->{'backlog'}->pending() == 0 and $self->{'num_tasks_completed'} >= $self->{'num_tasks_submitted'};
        } # unlock
        Time::HiRes::nanosleep(0.001*1e9);
    }
}


sub get_job_output {
    my ($self) = @_;
    my @output;

    { lock $self->{'lock'};
        @output = @{$self->{'job_output'}};
    }

    return @output;
}


sub clear_job_output {
    my ($self) = @_;

    { lock $self->{'lock'};
        @{$self->{'job_output'}} = ();
    }
}


sub scram {
    my ($self) = @_;
    $self->{'failed'} = 1;
    finish($self);
}


sub finish {
    my ($self) = @_;

    if (!$self->{'finished'}) {
        $self->{'finished'} = 1;
        $self->{'thread'}->join();
    }

    if ( $self->{'num_task_errors'} > $self->{'config'}->{'max_task_errors'} ) {
        my $excess_errors = $self->{'num_task_errors'} - $self->{'config'}->{'max_task_errors'};
        print STDERR "Suppressed ${excess_errors} errors due to exceeding configured max_task_errors ($self->{'config'}->{'max_task_errors'})\n";
        if ($self->{'err_fd'}) {
            my $fd = $self->{'err_fd'};
            print $fd    "Suppressed ${excess_errors} errors due to exceeding configured max_task_errors ($self->{'config'}->{'max_task_errors'})\n";
        }
    }

    if ($self->{'err_fd'}) {
        close($self->{'err_fd'});
    }

    if ( $self->{'config'}->{'no_output_dir'} ) {
        # Special case - no output generated
    }
    elsif ( not $self->{'config'}->{'output'} or $self->{'config'}->{'stdout'} ) {
        # Output to stdout
        system( qq(chunky -s 4 $self->{'config'}->{'output_path'}/* ) );
        system( qq(rm -rf $self->{'config'}->{'output_path'}) );
    }
    elsif ( $self->{'config'}->{'output_hackery'} ) {
        # output path is outside basepath
        system( qq(cp -a $self->{'config'}->{'output_path'} $self->{'config'}->{'final_destination'}) );
        system( qq(rm -rf $self->{'config'}->{'output_path'}) );
        print STDERR "output is in: $self->{'config'}->{'final_destination'}\n";
    }
    else {
        # output path is inside basepath
        print STDERR "output is in: $self->{'config'}->{'output_path'}\n";
    }

    # Always remove the bundle path
    system( qq(rm -rf $self->{'config'}->{'bundle_path'}) );

    if ( -e $self->{'error_path'} ) {
        # Remove the error path if it is empty
        system( "find '$self->{'error_path'}' -maxdepth 0 -empty -exec rmdir  {} \\;" );
    }
}


sub push {
    my ($self, $task) = @_;

    if ($self->{'finished'} or $self->{'failed'}) {
        my $log = Cmr::StartupUtils::get_logger();
        $log->debug("failed push - cmr instance is finished");
        return;
    }

    while ( $self->{'config'}->{'client_queue_size'} // 1000 <= (
            $self->{'backlog'}->pending()
           +$self->{'num_tasks_submitted'}
           -$self->{'num_tasks_completed'} ) ) 
    {
        Time::HiRes::nanosleep(0.001*1e9);
        return if &failed($self);
    }

    if ('input' ~~ $task && $task->{'input'}) {
        if ( $task->{'input'}->[0] =~ /\.gz$/o ) {
            $task->{'in_fmt_cmd'} = "pigz -dc";
        }
        for my $i (0..$#{$task->{'input'}}) {
            $task->{'input'}->[$i] =~ s/^$self->{'re_basepath'}//o;
        }
    }

    $task->{'failures'} = 0;

    if ($task->{'destination'}) {
        $task->{'destination'} =~ s/^$self->{'re_basepath'}//o;
    }

    $task->{'accept_timeout'} //= $self->{'config'}->{'accept_timeout'} // $self->{'config'}->{'accept_timeout'} // 10;
    $task->{'timeout'}        //= $self->{'config'}->{'task_timeout'}   // $self->{'config'}->{'task_timeout'}   // 300;
    $self->{'backlog'}->enqueue( $task );
}


sub thread_main {
    my ($self) = @_;

    my $log = Cmr::StartupUtils::get_logger();

    my $config = $self->{'config'};
    my $backlog = $self->{'backlog'};
    my $jid = $self->{'jid'};
    my $pid = $self->{'pid'};
    my $user = $self->{'user'};
    my $task_id = 0;

    my $s_server = nn_socket(AF_SP, NN_REP);
    nn_connect($s_server, $config->{'server_in'});
    nn_setsockopt($s_server, NN_SOL_SOCKET, NN_RCVTIMEO, 0);

    my $s_caster_in = nn_socket(AF_SP, NN_PUSH);
    nn_connect($s_caster_in, $config->{'caster_in'});
    nn_setsockopt($s_caster_in, NN_SOL_SOCKET, NN_SNDBUF, 4*1024*1024);


    my $s_caster_out = nn_socket(AF_SP, NN_SUB);
    nn_connect($s_caster_out, $config->{'caster_out'});
    nn_setsockopt($s_caster_out, NN_SOL_SOCKET, NN_RCVTIMEO, 0);
    nn_setsockopt($s_caster_out, NN_SOL_SOCKET, NN_RCVBUF, 4*1024*1024);
    nn_setsockopt($s_caster_out, NN_SUB, NN_SUB_SUBSCRIBE, $jid); 


    Time::HiRes::nanosleep(0.5*1e9); # Give everything a bit of time to get connected, mainly sub channel

  # Broadcast connect
    my $connect_event = {
        'id'       => $task_id,
        'user'     => $user,
        'jid'      => $jid,
        'type'     => &Cmr::Types::CMR_CONNECT,
        'result'   => &Cmr::Types::CMR_RESULT_CONNECT,
        'cmdline'  => $config->{'STARTUP::META'}->{'cmdline'},
        'warnings' => 0,
        'elapsed'  => 0,
    };

    $self->{'submitted'}->{$task_id} = $connect_event;
    $task_id++;

    my $connect_json = JSON::XS->new->encode($connect_event);
    nn_send($s_caster_in, "${jid}:${connect_json}");
    $self->{'num_tasks_submitted'}++;

    my $now = Time::HiRes::gettimeofday;
    my $last_resubmit = $now;

    my $bytes_recv = nn_recv($s_server, my $worker_req, 262143);
    my $md5 = md5_hex("{}");
    nn_send($s_server, "${jid}:NO_WORK:${md5}:{}");

    my $active = 1;

    ## Reactor Main Loop

    while ( not $self->{'failed'}
            and 
            ( 
                not $self->{'finished'}
                or $self->{'num_tasks_submitted'} > $self->{'num_tasks_completed'}
                or $backlog->pending() > 0 
          ) ) 
    {

        Time::HiRes::nanosleep(0.001*1e9) unless ( $active );

        $now = Time::HiRes::gettimeofday;
        $active = 0;


        ## Check to see if any tasks need to be resubmitted (due to timeout)

        if ( $self->{'num_tasks_submitted'} > $self->{'num_tasks_completed'} 
             and ( $now - $last_resubmit ) > $resubmit_check_interval )
        {
            $last_resubmit = $now;
            for my $task_id (keys %{$self->{'submitted'}}) {
                my $task = $self->{'submitted'}->{$task_id};

                ## Check the acceptance deadline first
                next unless $task->{'accept_deadline'} < $now;

                unless ( $task->{'accepted'} ) {
                    delete $self->{'submitted'}->{$task_id};
                    resubmit_task($self, {'result'=>&Cmr::Types::CMR_RESULT_ACCEPT_TIMEOUT}, $task);
                    $active = 1;
                    next;
                }

                ## Now check the task deadline
                next unless $task->{'deadline'} < $now;
        
                delete $self->{'submitted'}->{$task_id};
                resubmit_task($self, {'result'=>&Cmr::Types::CMR_RESULT_TIMEOUT}, $task);
                $active = 1;
            }
        }


        ## Check for any work requests

        my $bytes_recv = nn_recv($s_server, my $worker_req, 262143);
        if ($bytes_recv && $worker_req) {

            ## Send a task from the client backlog to the worker
            { lock($backlog);

                my $task = $backlog->dequeue_nb;

                if ( $task ) {
                    my $worker_data = JSON::XS->new->decode($worker_req);
                    $worker_req = undef;

                    $task->{'submit_time'} = $now;
                    $task->{'accept_deadline'} = $now + $task->{'accept_timeout'};
                    $task->{'deadline'} = $now + $task->{'timeout'};

                    if ( ( $task->{'reducer'} and not $task->{'mapper'} ) or $task->{'type'} == &Cmr::Types::CMR_MERGE ) {
                        # If this is a merge task or a reducer only task (probably hierarchical reduce)
                        # increase the allowed time based on the number of tasks previously completed 
                        #   ( which should vaguely reflect the amount of data present )
                        $task->{'deadline'} += $self->{'num_tasks_submitted'} * ( $config->{'deadline_scale_factor'} // .05 );
                    }

                    $task->{'jid'}          = $jid;
                    $task->{'wid'}          = $worker_data->{'wid'};
                    $task->{'rid'}          = $worker_data->{'rid'};
                    $task->{'pid'}          = $pid;
                    $task->{'id'}           = $task_id;
                    $task->{'user'}         = $user;
                    $task->{'accepted'}     = 0;

                    my $json = JSON::XS->new->encode($task);

                    $log->debug("Submitting a $Cmr::Types::Task->{$task->{'type'}} task [$jid:$task_id]");

                    $self->{'submitted'}->{$task_id} = $task;
                    $self->{'num_tasks_submitted'}++;

                    my $md5 = md5_hex($json);
                    nn_send($s_server, "${jid}:TASK:${md5}:${json}");

                    print STDERR "", $self->{'num_tasks_completed'}, "/", $self->{'num_tasks_submitted'} if $config->{'verbose'};
                    $task_id++;
                }
                else {
                    $worker_req = undef;
                    my $md5 = md5_hex("{}");
                    nn_send($s_server, "${jid}:NO_WORK:${md5}:{}");
                }
            } # unlock
            $active = 1;
        }


        ## Check for any broadcasts

        $bytes_recv = nn_recv($s_caster_out, my $completion_event, 262143);
        if ($bytes_recv && $completion_event) {

            my ($completion_jid, $json) = split(":", $completion_event, 2);
            my $comp = JSON::XS->new->decode($json);

            if ( $completion_jid eq $jid and $comp->{'id'} ~~ $self->{'submitted'} )
            {
                $log->debug("Received reply for a $Cmr::Types::Task->{$self->{'submitted'}->{$comp->{'id'}}->{'type'}} task [$jid:$comp->{'id'}]");
                $log->debug("Result: $comp->{'result'}\n");
                $completion_handlers->{$comp->{'result'}}->($self, $comp, $self->{'submitted'}->{$comp->{'id'}});
                delete $self->{'submitted'}->{$comp->{'id'}} unless $comp->{'result'} == &Cmr::Types::CMR_RESULT_ACCEPT;
            }
            elsif ( $comp->{'result'} == &Cmr::Types::CMR_RESULT_BROADCAST ) {
                print_msg($self, $comp);
            }

            $completion_event = undef;
            print STDERR "", $self->{'num_tasks_completed'}, "/", $self->{'num_tasks_submitted'} if $config->{'verbose'};
            $active = 1;
        }
    }

    if ( $self->{'failed'} ) {
        my $fail_event = {
            'jid'      => $self->{'jid'},
            'id'       => 0,
            'user'     => $self->{'user'},
            'type'     => &Cmr::Types::CMR_JOB_STATUS,
            'result'   => &Cmr::Types::CMR_RESULT_JOB_FAILED,
            'warnings' => 0,
            'elapsed'  => 0,
        };

        my $fail_json = JSON::XS->new->encode($fail_event);
        nn_send($s_caster_in, "${jid}:${fail_json}");
    }

    print STDERR "\n" if $config->{'verbose'};

    my $disconnect_event = {
        'jid'      => $jid,
        'id'       => $task_id,
        'user'     => $user,
        'type'     => &Cmr::Types::CMR_DISCONNECT,
        'result'   => &Cmr::Types::CMR_RESULT_DISCONNECT,
        'warnings' => 0,
        'elapsed'  => 0,
    };

    $task_id++;

    my $disconnect_json = JSON::XS->new->encode($disconnect_event);
    nn_send($s_caster_in, "${jid}:${disconnect_json}");
    $self->{'num_tasks_submitted'}++;

    nn_close($s_server);
    nn_close($s_caster_in);
    nn_close($s_caster_out);
}


sub connect {
    my ($self, $comp, $task) = @_;
    $self->{'num_tasks_submitted'}--;
}


sub disconnect {
    my ($self, $comp, $task) = @_;
    $self->{'num_tasks_submitted'}--;
}


sub status {
    my ($self, $comp, $task) = @_;

    for my $worker (keys %{$comp->{'workers'}}) {
        print STDERR "$worker : $comp->{'workers'}->{$worker}\n";
    }

    $self->{'num_tasks_submitted'}--;
}


sub get_status {
    my ($self) = @_;
    $self->push({'type' => &Cmr::Types::CMR_STATUS});
    $self->sync();
}


sub server_status {
    my ($self, $comp, $task) = @_;

    use Data::Dumper;
    print STDERR Data::Dumper::Dumper($comp);

    $self->{'num_tasks_submitted'}--;
}

sub get_server_status {
    my ($self) = @_;
    $self->push({'type' => &Cmr::Types::CMR_SERVER_STATUS});
    $self->sync();
}


sub accept_task {
    my ($self, $comp, $task) = @_;
    $self->{'submitted'}->{$comp->{'id'}}->{'accepted'} = 1;
}


sub finish_task {
    my ($self, $comp, $task) = @_;

    if ( $comp->{'warnings'} and $self->{'config'}->{'verbose'}) { 
        print STDERR "Task[$comp->{'id'}] completed with warnings\n";
        $self->{'warnings'}++;
    }

    if ( $task->{'type'} != &Cmr::Types::CMR_CLEANUP and $comp->{'zerobyte'} != 1 ) {
        { lock $self->{'lock'};

            if ( $comp->{'bucket_destinations'} ) {
                for my $id (0 .. $task->{'buckets'}-1) {
                    $self->{'job_output'}->[$id] //= shared_clone([]);
                    for my $file (@{$comp->{'bucket_destinations'}->[$id]}) {
                        CORE::push @{$self->{'job_output'}->[$id]}, $file;
                    }
                }
            }
            elsif ( $task->{'destination'} ) {
                # Destination specified locally
                my $file = sprintf("%s/%s", $self->{'config'}->{'basepath'}, $task->{'destination'});
                $file =~ s/\/+/\//go;

                CORE::push @{$self->{'job_output'}},  $file;
            }

        } # unlock
    }

    if ( $comp->{'errors'} and $self->{'num_task_errors'} < $self->{'config'}->{'max_task_errors'} ) {
        my $error_prefix = "Encountered errors during $Cmr::Types::Task->{$task->{'type'}} task\n";
        if ($task->{'input'}) {
            $error_prefix .= "Input Files: ";
            for my $file (@{$task->{'input'}}) {
                $error_prefix .= "$self->{'config'}->{'basepath'}/$file  ";
            }
            $error_prefix .= "\n";
        }

        unless ($self->{'err_fd'}) {
            my $error_file = "$self->{'config'}->{'error_path'}/error.log";
            open($self->{'err_fd'}, '>'.$error_file);
        }
        my $fd = $self->{'err_fd'};

        print $fd $error_prefix . $comp->{'errors'};
        print STDERR $error_prefix . $comp->{'errors'};
        print STDERR "\n";
    }

    if ( $comp->{'errors'} ) {
        if ( $self->{'num_task_errors'} == $self->{'config'}->{'max_task_errors'} ) {
            print STDERR "max_task_errors ($self->{'config'}->{'max_task_errors'}) exceeded suppressing subsequent errors\n";
        }
        $self->{'num_task_errors'}++;
    }

    $self->{'num_tasks_completed'}++;
}


sub resubmit_task {
    my ($self, $comp, $task) = @_;


    if ( $comp->{'result'} == &Cmr::Types::CMR_RESULT_ACCEPT_TIMEOUT ) {
        $log->debug("Accept Timeout triggered task resubmit! Client $Cmr::Types::Task->{$task->{'type'}} Task [$task->{'jid'}:$task->{'id'}] returned result $Cmr::Types::Result->{$comp->{'result'}}");

        Time::HiRes::nanosleep(0.1*1e9);
    }


    if ( $comp->{'result'} == &Cmr::Types::CMR_RESULT_TIMEOUT 
    or   $comp->{'result'} == &Cmr::Types::CMR_RESULT_FAILURE ) {
        $log->debug("Timeout triggered task resubmit! Client $Cmr::Types::Task->{$task->{'type'}} Task [$task->{'jid'}:$task->{'id'}] returned result $Cmr::Types::Result->{$comp->{'result'}}");

        $task->{'failures'}++;
        $task->{'timeout'} += ( $self->{'config'}->{'retry_timeout'} // $self->{'config'}->{'retry_timeout'} );

        Time::HiRes::nanosleep(0.1*1e9);
    }


    if ( $task->{'failures'} > $self->{'config'}->{'max_task_attempts'} ) {
        $log->debug("Too many resubmit attempts for $Cmr::Types::Task->{$task->{'type'}} Task [$task->{'jid'}:$task->{'id'}]");

        fail_job($self, $comp, $task);
    } else {
        ## Just another resubmit, put the task at the front of the backlog
        $self->{'backlog'}->insert(0, $task);
    }

    $self->{'num_tasks_submitted'}--;
}

sub fail_job {
    my ($self, $comp, $task) = @_;

    if ( $comp ) {
        $log->error("Job failed!  $Cmr::Types::Task->{$comp->{'type'}} Task [$comp->{'jid'}:$comp->{'id'}] returned result $Cmr::Types::Result->{$comp->{'result'}}");
    }

    $self->{'failed'} = 1;
    $self->{'num_tasks_submitted'}--;
}

sub print_msg {
    my ($self, $comp, $task) = @_;
    print STDERR "\n\n~ $comp->{'msg'}\n\n";
}

1;
