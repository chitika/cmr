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

package Cmr::RequestHandler;

use base qw(Exporter);
@EXPORT_OK = qw(new handle_request);

use strict;
use warnings;

use Time::HiRes qw(gettimeofday);
use IPC::Open3;
use Cmr::StartupUtils ();

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__));

use Cmr::Types;

sub new($$) {
    my ($class, $queue) = @_;
    my $self = {};
    bless $self, $class;
    $self->{'queue'} = $queue;
    return $self;
}

sub handle_request_local {
    return &Cmr::Types::CMR_RESULT_FAILURE;
}

sub handle_request($$$$) {
    my ($self, $request, $config, $reactor) = @_;
    my $log = Cmr::StartupUtils::get_logger();

    my $task = $request->{'data'};
    $task->{'started_time'} = Time::HiRes::gettimeofday;

    $task->{'wslot'} = $reactor->{'id'};

    $task->{'result'} = &Cmr::Types::CMR_RESULT_FAILURE;

    # Setup output paths
    my $out_file = sprintf("%s/%s", $config->{'basepath'}, $task->{'destination'});
    my $out_path = $out_file;
    $out_path =~ s/\/[^\/]*$//o;
    $task->{'out_path'} = $out_path;
 

    # Working around some gluster issues (client desync)
    my $retries = 30;
    while ( $retries > 0 && !(-e $out_path) ) {
      $retries--;
      system("ls -l $out_path > /dev/null 2>&1");
      Time::HiRes::nanosleep(0.3*1e9);
    }
    $task->{'retries'} = (30 - $retries);


    # Make sure the out path exists
    if (! -e $out_path) {
        # Not our problem, the client was supposed to create this
        $task->{'result'} = &Cmr::Types::CMR_RESULT_MISSING_OUT_PATH;
        $self->{'queue'}->enqueue($task);
        return;
    }

    # Setup input path
    my $input = "";
    if ($task->{'input'}) {
        for my $file (@{$task->{'input'}}) {
            # Prepend input files with warehouse basepath (stripped by client)
            $input .= sprintf("%s/%s ", $config->{'basepath'}, $file);
            next if $task->{'type'} == &Cmr::Types::CMR_CLEANUP;

            # More Working around some gluster issues (client desync)
            my $more_retries = 30;
            while ( $more_retries > 0 && !(-e "$config->{'basepath'}/$file") ) {
              $more_retries--;
              system("ls -l $config->{'basepath'}/$file > /dev/null 2>&1");
              Time::HiRes::nanosleep(0.1*1e9);
            }

            $task->{'retries'} = (30 - $more_retries);
        }
    }

    # Handle the request
    $task->{'result'} = $self->handle_request_local($task, $config, $input, $out_file);

    # If no output was produced tell the client so it doesn't waste a bunch of time working on an empty files
    if (-z ${out_file}) {
      $task->{'zerobyte'} = 1;
      if ($config->{'delete_zerobyte_output'}) {
        system("rm -f ${out_file}");
      }
    }


    $self->{'queue'}->enqueue($task);
    return;
}

sub task_exec {
    my ($task, $cmd) = @_;
    my $rc;

    {
        my $result = `$cmd`;
        $rc = $? >> 8;

        if ($result) {
            $task->{'warnings'} = 1;
            $task->{'errors'} = substr( $result, 0, 65536 ) . "\n";
        }

        if ($rc != 0) {
            #TODO: figure out what happened
        }
    }

    return $rc;
}

1;
