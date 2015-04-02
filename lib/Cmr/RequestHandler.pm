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
use lib dirname (abs_path(__FILE__))."/..";

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
    my ($self, $request, $reactor_self, $config, $reactor) = @_;
    my $log = Cmr::StartupUtils::get_logger();

    my $task = $request->{'data'};
    $task->{'started_time'} = Time::HiRes::gettimeofday;
    $task->{'wslot'} = $reactor->{'id'};

    $task->{'result'} = &Cmr::Types::CMR_RESULT_FAILURE;
    $task->{'result'} = $self->handle_request_local($task, $config);

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
            $task->{'warnings'} = 1;
            $task->{'errors'} = "RC: $rc\n???\n";
            #TODO: figure out what happened
        }
    }

    return $rc;
}

1;
