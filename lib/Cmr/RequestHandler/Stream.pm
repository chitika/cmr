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

package Cmr::RequestHandler::Stream;

use parent Cmr::RequestHandler;
@EXPORT_OK = qw(handle_request_local);

use strict;
use warnings;

sub handle_request_local {
    my ($self, $task, $config) = @_;
    return &Cmr::Types::CMR_RESULT_SUCCESS unless $task->{'input'};

    # Assume  failure
    my $result = &Cmr::Types::CMR_RESULT_FAILURE;

    # Build command pipeline
    my @cmds;
    push @cmds, "curl -s -H 'Accept-Encoding: gzip' " . join(' ', @{$task->{'input'}});
    push @cmds, "gzip -dc";

    if ($task->{'mapper'}) {
        push @cmds, "$task->{'mapper'} --CMR_NAME mapper";
    }

    if ($task->{'reducer'}) {
        push @cmds, "$task->{'reducer'} --CMR_NAME reducer";
    }

    push @cmds, "gzip -c";

    if (exists $task->{'persist'}) {
        # This output is being persisted as user data
        push @cmds, "seaweed_set -d 2 -p user -k $task->{'user'}/$task->{'persist'}/$task->{'output'}";
    }
    else {
        # This output is temporary job data
        push @cmds, "seaweed_set -d 1 -p job -k $task->{'jid'}/$task->{'output'}";
    }

    my $timeout = $task->{'deadline'} - Time::HiRes::gettimeofday;
    if ($timeout < 0) { return $result; }
    my $cmd = "timeout -s KILL ${timeout} cmr-pipe --CMR_PIPE_UID $task->{'uid'} --CMR_PIPE_GID $task->{'gid'} " . join(' : ', @cmds);
    my $rc = &Cmr::RequestHandler::task_exec($task, $cmd);

    # Check for success
    $result = &Cmr::Types::CMR_RESULT_SUCCESS if $rc == 0;

    return $result;
}

1;
