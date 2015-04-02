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

package Cmr::RequestHandler::Grep;

use parent Cmr::RequestHandler;
@EXPORT_OK = qw(handle_request_local);

use strict;
use warnings;

sub handle_request_local {
    my ($self, $task, $config) = @_;
    return &Cmr::Types::CMR_RESULT_SUCCESS unless $task->{'input'};

    # Assume failure
    my $result = &Cmr::Types::CMR_RESULT_FAILURE;

    # Build up grep command from task args
    my $flags = $task->{'flags'} // [];
    my $grep = "grep " . join(' ', @{$flags} );

    for my $pattern (@{$task->{'patterns'}}) {
        $grep .= " -e '$pattern' ";
    }

    # Build up command line
    my @cmds;

    push @cmds, "curl -s -H 'Accept-Encoding: gzip' " . join(' ', @{$task->{'input'}});
    push @cmds, "gzip -dc";
#    push @cmds, "seaweed_get " . join(' ', @{$task->{'input'}});
#    push @cmds, "curl -N -s " . join(' ', @{$task->{'input'}});

#    if ( $task->{'in_fmt_cmd'} ) {
#        push @cmds, "$task->{'in_fmt_cmd'}";
#    }

    push @cmds, "$grep";
    push @cmds, "gzip -c";
    push @cmds, "seaweed_set -k $task->{'jid'}/$task->{'output'} -d 1";

    my $timeout = $task->{'deadline'} - Time::HiRes::gettimeofday;
    if ($timeout < 0) { return $result; }
    my $cmd = "timeout -s KILL ${timeout} cmr-pipe --CMR_PIPE_UID $task->{'uid'} --CMR_PIPE_GID $task->{'gid'} " . join(' : ', @cmds);
    my $rc = &Cmr::RequestHandler::task_exec($task, $cmd);

    # Check for success
    $result = &Cmr::Types::CMR_RESULT_SUCCESS if $rc == 0;

    return $result;
}

1;
