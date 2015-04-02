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

package Cmr::RequestHandler::Polymer;

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
 
    my $paths = join(' ', map { "-p $_" } @{$task->{'input'}});
    my $fields = join(' ', map { "-f $_" } @{$task->{'fields'}});
    push @cmds, "polymer_paste ${paths} ${fields}";

    my @include_filters;
    my @exclude_filters;

    if ( exists( $task->{'filter-include'} ) ) {
        for my $filter (@{$task->{'filter-include'}}) {
            my ($k, $v) = @{$filter};
            next unless ( defined($k) and defined($v) );
            push @include_filters, "\$$k ~ /$v/";
        }
    }

    if ( exists( $task->{'filter-exclude'} ) ) {
        for my $filter (@{$task->{'filter-exclude'}}) {
            my ($k, $v) = @{$filter};
            next unless ( defined($k) and !defined($v) );
            push @exclude_filters, "\$$k !~ /$v/";
        }
    }

    if ( exists($task->{'filter-include-range'}) ) { 
        for my $filter ( @{$task->{'filter-include-range'}} ) {
            my ($k, $lo, $hi) = @{$filter};
            next unless ( defined($k) and defined($lo) and defined($hi) );
            if ( $lo =~ /\s/o and not $lo =~ /^\".*\"$/o ) {
                $lo = qq("$lo");
            }
            if ( $hi =~ /\s/o and not $hi =~ /^\".*\"$/o ) {
                $hi = qq("$hi");
            }
            push @include_filters, "\$$k >= $lo && \$$k < $hi";
        }
    }

    if ( exists($task->{'filter-exclude-range'}) ) {
        for my $filter ( @{$task->{'filter-exclude-range'}} ) {
            my ($k, $lo, $hi) = @{$filter};
            next unless ( defined($k) and defined($lo) and defined($hi) );
            if ( $lo =~ /\ /o and not $lo =~ /^\".*\"$/o ) {
                $lo = qq("$lo");
            }
            if ( $hi =~ /\ /o and not $hi =~ /^\".*\"$/o ) {
                $hi = qq("$hi");
            }
            push @exclude_filters, "\$$k < $lo && \$$k >= $hi";
        }
    }

    if (@include_filters || @exclude_filters) {
        my @print_fields;
        if ( $task->{'print-fields'} ) {
            for my $field ( @{$task->{'print-fields'}} ) {
                push @print_fields, "\$${field}";
            }
        }
        else {
             @print_fields = ("\$0");
        }

        my $awk_cmd = "awk -F\"\\001\" \'{if (" . join(" && ", @include_filters, @exclude_filters) . ") { print " . join('"\001"', @print_fields) . "; }}\'";
        push @cmds, "${awk_cmd} --CMR_NAME awk";
    }

    if ($task->{'mapper'}) {
        push @cmds, "$task->{'mapper'} --CMR_NAME mapper";
    }

    if ($task->{'reducer'}) {
        push @cmds, "$task->{'reducer'} --CMR_NAME reducer";
    }

    push @cmds, "seaweed_set -k $task->{'jid'}/$task->{'output'} -d 1";

    my $timeout = $task->{'deadline'} - Time::HiRes::gettimeofday;
    if ($timeout < 0) { return $result; }
    my $cmd = "timeout -s KILL ${timeout} cmr-pipe --CMR_PIPE_UID $task->{'uid'} --CMR_PIPE_GID $task->{'gid'} " . join(' : ', @cmds);
    my $rc = &Cmr::RequestHandler::task_exec($task, $cmd);

    print "$rc : $cmd\n";

    # Check for success
    $result = &Cmr::Types::CMR_RESULT_SUCCESS if $rc == 0;
    return $result;
}

1;

