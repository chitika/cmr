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

package Cmr::Types;

use strict;
use warnings;

use constant {
    CMR_CONNECT             => 0,
    CMR_DISCONNECT          => 1,
    CMR_GREP                => 2,
    CMR_STREAM              => 3,
    CMR_NODE_COUNT          => 4,
    CMR_TASK                => 5,
    CMR_CLEANUP             => 6,
    CMR_REJECT              => 7,
    CMR_FILTER              => 8,
    CMR_STATUS              => 9,
    CMR_MERGE               => 10,
    CMR_JOB_STATUS          => 11,
    CMR_SERVER_STATUS       => 12,
    CMR_BROADCAST           => 13,
    CMR_BUCKET              => 14,
    CMR_CLEANUP_TEMPORARY   => 15,
    CMR_POLYMER             => 16,
};

our $Watcher = {
    &CMR_CONNECT            => "connect",
    &CMR_DISCONNECT         => "disconnect",
    &CMR_GREP               => "grep",
    &CMR_STREAM             => "stream",
    &CMR_NODE_COUNT         => "node_count",
    &CMR_TASK               => "task",
    &CMR_CLEANUP            => "cleanup",
    &CMR_REJECT             => "reject",
    &CMR_FILTER             => "filter",
    &CMR_STATUS             => "status",
    &CMR_MERGE              => "merge",
    &CMR_JOB_STATUS         => "job_status",
    &CMR_SERVER_STATUS      => "server_status",
    &CMR_BROADCAST          => "broadcast",
    &CMR_BUCKET             => "bucket",
    &CMR_POLYMER            => "polymer",
    &CMR_CLEANUP_TEMPORARY  => "cleanup_temporary",
};

our $Task = {
    &CMR_CONNECT            => "Connect",
    &CMR_DISCONNECT         => "Disconnect",
    &CMR_GREP               => "Grep",
    &CMR_STREAM             => "Stream",
    &CMR_NODE_COUNT         => "Node Count",
    &CMR_TASK               => "Task",
    &CMR_CLEANUP            => "Cleanup",
    &CMR_REJECT             => "Reject",
    &CMR_FILTER             => "Filter",
    &CMR_STATUS             => "Status",
    &CMR_MERGE              => "Merge",
    &CMR_JOB_STATUS         => "Job Status",
    &CMR_SERVER_STATUS      => "Server Status",
    &CMR_BROADCAST          => "Broadcast",
    &CMR_BUCKET             => "Bucket",
    &CMR_POLYMER            => "Polymer",
    &CMR_CLEANUP_TEMPORARY  => "Cleanup Temporary",
};

use constant {
    CMR_RESULT_SUCCESS          => 0,
    CMR_RESULT_TIMEOUT          => 1,
    CMR_RESULT_FAILURE          => 2,
    CMR_RESULT_JOB_FAILURE      => 3,
    CMR_RESULT_WORKER_REJECT    => 4,
    CMR_RESULT_SERVER_REJECT    => 5,
    CMR_RESULT_MISSING_OUT_PATH => 6,
    CMR_RESULT_MISSING_ERR_PATH => 7,
    CMR_RESULT_CONNECT          => 8,
    CMR_RESULT_DISCONNECT       => 9,
    CMR_RESULT_WORKER_FINISHED  => 10,
    CMR_RESULT_STATUS           => 11,
    CMR_RESULT_UNKNOWN          => 12,
    CMR_RESULT_JOB_FAILED       => 13,
    CMR_RESULT_ACCEPT           => 14,
    CMR_RESULT_SERVER_STATUS    => 15,
    CMR_RESULT_ACCEPT_TIMEOUT   => 16,
    CMR_RESULT_BROADCAST        => 17,
};

our $Result = {
    &CMR_RESULT_SUCCESS          => "Success",
    &CMR_RESULT_TIMEOUT          => "Timeout",
    &CMR_RESULT_FAILURE          => "Failure",
    &CMR_RESULT_WORKER_REJECT    => "Worker Rejected",
    &CMR_RESULT_SERVER_REJECT    => "Server Rejected",
    &CMR_RESULT_MISSING_OUT_PATH => "Missing Output Path",
    &CMR_RESULT_MISSING_ERR_PATH => "Missing Error Path",
    &CMR_RESULT_CONNECT          => "Connected",
    &CMR_RESULT_DISCONNECT       => "Disconnected",
    &CMR_RESULT_WORKER_FINISHED  => "Worker Finished",
    &CMR_RESULT_STATUS           => "Status",
    &CMR_RESULT_UNKNOWN          => "Unknown Result",
    &CMR_RESULT_JOB_FAILED       => "Job Failed",
    &CMR_RESULT_ACCEPT           => "Accept",
    &CMR_RESULT_SERVER_STATUS    => "Server Status",
    &CMR_RESULT_ACCEPT_TIMEOUT   => "Accept Timeout",
    &CMR_RESULT_BROADCAST        => "Broadcast",
};

1;
