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

package Cmr::StartupUtils::StartupLogTrap;
use parent Cmr::StartupUtils;

## Trapper for messages logged to stderr that should have gone to the logger

our $VERSION = '0.1';

use strict;
use warnings;

use Log::Log4perl qw(:easy);
use Cmr::StartupUtils ();

sub TIEHANDLE {
    my $class = shift;
    bless [], $class;
}

sub PRINT {
    my $self = shift;
    $Log::Log4perl::caller_depth++;
    my $logger = Cmr::StartupUtils::get_logger();
    $logger->error(@_);
    $Log::Log4perl::caller_depth--;
}

1;
