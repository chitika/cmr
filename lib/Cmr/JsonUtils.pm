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

package Cmr::JsonUtils;

use strict;
use warnings;

use JSON::XS ();

sub Encode {
    if (ref($_[0])) {
        return JSON::XS->new->encode($_[0]);
    }
    return $_[0];
}

sub Decode($) {
    return undef unless ($_[0]);
    my $json = eval {JSON::XS->new->decode($_[0]);};
    return undef if ($@);
    return $json;
}

sub PathSplit {
    my @args = @_;
    my @params;
    foreach my $p (@args) {
        my @path = split(/\./, $p);
        push(@params, \@path);
    }
    return @params;
}

sub GetField($$) {
    my ($path, $hash) = @_;
    return undef unless (defined($path));

    my $depth = 0;
    my $max_depth = scalar(@$path);

    while ( $max_depth > $depth ) {
        if (ref($hash) eq 'HASH') {
            $hash = $hash->{$path->[$depth]};
        } elsif ( ref($hash) eq 'ARRAY' ) {
            $hash = $hash->[$path->[$depth]];
        } else {
            $hash = undef;
            last;
        }
        ++$depth;
    }
    return $hash;
}

1;
