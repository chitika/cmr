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

package Cmr::Index::Redis;

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__))."/../../../../";
use lib '/srv/perl-bricks/';

use JSONUtils ();
use Time::HiRes ();
use Redis ();
use Cmr::Seaweed ();
use Data::Dumper;

our $VERSION = '0.1';

use strict;
use warnings;

our $checkmetare = qr/[\|\(\)\[\{\^\$\*\+\?\.]/o;
our $metare      = qr/(?<!\\)[\|\(\)\[\{\^\$\*\+\?\.]/o;
our $metacapre   = qr/(?<!\\)([\|\(\)\[\{\^\$\*\+\?\.])/o;

our $fs;
our $redis;

our $handlers = {
    'index_traverse' => \&traverse,
};


sub traverse {
    my ($task, $self, $config, $reactor) = @_;

    my $db = $task->{'db'} // 0;
    my $prefix = $task->{'prefix'} // "warehouse";

    unless ($fs) {
        $fs = Cmr::Seaweed::new(redis_addr=>$config->{redis_addr}, seaweed_addr=>$config->{seaweed_addr}, 'db'=>$db, 'prefix'=>$prefix);
    }
    unless ($redis) {
        # Thats what i get for implenting things slightly differently here... (*sigh*)
        $redis = Redis->new(server => $config->{redis_addr});
        $redis->select($db);
    }

    my $lhs = [];
    my $rhs;

    if ($task->{'path'}) {
        my $path = $task->{'path'};
        my @rhs = (split(/\//, $prefix), split(/\//, $path));
        $rhs = \@rhs;
    }
    else {
        $lhs = [@{$task->{'lhs'}}, $task->{match}];
        $rhs = $task->{'rhs'};
    }

    foreach my $i (0..$#{$rhs}) {
        if ( $rhs->[$i] =~ m/$metare/o ) {
            next if $rhs->[$i] =~ /^\.$/o; # Special case './'
            my @new_rhs = @{$rhs}[$i+1..$#{$rhs}];
            return &__expand($rhs->[$i], $lhs, \@new_rhs, $config, $reactor);
        }
        push $lhs, $rhs->[$i];
    }

    return;

};


sub __expand {
    my ($pattern, $lhs, $rhs, $config, $reactor) = @_;

    my $lhs_joined = join('/', @{$lhs});

    if ($config->{polymer}) {
        return ($lhs_joined) unless @$rhs;
    }

    my @matched;
    if ( not $pattern or $pattern eq ".*"  ) {
        @matched = $redis->hkeys($lhs_joined);
    }
    else {
        my $reply = $redis->hkeys($lhs_joined);
        @matched = grep (/\b$pattern\b/, @$reply);
    }
 
    if (@$rhs or $config->{polymer}) {
        foreach my $match (@matched) {
            if ($match =~ m/$checkmetare/o) {
                $match =~ s/$metacapre/\\$1/g;
            }
            $reactor->push_front({'task'=>'index_traverse', 'lhs'=>$lhs, 'match'=>$match, 'rhs'=>$rhs});
        }
    }
    else {
        my @result;
        foreach my $match (@matched) {
            my $file_data = $redis->hget($lhs_joined, $match);
            my ($size,$fid) = $file_data =~ /^(\d+):(.*)$/o; # new format
            $fid //= $file_data;                             # old format
            my $key = "$lhs_joined/$match";
            if ($fid ne "1") {
                push @result, $fs->__fid2loc($fid, $key);
            }
        }
        return @result if @result;
    }
};


1;
