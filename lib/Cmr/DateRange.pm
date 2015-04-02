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

use strict
use warnings;

use Date::Manip qw(UnixDate);

our @thresh_lo = (0,1,1,0,0,0);
our @thresh_hi = (9001,13,32,24,60,60);

use constant {
    LEFT => 0,
    RIGHT => 1,
    MIDDLE => 2,
};

sub get_wildcard_ranges {
    my ($start, $end) = @_;
    my @ranges = get_ranges($start, $end);

    my @wildcards;
    my @parts = ('YEAR','MONTH','DAY','HOUR','MINUTE','SECOND');
    RANGE: foreach my $range (@ranges)
    {
        my %fields;
        for my $i (0 .. $#parts) {
            if ( $range->[0]->[$i] == $thresh_lo[$i] and
                 $range->[1]->[$i] == $thresh_hi[$i]) {
                $fields{$parts[$i]} = "*";
            }
            elsif ( $range->[0]->[$i] == $range->[1]->[$i] ) {
                $fields{$parts[$i]} = int( $range->[0]->[$i] );
                if ($parts[$i] ~~ ['MONTH','DAY'] and $fields{$parts[$i]} < 10) {
                    $fields{$parts[$i]} = "0".$fields{$parts[$i]};
                }
            }
            else {
                $range->[1]->[$i] = ($range->[1]->[$i] == $thresh_hi[$i]) ? $thresh_hi[$i] - 1 : $range->[1]->[$i];
                my @values = ($range->[0]->[$i] .. $range->[1]->[$i]);
                if ($parts[$i] ~~ ['MONTH','DAY']) {
                    for my $idx (0 .. $#values) {
                        if ($values[$idx] < 10) {
                            $values[$idx] = "0".$values[$idx];
                        }
                    }
                }
                next RANGE unless @values;
                $fields{$parts[$i]} = "{" . join(',', @values) . "}";
            }
        }
        push @wildcards, [$fields{'YEAR'},$fields{'MONTH'},$fields{'DAY'},$fields{'HOUR'},$fields{'MINUTE'},$fields{'SECOND'}];
    }
    return @wildcards;
}


sub get_ranges {
    my ($start, $end) = @_;

    my $date_start = UnixDate ($start, "%Y-%m-%d-%H-%M-%S");
    my $date_end   = UnixDate ($end,   "%Y-%m-%d-%H-%M-%S");

    my @parts_start = split(/-/o, $date_start);
    my @parts_end   = split(/-/o, $date_end);

    my @ranges = _split_ranges(\@parts_start, \@parts_end);
}

sub _split_ranges {
    my ($start_ref, $end_ref, $side) = @_;
    $side //= 0;

    my @start = @$start_ref;
    my @end = @$end_ref;

    my @d;
    my @results;
    my $i = 0;

    return if @start ~~ @end;

    while (@start) {
        my $s = shift(@start);
        my $e = shift(@end);

        if ($s == $e) {
            # do nothing
            push @d, $s;
            $i++;
        } else {
            # split into three ranges

            return if $s == 0 && $e == $thresh_hi[$i] && @start ~~ @thresh_lo[($i+1) .. $#thresh_lo];
            $i++;

# part 1 - left
            my $s1 = $s;
            my $e1 = $s;

# part 2 - middle
            my $s2 = $s;
            my $e2 = $e-1;

            if ( $side == &LEFT ) {
              $s2 = $s+1;
            } elsif ( $side == &RIGHT ) {
              $s2 = $s+1;
              $e2 = $e;
            }

# part 3 - right
            my $s3 = $e;
            my $e3 = $e;

            if ( $e1 < $e && $side != &MIDDLE) { # case 1
                my $ns = [@d,$s1,@start[0 .. $#start]];
                my $ne = [@d,$e1,@thresh_hi[$i .. $#thresh_hi]];

                my @ranges = _split_ranges($ns, $ne, &RIGHT);
                push @results, @ranges if @ranges;
            }

            if ( $s3 >= 0 && $s3 != $s1 && $side != &RIGHT) { # case 3
                my $ns = [@d,$s3,@thresh_lo[$i .. $#thresh_lo]];
                my $ne = [@d,$e3,@end[0 .. $#end]];

                my @ranges = _split_ranges($ns, $ne, &MIDDLE);
                push @results, @ranges if @ranges;
            }

            if ($e2 < $s) {
                return @results;
            }

            my @mid_start = (@d,$s2,@thresh_lo[$i .. $#thresh_lo]);
            my @mid_end   = (@d,$e2,@thresh_hi[$i .. $#thresh_hi]);

            # make sure we're using properly formatted isodate
            for my $i (1 .. $#mid_start) {
                $mid_start[$i] = ($mid_start[$i] < 10) ? "0".int($mid_start[$i]) : $mid_start[$i];
                $mid_end[$i]   = ($mid_end[$i] < 10)   ? "0".int($mid_end[$i])   : $mid_end[$i];
            }

            if ( @start ~~ @thresh_lo[$i .. $#thresh_lo] || $side == &MIDDLE ) {
                @mid_start = (@d,$s,@thresh_lo[$i .. $#thresh_lo]);
            }


            if (fmtdate(@mid_start) le fmtdate(@mid_end)) {
                push @results, [\@mid_start, \@mid_end];
            }

            return @results; #  case 2
        }
    }

    return @results;
}

sub fmtdate {
    return "$_[0]-$_[1]-$_[2] $_[3]:$_[4]:$_[5]";
}

