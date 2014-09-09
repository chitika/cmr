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

package Cmr::GlusterUtils;

our $VERSION = '0.1';

use strict;
use warnings;

sub CheckMount {
    # Get the mount point from mtab
    my $mount;
    open(my $mtab, "<", '/etc/mtab');
    while(<$mtab>) {
        ($mount) = $_ =~ /\s+(\S+)\s+fuse\.glusterfs/o;
        last if $mount;
    }
    close($mtab);

    # Compare dev id of mount point and parent ( they should be different )
    my ($mdev) = stat("$mount");
    my ($rdev) = lstat("${mount}/..");

    return ($mdev != $rdev) ? 1 : 0;
}


# Internal to RegexpGlob, probably not very useful on its own
sub _REGEXP_GLOB_EVAL {
  my ($glob, $lhs, $rhs) = @_;
  my @batch = ();
  opendir(my $dir, $lhs) || return @batch;
  my @matched = grep (/\b$glob\b/, readdir($dir));
  closedir($dir);
  foreach my $match (@matched) {
    @batch = (@batch, RegexpGlob($rhs, "$lhs/$match"));
  }
  return @batch;
}

sub RegexpGlob {
  # NOTE: doesn't support '..'
  # NOTE: if provided pattern matches directories they will be returned

  my ($rhs, $lhs) = @_;
  my @rhs = split(/\//, $rhs);
  $lhs //= "";
  foreach my $i (0..$#rhs) {
    foreach my $metachar (qw /* + . ? { [ (/ ) {
      if ( $metachar ~~ [split(//,$rhs[$i])] ) {
        next if $rhs[$i] =~ /^\.$/o; # Special case './'
        my $rest = join('/',@rhs[$i+1..$#rhs]) || "";
        return _REGEXP_GLOB_EVAL($rhs[$i], $lhs, join('/',@rhs[$i+1..$#rhs]));
      }
    }
    $lhs .= "/$rhs[$i]/";
    $lhs =~ s/\/\//\//go;
  }
  if ($lhs =~ /\.\.?$/o) { return (); }
  #$lhs =~ s/\/\//\//go;
  return ($lhs);
}

sub PosixGlob {
  # NOTE: doesn't support '..'
  # NOTE: if provided pattern matches directories they will be returned
  my ($glob) = @_;

  # try to translate bash style glob pattern to regexp and then call regexp glob

  # * => .*
  $glob =~ s/(?<!\\)\*/.*/go;

  # {abc,def} => (abc|def)
  while ( $glob =~ /(?<!\\){.*?(?<!\\)}/o ) { 
    my ($curly) = $glob =~ /((?<!\\){.*?(?<!\\)})/o;
    $curly =~ s/(?<!\\),/|/go;
    $curly =~ s/^(?<!\\){/(/o;
    $curly =~ s/(?<!\\)}$/)/o;
    $glob =~ s/(?<!\\){.*?(?<!\\)}/$curly/o;
  }

  return RegexpGlob($glob);
}

1;

__END__

# This package provides some routines for interacting with gluster
# If we need support for relative path creating a C binding to non-stat'ing glob is likely the simplest thing to do.

use GlusterUtils ();

my @glob_result = GlusterUtils::PosixGlob('/mnt/fs/my_{folder,other_folder}/*/*');
my @glob_result = GlusterUtils::RegexpGlob('/mnt/fs/my_folder/.*/.*');

print join(' ', @glob_result), "\n";

