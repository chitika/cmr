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

package Cmr::GlusterGlobAsync;

our $VERSION = '0.1';

use strict;
use warnings;

use threads ();
use threads::shared;
use Thread::Queue;

use Time::HiRes ();

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname(abs_path(__FILE__));

use Cmr::GlusterGlobParallel ();

use constant {
  ID => 0,
  TYPE => 1,
  PATTERN => 2,
};

use constant {
  REGEXP_PATTERN => 0,
  POSIX_PATTERN => 1,
  FINISH => 2,
  SCRAM => 3,
};

sub new {
  my ($config) = @_;

  my %files    : shared = ();
  my %complete : shared = ();
  my $lock     : shared = 0;

  my $obj = bless({
      'config' => $config,
      'queue'  => Thread::Queue->new,
      'files_by_ext'  => \%files,
      'complete' => \%complete,
      'lock'  => \$lock,
      'id'    => 0,
  });

  $obj->{'thread'} = threads->create(\&_GLOB_MAIN, $obj);
  return $obj;
}

sub scram {
  my ($self) = @_;
  $self->{'queue'}->enqueue({&TYPE=>&SCRAM});
  $self->{'thread'}->join();
}

sub finish {
  my ($self) = @_;
  $self->{'queue'}->enqueue({&TYPE=>&FINISH});
  $self->{'thread'}->join();
}

sub RegexpGlob {
    my ($self, $pattern) = @_;

    my $id = $self->{'id'};
    $self->{'id'}++;

    my $glob = bless({
      'globber' => $self,
      'id'      => $id,
    });

    my %exts : shared = ();
    my $complete : shared = 0;

    $self->{'files_by_ext'}->{$id} = \%exts;
    $self->{'complete'}->{$id} = \$complete;

    $self->{'queue'}->enqueue({&ID=>$id, &TYPE=>&REGEXP_PATTERN, &PATTERN=>$pattern});
    return $glob;
}


sub PosixGlob {
    my ($self, $pattern) = @_;

    my $id = $self->{'id'};
    $self->{'id'}++;

    my $glob = bless({
      'globber' => $self,
      'id'      => $id,
    });

    my %exts : shared = ();
    my $complete : shared = 0;
    $self->{'files_by_ext'}->{$id} = \%exts;
    $self->{'complete'}->{$id} = \$complete;

    $self->{'queue'}->enqueue({&ID=>$id, &TYPE=>&POSIX_PATTERN, &PATTERN=>$pattern});
    return $glob;
}


sub _GLOB_MAIN {
  my ($self) = @_;

  my $glob;
  my $finished = 0;
  my $scram = 0;
  my $queue = $self->{'queue'};

  my $basepath = qr/$self->{'config'}->{'basepath'}/;

  while( (!$scram) and ( (!$finished) or $glob ) ) {
    if ($glob) {
      if ($scram) {
          $glob->[0]->scram();
          $glob = undef;
          last;
      }

      my @files = $glob->[0]->next();
      if (@files) {
        { lock ${$self->{'lock'}};
          for my $file (@files) {
            my ($ext) = $file =~ /\.([^\.]*)$/o;
            $ext //= "uncompressed";
            unless (exists $self->{'files_by_ext'}->{$glob->[1]}->{$ext}) {
                my @files = ();
                share(@files);
                $self->{'files_by_ext'}->{$glob->[1]}->{$ext} = \@files;
            }
            push @{$self->{'files_by_ext'}->{$glob->[1]}->{$ext}}, $file;
          }
        }

      } else {

        { lock ${$self->{'lock'}};
          ${$self->{'complete'}->{$glob->[1]}} = 1;
        }

        $glob = undef;
      }
      next;
    }

    my $cmd = $queue->dequeue_nb;
    if ( $cmd ) {
      if      ( $cmd->{&TYPE} == &REGEXP_PATTERN ) {
        $glob = [Cmr::GlusterGlobParallel::RegexpGlob($self->{'config'}, $cmd->{&PATTERN}), $cmd->{&ID}];
      } elsif ( $cmd->{&TYPE} == &POSIX_PATTERN ) {
        $glob = [Cmr::GlusterGlobParallel::PosixGlob($self->{'config'}, $cmd->{&PATTERN}), $cmd->{&ID}];
      } elsif ( $cmd->{&TYPE} == &FINISH ) {
        $finished = 1;
      } elsif ( $cmd->{&TYPE} == &SCRAM ) {
        $scram = 1;
      }
    }
    Time::HiRes::nanosleep(0.01*1e9);
  }
}

sub next {
  my ($self, $batchsize) = @_;
  $batchsize //= 1;

  my $globber = $self->{'globber'};
  my @batch = ();

  my $cur_ext;
  while(1) {
    for my $ext (keys %{$globber->{'files_by_ext'}->{$self->{'id'}}}) {
      if ( scalar(@{$globber->{'files_by_ext'}->{$self->{'id'}}->{$ext}}) > $batchsize ) {
        { lock ${$globber->{'lock'}};
          for my $i (0..($batchsize-1)) {
            push( @batch, pop ( @{$globber->{'files_by_ext'}->{$self->{'id'}}->{$ext}} ) );
          }
        }
        $cur_ext = $ext;
        last;
      }
    }

    if ( ${$globber->{'complete'}->{$self->{'id'}}} ) {
      { lock ${$globber->{'lock'}};
        for my $ext (keys %{$globber->{'files_by_ext'}->{$self->{'id'}}}) {
          if ( scalar( @{$globber->{'files_by_ext'}->{$self->{'id'}}->{$ext}} ) ) {
            my $count = $#{$globber->{'files_by_ext'}->{$self->{'id'}}->{$ext}};
            for my $i (0..$count) {
              push( @batch, pop ( @{$globber->{'files_by_ext'}->{$self->{'id'}}->{$ext}} ) );
            }
            $cur_ext = $ext;
            last;
          }
        }
      }
      last;
    }
    Time::HiRes::nanosleep(0.01*1e9);
  }

  if (scalar(@batch)) {
    return $cur_ext, \@batch;
  }
  return;
}

1;
