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

package Cmr::GlusterGlobParallel;

our $VERSION = '0.1';

use strict;
use warnings;

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__));

use Cmr::ReactorAsync();

use threads;
use threads::shared;

use Thread::Queue;

our $metare  = qr/(?<!\\)([\|\(\)\[\{\^\$\*\+\?\.])/;

sub RegexpGlob {
    my ($config, $pattern) = @_;

    my $return_queue = Thread::Queue->new();

    my @paths = ($pattern);
    $return_queue->enqueue(\@paths);

    my $handlers = {
      'expand_path' => sub {
        my ($task) = @_;
        my @paths = ();

        my $rc = opendir(my $dir, $task->{'lhs'});
        if (!$rc) {
            $return_queue->enqueue(\@paths);
            return;
        }

        my @matched = grep (/\b$task->{'pattern'}\b/, readdir($dir));
        closedir($dir);

        foreach my $match (@matched) {
          $match =~ s/$metare/\\$1/g; # escape all metacharacters in matched files
          push @paths, "$task->{'lhs'}/$match/$task->{'rhs'}";
        }
        $return_queue->enqueue(\@paths);
      }
    };

    my $ev = Cmr::ReactorAsync::init( $handlers, $config );

    return bless( {
        'return_queue' => $return_queue,
        'submitted' => 1,
        'completed' => 0,
        'reactor' => $ev,
        'batch'   => [],
        'finished' => 0,
    } );
}

sub PosixGlob {
    my ($config, $pattern) = @_;

    # try to translate bash style glob pattern to regexp and then call regexp glob

    # * => .*
    $pattern =~ s/(?<!\\)\*/.*/go;
    # {abc,def} => (abc|def)
    while ( $pattern =~ /(?<!\\){.*?(?<!\\)}/o ) {
        my ($curly) = $pattern =~ /((?<!\\){.*?(?<!\\)})/o;
        $curly =~ s/(?<!\\),/|/go;
        $curly =~ s/^(?<!\\){/(/o;
        $curly =~ s/(?<!\\)}$/)/o;
        $pattern =~ s/(?<!\\){.*?(?<!\\)}/$curly/o;
    }

    return RegexpGlob($config, $pattern);
}

sub next {
    my ($self, $batchsize) = @_;
    $batchsize //= 1;
    my @batch = ();

    while( scalar(@batch) < $batchsize && scalar(@{$self->{'batch'}}) > 0 ) {
        push (@batch, pop (@{$self->{'batch'}}));
    }

    if ( ( $self->{'return_queue'}->pending + $self->{'submitted'} - $self->{'completed'} ) == 0 ) {
        $self->{'reactor'}->finish() if ! $self->{'finished'};
        $self->{'finished'} = 1;
    }

    if ( scalar(@batch) >= $batchsize || $self->{'finished'} ) {
        return @batch;
    }

    # grab everything that hasn't been scheduled yet and schedule it
    while ( $self->{'return_queue'}->pending() ) {
        my $paths = $self->{'return_queue'}->dequeue();
        $self->{'completed'}++;
        for my $path (@{$paths}) {
            $self->__EVAL_PATH($path);
        }
    }

    while ( ( $self->{'return_queue'}->pending + $self->{'submitted'} - $self->{'completed'} ) > 0 && scalar(@{$self->{'batch'}}) < $batchsize ) {
        # Wait on the return queue for something to finish
        my $paths = $self->{'return_queue'}->dequeue();
        $self->{'completed'}++;
        for my $path (@{$paths}) {
            $self->__EVAL_PATH($path);
        }
    }

    while( scalar(@batch) < $batchsize && scalar(@{$self->{'batch'}}) > 0 ) {
        push (@batch, pop (@{$self->{'batch'}}));
    }

    if (  scalar(@batch) == 0 && ( $self->{'return_queue'}->pending + $self->{'submitted'} - $self->{'completed'} ) == 0 ) {
        $self->{'reactor'}->finish() if ! $self->{'finished'};
        $self->{'finished'} = 1;
    }

    return @batch;
}

sub scram {
    my ($self) = @_;
    $self->{'reactor'}->scram();
    $self->{'finished'} = 1;
}


# Some bugs related to '.' characters, don't use them in paths
# TODO: fix regexps' to detect escape characters using lookbehind
sub __EVAL_PATH {
    my ($self, $path) = @_;

    if ($path !~ $metare) {
        $path =~ s/\/+$//o; # strip trailing slashes
        $path =~ s/\\\././go; # unescape dots
        push $self->{'batch'}, $path;
        return;
    }

    my @rhs = split(/\//, $path);
    my $lhs = "";

    foreach my $i (0..$#rhs) {
        if ( $rhs[$i] =~ $metare ) {
            next if $rhs[$i] =~ /^\.$/o; # Special case './'
            my $new_rhs = join('/',@rhs[$i+1..$#rhs]) || "";
            $self->{'submitted'}++;
            $self->{'reactor'}->push({'task'=>'expand_path', 'pattern'=>$rhs[$i], 'lhs'=>$lhs, 'rhs'=>$new_rhs});
            return;
        }
        $lhs .= "/$rhs[$i]/";
        $lhs =~ s/\/\//\//go;
    }
    return;
}

1;
