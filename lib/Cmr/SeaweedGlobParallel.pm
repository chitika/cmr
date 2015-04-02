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

package Cmr::SeaweedGlobParallel;

our $VERSION = '0.1';

use strict;
use warnings;

use threads::shared;

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__))."/..";

use Cmr::Index::Redis ();
use Cmr::ReactorAsync();

our $metare  = qr/(?<!\\)([\|\(\)\[\{\^\$\*\+\?\.])/;

sub Init {
    my ($config, $db, $prefix) = @_;
    my $self = {
        'config'    => $config,
        'db'        => $db,
        'prefix'    => $prefix,
    };

    return bless ($self);
}

sub RegexpGlob {
    my ($self, $pattern, $fields) = @_;

    my $i = 0;
    my $fields_index;
    for my $field (@$fields) {
        $fields_index->{$field} = $i++;
    }

    my $ev = Cmr::ReactorAsync::new( $self->{'config'}, $Cmr::Index::Redis::handlers );
    $ev->push({'task'=>'index_traverse', 'db'=>$self->{'db'}, 'prefix'=>$self->{'prefix'}, 'path'=>$pattern, 'fields'=>$fields_index});

    return bless( {
        'reactor'  => $ev,
        'finished' => 0,
    });
}

sub PosixGlob {
    my ($self, $pattern, $fields) = @_;

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

    return RegexpGlob($self, $pattern, $fields);
}

sub pop {
    my ($self, $batchsize) = @_;
    $batchsize //= 1;
    my @batch = ();

    while ( scalar(@batch) < $batchsize ) {
        last if $self->{finished};

        my $pending = $self->{reactor}->pending();
        my @values = $self->{reactor}->pop( $batchsize - scalar(@batch) );

        unless ($pending or scalar(@values)) {
            $self->{reactor}->finish();
            $self->{finished} = 1;
            last;
        }

        if (@values) {
            @batch = (@batch, @values);
        }
        else {
            Time::HiRes::nanosleep(1e8);
        }
    }

    return @batch;
}

sub scram {
    my ($self) = @_;
    $self->{reactor}->scram();
    $self->{finished} = 1;
}

1;
