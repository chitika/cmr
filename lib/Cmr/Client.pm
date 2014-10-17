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

package Cmr::Client;

use strict;
use warnings;

our $VERSION = 1.0.0;
sub Version { return $VERSION }

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__));

use Cmr::ClientReactor();
use Cmr::GlusterGlobAsync ();
use Cmr::GlusterUtils ();
use Date::Manip ();

use Fcntl qw/O_RDONLY O_WRONLY O_CREAT O_BINARY/;

use constant {
    SUCCESS => 0,
    FAIL => 1,
};


sub new {
    my ($config) = @_;

    my $obj = {
        'config'  => $config,
        'reactor' => &Cmr::ClientReactor::new($config),
        'globber' => &Cmr::GlusterGlobAsync::new($config),
        'scram'   => 0,
        'finish'  => 0,
        'failed'  => 0,
    };

    return bless($obj);
}

sub failed {
    my ($self) = @_;
    return $self->{'failed'};
}


sub fail {
    my ($self) = @_;

    print STDERR "\nJob Failed!\n\n" unless $self->{'failed'};
    $self->{'failed'} = 1;

    return &FAIL;
}


sub scram {
    my ($self) = @_;
    my $result = $self->fail();

    return $result if $self->{'scram'};

    $self->{'scram'} = 1;
    $self->{'reactor'}->scram();
    $self->{'globber'}->scram();

    return $result;
}


sub finish {
    my ($self) = @_;

    unless ( $self->{'finish'} or $self->{'scram'} ) {
        $self->{'finish'} = 1;
        $self->{'reactor'}->sync();
        $self->{'reactor'}->push({'type' => &Cmr::Types::CMR_CLEANUP_TEMPORARY});
        $self->{'reactor'}->finish();
        $self->{'globber'}->finish();
    }

    if ( $self->{'failed'} ) {
        return $self->fail();
    }

    return &SUCCESS;
}


sub _reduce_input_set {
  my ($input) = @_;

  if (scalar(@$input) == 1) { return @$input; }

  my $pattern_hash = {};
  for my $pattern (@{$input}) {
      my @elems = split(/\/(?=[^\/]*$)/, $pattern);
      my ($key, $file) = @elems;
      $pattern_hash->{$key} //= [];
      push @{$pattern_hash->{$key}}, $file;
  }

  my @patterns = ();
  for my $hash (keys %$pattern_hash) {
      push @patterns,  "${hash}/{".join(',',@{$pattern_hash->{$hash}})."}";
  }
  return @patterns;
}


sub grep {
    my ($self, %kwargs) = @_; 

    my %defaults = (
        'prefix'            => 'grep',
        'batch_size'        => 1,
        'batch_multiplier'  => 1,
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "\nJob failed: No output path specified\n";
        return $self->fail();
    }


    print STDERR "Grep Started\n" if $args{'verbose'};

    my $part_id = 0;
    my @paths = _reduce_input_set($args{'input'});

    for my $path (@paths) {
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};

        $path =~ s/^(?!$args{'basepath'})/$args{'basepath'}\//o;
        my $glob = $self->{'globber'}->PosixGlob($path);

        while ( my ($ext, $batch) = $glob->next($batchsize) ) {
            map { s/^$args{'basepath'}//o; $_; } @$batch;

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_GREP,
                'patterns'      =>  $args{'patterns'},
                'input'         =>  $batch,
                'destination'   =>  sprintf("%s/%s-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_id),
                'ext'           =>  $ext,
                'flags'         =>  $args{'flags'}
            });

            return $self->fail() if $self->{'reactor'}->failed();

            $part_id++;
        }
    }


    print STDERR "waiting for all tasks to finish\n" if $args{'verbose'};

    $self->{'reactor'}->sync();
    my @output = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();
    return $self->fail() if $self->{'reactor'}->failed();


    print STDERR "merging output files\n" if $args{'verbose'};
    if ( $args{'do_hierarchical_merge'} ) {
        $self->hierarchical_merge("input"=>\@output);
        return $self->fail() if $self->{'reactor'}->failed();
    } else {
        $self->sequential_merge("input"=>\@output);
        # reactor isn't used for simple merge
    }

    print STDERR "Grep Finished\n" if $args{'verbose'};
    return &SUCCESS;
}


sub hierarchical_reduce {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'reduce',
        'reduce_batch_size' => 40,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my @input = @{$args{'input'}};
    my $index = 0;
    my $part_id = 0;
    my $part_depth = 0;

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "Job failed: No output path specified\n";
        return;
    }

    print STDERR "performing hierarchical reduce\n";
    while ( $#input >= $args{'reduce_batch_size'} ) {
        $index = 0;
        $part_id = 0;

        while ( $index <= $#input ) {

            my $eindex = $index + $args{'reduce_batch_size'} - 1;
            if ( $eindex > $#input ) { 
                $eindex = $#input;
            }

            my @task_files = @input[$index..$eindex];
            map { s/^$args{'basepath'}//o; $_; } @task_files;

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_STREAM,
                'reducer'       =>  $args{'reducer'},
                'input'         =>  \@task_files,
                'destination'   =>  sprintf("%s/%s-%d-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_depth, $part_id),
            });

            $part_id++;
            $index += $args{'reduce_batch_size'};
        }


        $self->{'reactor'}->sync();
        my @output = $self->{'reactor'}->get_job_output();
        $self->{'reactor'}->clear_job_output();

        return $self->fail() if $self->{'reactor'}->failed();

        if ( @input ) {
            $self->cleanup('input'=>\@input);
        }

        @input = @output;
        $part_depth++;
    } 

    my $final_reducer = $args{'final_reducer'} // $args{'reducer'};

    if ($#input >= 0) {
        print STDERR "Starting Final Reduce\n";

        my @task_files = @input[0..$#input];
        map { s/^$args{'basepath'}//o; $_; } @task_files;

        $self->{'reactor'}->push({
            'type'          =>  &Cmr::Types::CMR_STREAM,
            'reducer'       =>  $final_reducer,
            'input'         =>  \@task_files,
            'destination'   =>  sprintf("%s/%s", $self->{'reactor'}->{'output_path'}, $args{'outfile'}),
        });

        $self->{'reactor'}->sync();
        $self->{'reactor'}->clear_job_output();

        return $self->fail() if $self->{'reactor'}->failed();

        $self->cleanup('input'=>\@input);
    }

    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    print STDERR "Finished Reduce\n";

    if ($#input < 0) {
        # No output
        print STDERR "Reduce produced no output\n";
    }


    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}


sub sequential_merge {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'merge',
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );
    my @input        = @{$args{'input'}};

    print STDERR "Performing Sequential Merge\n";
    sysopen(OUT, sprintf("%s/%s", $self->{'reactor'}->{'output_path'}, $args{'outfile'}), O_WRONLY|O_CREAT|O_BINARY);
    for my $file (@input) {
        sysopen(IN, $file, O_RDONLY|O_BINARY );
        my $size = -s IN;
        sysread(IN, my $buf, $size);
        syswrite(OUT, $buf, $size);
        close(IN);
    }

    print STDERR "Finished Merge\n";
    return &SUCCESS;
}


sub hierarchical_merge {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'merge',
        'merge_batch_size'  => 25,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my @input        = @{$args{'input'}};

    my $index = 0;
    my $part_id = 0;
    my $part_depth = 0;

    print STDERR "performing hierarchical merge\n";
    while ( $#input >= $args{'merge_batch_size'} ) {
        $index = 0;
        $part_id = 0;

        while ( $index <= $#input ) {

            my $eindex = $index + $args{'merge_batch_size'}-1;
            if ( $eindex > $#input ) {
                $eindex = $#input;
            }

            my @task_files = @input[$index..$eindex];
            map { s/^$args{'basepath'}//o; $_; } @task_files;

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_MERGE,
                'input'         =>  \@task_files,
                'destination'   =>  sprintf("%s/%s-%d-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_depth, $part_id),
            });

            $part_id += 1;
            $index   += $args{'merge_batch_size'};
        }


        $self->{'reactor'}->sync();
        my @output = $self->{'reactor'}->get_job_output();
        $self->{'reactor'}->clear_job_output();
        return $self->fail() if $self->{'reactor'}->failed();


        if ( @input ) {
            $self->cleanup('input'=>\@input);
        }

        @input = @output;
        $part_depth++;
    }

    if ($#input >= 0) {
        print STDERR "Starting Final Merge\n";

        my @task_files = @input[0..$#input];
        map { s/^$args{'basepath'}//o; $_; } @task_files;

        $self->{'reactor'}->push({
            'type'          =>  &Cmr::Types::CMR_MERGE,
            'input'         =>  \@task_files,
            'destination'   =>  sprintf("%s/%s", $self->{'reactor'}->{'output_path'}, $args{'outfile'}),
        });

        $self->{'reactor'}->sync();
        $self->{'reactor'}->clear_job_output();
        return $self->fail() if $self->{'reactor'}->failed();

        $self->cleanup('input'=>\@input);
    }

    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    print STDERR "Finished Merge\n";

    if ($#input < 0) {
        print STDERR "Merge produced no output\n";
    }


    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}


sub stream {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'stream',
        'batch_size'        => 1,
        'batch_multiplier'  => 1,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my $part_id = 0;
    my $part_depth = 0;

    my $reducer = $args{'initial-reducer'} // $args{'reducer'};

    my $fixed_args = {};
    $fixed_args->{'type'}    = &Cmr::Types::CMR_STREAM;
    $fixed_args->{'mapper'}  = $args{'mapper'}  if exists $args{'mapper'};
    $fixed_args->{'reducer'} = $reducer if defined $reducer;

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "Job failed: No output path specified\n";
        return;
    }

    my @paths = _reduce_input_set($args{'input'});
    for my $path (@paths) {
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};

        $path =~ s/^(?!$args{'basepath'})/$args{'basepath'}\//o;
        my $glob = $self->{'globber'}->PosixGlob($path);
        while ( my ($ext, $batch) = $glob->next($batchsize) ) {
            map { s/^$args{'basepath'}//o; $_; } @$batch;

            my $task_args = {
                'input'         =>  $batch,
                'ext'           =>  $ext,
                'destination'   =>  sprintf("%s/%s-%d-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_depth, $part_id),
            };

            my $cmd = {%$fixed_args, %$task_args};
            $self->{'reactor'}->push($cmd);

            return $self->fail() if $self->{'reactor'}->failed();

            $part_id++;
        }
    }

    print STDERR "waiting for all tasks to finish\n";
    $self->{'reactor'}->sync();
    my @output = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();

    return $self->fail() if $self->{'reactor'}->failed();

    if ($args{'reducer'}) {
        $self->hierarchical_reduce('reducer'=>$args{'reducer'}, 'input'=>\@output);
    }

    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}


sub merge_buckets {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'merge_bucket',
        'merge_batch_size'  => 25,
        'in_order'          => 0,
        'delimiter'         => "",
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my $input       = $args{'input'};
    my $batchsize   = $args{'merge_batch_size'};

    my @cleanup_files;

    my $part_id = 0;
    my $part_depth = 0;
    my $done = 0;
    while ( not $done ) {
        my %bucket_map;

        for my $i (0 .. $#{$input}) {
            my $bucket = $input->[$i];

            my $start_index = 0;
            my $part_id = 0;

            for my $file (@$bucket) {
                push @cleanup_files, $file;
            }

            while ( $start_index <= $#{$bucket} ) {
                my $end_index = $start_index + $args{'merge_batch_size'}-1;
                if ( $end_index > $#{$bucket} ) {
                    $end_index = $#{$bucket};
                }

                my @task_files = @{$bucket}[ $start_index .. $end_index ];
                map { s/^$args{'basepath'}//o; $_; } @task_files;

                my $file = sprintf("%s/%s-%d-%d-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_id, $part_depth, $i);

                $self->{'reactor'}->push({
                    'type'          =>  &Cmr::Types::CMR_MERGE,
                    'input'         =>  \@task_files,
                    'destination'   =>  $file,
                    'in_order'      =>  $args{'in_order'},
                    'delimiter'     =>  $args{'delimiter'},
                });

                # Keep a mapping files -> buckets
                $bucket_map{$file} = $i;

                $start_index += $batchsize;
                $part_id++;
            }
            $input->[$i] = [];
        }

        $self->{'reactor'}->sync();
        my @new_input_files = $self->{'reactor'}->get_job_output();
        $self->{'reactor'}->clear_job_output();

        for my $file (@new_input_files) {
            # Match up each file with its originating bucket
            if ( defined ($bucket_map{$file}) ) {
                push @{$input->[$bucket_map{$file}]}, $file;
            }
            else {
                # This should never happen...
                print STDERR "Why does this keep happening!?\n";
            }
        }

        $part_depth++;

        # Assume done
        $done = 1;
        for my $i (0 .. $#{$input}) {
            if ( scalar(@{$input->[$i]}) > 1 ) {
                # Set not done if any of the buckets contain more than one file
                $done = 0;
                last;
            }
        }
    }

    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    print STDERR "done merge\n";

    $self->cleanup('input'=>\@cleanup_files);
    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    print STDERR "done cleanup\n";

    return $input;
}


sub reduce_buckets {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'reduce_bucket',
        'reduce_batch_size' => 40,
        'final_reduce'      => 0,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );
    my $input           = $args{'input'};
    my $batchsize       = $args{'reduce_batch_size'};
    my $part_depth      = 0;

    my %bucket_map;
    my @cleanup_files;

    my $done = 0;
    while (!$done) {
        for my $i (0 .. $#{$input}) {

            my $bucket = $input->[$i];
            my $start_index = 0;
            my $part_id = 0;

            for my $file (@$bucket) {
                push @cleanup_files, $file;
            }

            my $last_reduce = 0;
            if ( $#{$bucket} < $batchsize ) {
                $last_reduce = 1;
            }

            while ( $start_index <= $#{$bucket} ) {
                my $end_index = $start_index + $batchsize-1;

                if ( $end_index > $#{$bucket} ) {
                    $end_index = $#{$bucket};
                }

                my @task_files = @{$bucket}[ $start_index .. $end_index ];
                map { s/^$args{'basepath'}//o; $_; } @task_files;

                my $file = sprintf("%s/%s-%d-%d-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_id, $part_depth, $i);

                if ( $args{'final_reduce'} and $last_reduce and $args{'final_reducer'} ) {
                    $self->{'reactor'}->push({
                        'type'          =>  &Cmr::Types::CMR_STREAM,
                        'reducer'       =>  $args{'final_reducer'},
                        'input'         =>  \@task_files,
                        'destination'   =>  $file,
                    });
                }
                else {
                    $self->{'reactor'}->push({
                        'type'          =>  &Cmr::Types::CMR_STREAM,
                        'reducer'       =>  $args{'reducer'},
                        'input'         =>  \@task_files,
                        'destination'   =>  $file,
                    });
                }

                # Keep a mapping files -> buckets
                $bucket_map{$file} = $i;

                $start_index += $batchsize;
                $part_id++;
            }

            $input->[$i] = [];
        }

        $self->{'reactor'}->sync();
        my @new_input_files = $self->{'reactor'}->get_job_output();
        $self->{'reactor'}->clear_job_output();

        for my $file (@new_input_files) {
            if ( defined ($bucket_map{$file}) ) {
                push @{$input->[$bucket_map{$file}]}, $file;
            }
            else {
                # This should never happen...
                print STDERR "Why does this keep happening!?\n";
            }
        }

        $part_depth++;

        $done = 1;
        for my $i (0 .. $#{$input}) {
            if ( scalar(@{$input->[$i]}) > 1 ) {
                $done = 0;
            }
        }
    }

    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    $self->cleanup('input'=>\@cleanup_files);
    $self->{'reactor'}->sync();
    $self->{'reactor'}->clear_job_output();

    return $input;
}

sub bucket_stream {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'bucket',
        'batch_size'        => 16,
        'batch_multiplier'  => 1,
        'buckets'           => 16,
        'delimiter'         => "",
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "\nJob failed: No output path specified\n";
        return $self->fail();
    }

    my $map_id = 0;
    my @paths = _reduce_input_set($args{'input'});
    for my $path (@paths) {
    
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};
        $path =~ s/^(?!$args{'basepath'})/$args{'basepath'}\//o;
        my $glob = $self->{'globber'}->PosixGlob($path);
        
        while ( my ($ext, $batch) = $glob->next($batchsize) ) {
            map { s/^$args{'basepath'}//o; $_; } @$batch;

            my $file = sprintf("%s/this_is_a_bit_of_a_hack", $self->{'reactor'}->{'output_path'});

            $self->{'reactor'}->push({
                'type'                  => &Cmr::Types::CMR_BUCKET,
                'mapper'                => $args{'mapper'},
                'buckets'               => $args{'buckets'},
                'delimiter'             => $args{'delimiter'},
                'input'                 => $batch,
                'ext'                   => $ext,
                'map_id'                => $map_id,
                'destination'           => $file,
            });

            return $self->fail() if $self->{'reactor'}->failed();
            $map_id++;
        }
    }

    $self->{'reactor'}->sync();
    my @outputs = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();


    # -- Merge files ( in order merge )
    my $merged_buckets  = $self->merge_buckets('input'=>\@outputs, 'in_order'=>1);
    return $self->fail() if $self->{'reactor'}->failed();

    if ($args{'reducer'}) {
        my $reduced_buckets = $self->reduce_buckets('input'=>$merged_buckets, 'reducer'=>$args{'reducer'}, 'final_reduce'=>1);
        return $self->fail() if $self->{'reactor'}->failed();

        my @reduced_files;
        for my $bucket (@$reduced_buckets) {
            for my $file (@$bucket) {
                push @reduced_files, $file;
            }
        }

        print STDERR "merging output files\n" if $args{'verbose'};
        if ( $args{'do_hierarchical_merge'} ) {
            $self->hierarchical_merge("input"=>\@reduced_files);
            return $self->fail() if $self->{'reactor'}->failed();
        } else {
            $self->sequential_merge("input"=>\@reduced_files);
            # reactor isn't used for simple merge
        }

        return $self->fail() if $self->{'reactor'}->failed();
    }

    return &SUCCESS;
}


sub bucket_join {
my ($self, %kwargs) = @_;

    my %defaults = (
        'num_buckets'       => 8,
        'batch_size'        => 16,
        'batch_multiplier'  => 1,
        'delimiter'         => '',
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "\nJob failed: No output path specified\n";
        return $self->fail();
    }
    
    my $map_id = 0;
    my @paths = _reduce_input_set($args{'input'});
    for my $path (@paths) {
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};
        $path =~ s/^(?!$args{'basepath'})/$args{'basepath'}\//o;
        my $glob = $self->{'globber'}->PosixGlob($path);
        while ( my ($ext, $batch) = $glob->next($batchsize) ) {
            map { s/^$args{'basepath'}//o; $_; } @$batch;

            my $not_a_real_file = sprintf("%s/this_is_a_bit_of_a_hack", $self->{'reactor'}->{'output_path'});

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_BUCKET,
                'mapper'        =>  $args{'mapper'},
                'buckets'       =>  $args{'num_buckets'},
                'delimiter'     =>  $args{'delimiter'},
                'join'          =>  1,
                'input'         =>  $batch,
                'ext'           =>  $ext,
                'map_id'        =>  $map_id,
                'destination'   =>  $not_a_real_file,
                'prefix'        => 'bucket',
            });

            $self->fail() if $self->{'reactor'}->failed();
            $map_id++;
        }
    }

    $self->{'reactor'}->sync();
    my @outputs = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();


    # -- Merge files ( in order merge )
    my $merged_buckets  = $self->merge_buckets('input'=>\@outputs, 'in_order'=>1);
    return $self->fail() if $self->{'reactor'}->failed();

    # -- Reduce files :
    # For each secondary key range invoke the join-reducer
    my $reduced_buckets = $self->reduce_buckets('input'=>$merged_buckets, 'reducer'=>$args{'join_reducer'}, 'prefix'=>'join_reduce', 'join'=>1);
    return $self->fail() if $self->{'reactor'}->failed();

    my @reduced_files;
    for my $bucket (@$reduced_buckets) {
        for my $file (@$bucket) {
            push @reduced_files, $file;
        }
    }

    # We're not done yet, time to go through the entire process again...
    # -- Rebucket files ( this time on primary key )
    $map_id = 0;
    for my $file (@reduced_files) {
        $file =~ s/^$args{'basepath'}//o;
        my $not_a_real_file = sprintf("%s/this_is_a_bit_of_a_hack", $self->{'reactor'}->{'output_path'});
   
        $self->{'reactor'}->push({
            'type'          =>  &Cmr::Types::CMR_BUCKET,
            'buckets'       =>  $args{'num_buckets'},
            'join'          =>  $args{'join'},
            'strip_joinkey' =>  1,
            'delimiter'     =>  $args{'delimiter'},
            'input'         =>  [$file],
            'map_id'        =>  $map_id,
            'destination'   =>  $not_a_real_file,
            'prefix'        => 'rebucket',
        });

        $self->fail() if $self->{'reactor'}->failed();
        $map_id++;
    }

    $self->{'reactor'}->sync();
    @outputs = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();

    # -- Merge files ( in order merge )
    $merged_buckets  = $self->merge_buckets('input'=>\@outputs, 'in_order'=>1);
    return $self->fail() if $self->{'reactor'}->failed();

    # -- Reduce files :
    # For each secondary key range invoke the join-reducer
    $reduced_buckets = $self->reduce_buckets('input'=>$merged_buckets, 'reducer'=>$args{'reducer'}, 'prefix'=>'final_reduce', 'final_reduce'=>1);
    return $self->fail() if $self->{'reactor'}->failed();

    @reduced_files = ();
    for my $bucket (@$reduced_buckets) {
        for my $file (@$bucket) {
            push @reduced_files, $file;
        }
    }

    # -- Merge the partitions
    print STDERR "merging output files\n" if $args{'verbose'};
    if ( $args{'do_hierarchical_merge'} ) {
        $self->hierarchical_merge("input"=>\@reduced_files);
        return $self->fail() if $self->{'reactor'}->failed();
    } else {
        $self->sequential_merge("input"=>\@reduced_files);
        # reactor isn't used for simple merge
    }

    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}

sub cleanup {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'                => 'cleanup',
        'cleanup_batch_size'    => 25,
        'outfile'               => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my $part_id = 0;
    my @input = @{$args{'input'}};
    my $index = 0;

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "Job failed: No output path specified\n";
        return;
    }

    while ( $index <= $#input ) {

        my $eindex = $index + $args{'cleanup_batch_size'}-1;
        if ( $eindex > $#input ) {
            $eindex = $#input;
        }

        my @task_files = @input[$index..$eindex];
        map { s/^$args{'basepath'}//o; $_; } @task_files;

        $self->{'reactor'}->push({
            'type'          =>  &Cmr::Types::CMR_CLEANUP,
            'input'         =>  \@task_files,
            'destination'   =>  sprintf("%s/%s-%d", $self->{'reactor'}->{'output_path'}, $args{'prefix'}, $part_id),
        });

        $part_id++;
        $index += $args{'cleanup_batch_size'};
    }

    # NOTE: cleanup does not sync

    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}


sub status {
    my ($self) = @_;
    $self->{'reactor'}->get_status();
}

sub server_status {
    my ($self) = @_;
    $self->{'reactor'}->get_server_status();
}

1;
