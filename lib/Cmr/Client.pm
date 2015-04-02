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
use lib dirname (abs_path(__FILE__))."/..";

use Cmr::ClientReactor();
use Cmr::SeaweedGlob ();
use Cmr::GlusterUtils ();
use Cmr::Seaweed ();

use Date::Manip ();

use Fcntl qw/O_RDONLY O_WRONLY O_CREAT O_BINARY/;

use constant {
    SUCCESS => 0,
    FAIL => 1,
};


sub new {
    my ($config) = @_;

    my ($user, undef, undef, undef, undef, undef, undef, undef, undef) = getpwuid($<);

    my $obj = {
        'config'  => $config,
        'reactor' => &Cmr::ClientReactor::new($config),
        'globbers' => {
            'warehouse' => &Cmr::SeaweedGlob::new($config, 0, "warehouse"),
            'user'      => &Cmr::SeaweedGlob::new($config, 2, "user/${user}"),
        },
        'warehouse_index' => &Cmr::Seaweed::new(redis_addr=>$config->{redis_addr}, seaweed_addr=>$config->{seaweed_addr}),
        'job_index'       => &Cmr::Seaweed::new(redis_addr=>$config->{redis_addr}, seaweed_addr=>$config->{seaweed_addr}, db=>1, prefix=>"job"),
        'user_index'      => &Cmr::Seaweed::new(redis_addr=>$config->{redis_addr}, seaweed_addr=>$config->{seaweed_addr}, db=>2, prefix=>"user/${user}"),
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
    for my $key (keys %{$self->{'globbers'}}) {
        $self->{'globbers'}->{$key}->scram();
    }

    return $result;
}


sub finish {
    my ($self) = @_;

    unless ( $self->{'finish'} or $self->{'scram'} ) {
        $self->{'finish'} = 1;
        $self->{'reactor'}->sync();
#        $self->{'reactor'}->push({'type' => &Cmr::Types::CMR_CLEANUP_TEMPORARY});
        $self->{'reactor'}->finish();
        for my $key (keys %{$self->{'globbers'}}) {
            $self->{'globbers'}->{$key}->finish();
        }
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

    print STDERR "Grep Started\n" if $args{'verbose'};

    my $part_id = 0;
#    my @paths = _reduce_input_set($args{'input'});
    my @paths = @{$args{'input'}};

    for my $path (@paths) {
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};

        for my $globber ('warehouse','user') {
            my $glob    = $self->{globbers}->{$globber}->PosixGlob($path);

            my $num_globs = 0;
            while ( my ($ext, $batch) = $glob->pop($batchsize) ) {

                $self->{'reactor'}->push({
                    'type'          =>  &Cmr::Types::CMR_GREP,
                    'patterns'      =>  $args{'patterns'},
                    'input'         =>  $batch,
                    'output'   => sprintf("%s.%s", $args{'prefix'}, $part_id),
                    'ext'           =>  'gz',
                    'flags'         =>  $args{'flags'}
                });

                return $self->fail() if $self->{'reactor'}->failed();

                $num_globs++;
                $part_id++;
            }
            last if $num_globs > 0;
        }
    }


    print STDERR "waiting for all tasks to finish\n" if $args{'verbose'};

    $self->{'reactor'}->sync();
#    my @output = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();
    return $self->fail() if $self->{'reactor'}->failed();

    # TODO: don't grab all of this at once... need a cursor
    my @results = $self->{'job_index'}->get_pattern("$self->{'reactor'}->{'jid'}/.*");


    if ($self->{'reactor'}->{'output_path'}) {
        open(OUT, '>'."$self->{'reactor'}->{'output_path'}");
        for my $result (@results) {
            print OUT $$result;
        }
        close(OUT);
    }
    else {
        for my $result (@results) {
            print $$result;
        }
    }

    print STDERR "Grep Finished\n" if $args{'verbose'};
    return &SUCCESS;
}


sub hierarchical_reduce {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'reduce',
        'reduce_batch_size' => 20,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my @input = @{$args{'input'}};
    my $index = 0;
    my $part_id = 0;
    my $part_depth = 0;

    print STDERR "performing hierarchical reduce\n";
    while ( $#input >= $args{'reduce_batch_size'} ) {
        $index = 0;
        $part_id = 0;

        $self->{'reactor'}->clear_job_output();

        while ( $index <= $#input ) {

            my $eindex = $index + $args{'reduce_batch_size'} - 1;
            if ( $eindex > $#input ) { 
                $eindex = $#input;
            }

            my @task_files = @input[$index..$eindex];
            my @batch = map { $self->{'job_index'}->get_locations("$self->{'reactor'}->{'jid'}/$_"); } @task_files;

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_STREAM,
                'reducer'       =>  $args{'reducer'},
                'input'         =>  \@batch,
                'output'        => sprintf("%s.%d.%d", $args{'prefix'}, $part_depth, $part_id),
            });

            $part_id++;
            $index = $eindex+1;
        }


        $self->{'reactor'}->sync();
        my @output = $self->{'reactor'}->get_job_output();

        return $self->fail() if $self->{'reactor'}->failed();

        @input = @output;
        $part_depth++;
    } 

    my $final_reducer = $args{'final_reducer'} // $args{'reducer'};

    if ($#input >= 0) {
        print STDERR "Starting Final Reduce\n";

        $self->{'reactor'}->clear_job_output();

        my @task_files = @input[0..$#input];
        my @batch = map { $self->{'job_index'}->get_locations("$self->{'reactor'}->{'jid'}/$_"); } @task_files;

        my $task = {
            'type'          =>  &Cmr::Types::CMR_STREAM,
            'reducer'       =>  $final_reducer,
            'input'         =>  \@batch,
            'output'        => sprintf("%s.%d.%d", $args{'prefix'}, $part_depth, $part_id),
        };

        $task->{'persist'} = $args{'persist'} if exists $args{'persist'};
        $self->{'reactor'}->push($task);
        $self->{'reactor'}->sync();
        return $self->fail() if $self->{'reactor'}->failed();
    } else {
        print STDERR "Reduce produced no output\n";
    }

    $self->{'reactor'}->sync();
    print STDERR "Finished Reduce\n";

    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}


sub sequential_merge {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'merge',
        'outfile'           => 'output',
    );

    #FIXME This is just broken right now

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
            my @batch = map { $self->{'job_index'}->get_locations("$self->{'reactor'}->{'jid'}/$_"); } @task_files;

#            map { s/^$args{'basepath'}//o; $_; } @task_files;

            $self->{'reactor'}->push({
                'type'          =>  &Cmr::Types::CMR_MERGE,
                'input'         =>  \@batch,
                'output'        =>  sprintf("%s.%d.%d", $args{'prefix'}, $part_depth, $part_id),
            });

            $part_id += 1;
            $index   += $args{'merge_batch_size'};
        }


        $self->{'reactor'}->sync();
        my @output = $self->{'reactor'}->get_job_output();
        $self->{'reactor'}->clear_job_output();
        return $self->fail() if $self->{'reactor'}->failed();

        @input = @output;
        $part_depth++;
    }

    if ($#input >= 0) {
        print STDERR "Starting Final Merge\n";

        my @task_files = @input[0..$#input];
#        map { s/^$args{'basepath'}//o; $_; } @task_files;

        my @batch = map { $self->{'job_index'}->get_locations("$self->{'reactor'}->{'jid'}/$_"); } @task_files;

        $self->{'reactor'}->push({
            'type'          =>  &Cmr::Types::CMR_MERGE,
            'input'         =>  \@batch,
            'output'        =>  sprintf("%s", $args{'outfile'}),
        });

        $self->{'reactor'}->sync();
        $self->{'reactor'}->clear_job_output();
        return $self->fail() if $self->{'reactor'}->failed();
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

    my @paths = @{$args{'input'}};

    for my $path (@paths) {
        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};

        for my $globber ('warehouse','user') {
            my $glob    = $self->{globbers}->{$globber}->PosixGlob($path);

            my $num_globs = 0;
            while ( my ($ext, $batch) = $glob->pop($batchsize) ) {
                my $task_args = {
                    'input'         =>  $batch,
                    'ext'           =>  'gz',
                    'output'        =>  sprintf("%s.%d.%d", $args{'prefix'}, $part_depth, $part_id),
                };

                if (!$reducer and $args{'persist'}) {
                    # If there is no reducer, and data is meant to persist in seaweed, then when we complete these tasks we're done processing.
                    $task_args->{'persist'} = $args{'persist'};
                }

                my $cmd = {%$fixed_args, %$task_args};
                $self->{'reactor'}->push($cmd);
                return $self->fail() if $self->{'reactor'}->failed();

                $num_globs++;
                $part_id++;
            }
            last if $num_globs > 0;

        }
    }

    print STDERR "waiting for all tasks to finish\n";
    $self->{'reactor'}->sync();
    my @output = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();

    return $self->fail() if $self->{'reactor'}->failed();

    if ($args{'reducer'}) {
        $self->hierarchical_reduce('reducer'=>$args{'reducer'}, 'input'=>\@output);
        unless ($args{'persist'}) {
            my @outputs = $self->{'reactor'}->get_job_output();
            for my $output (@outputs) {
                print STDERR "retrieving data for job $self->{'reactor'}->{'jid'}\n";
                my @results = $self->{'job_index'}->get_pattern("$self->{'reactor'}->{'jid'}/$output");

                if ($self->{'reactor'}->{'output_path'}) {
                    open(OUT, '>'."$self->{'reactor'}->{'output_path'}");
                    for my $result (@results) {
                        print OUT $$result;
                    }
                    close(OUT);
                }
                else {
                    for my $result (@results) {
                        print $$result;
                    }
                }
            }
        }
    } else {
        # MAPPER ONLY
        unless ($args{'persist'}) {
            my @mappings = $self->{'job_index'}->get_mapping("$self->{'reactor'}->{'jid'}/.*");

            if ( $self->{'reactor'}->{'output_path'} ) {
                # OUTPUT -> output_path
                system("mkdir -p $self->{'reactor'}->{'output_path'}");

                my $part_id = 0;
                for my $mapping (@mappings) {
                    my ($file, $uri, $size) = @$mapping;
                    system(qq(curl -s -H 'Accept-Encoding: gzip' ${uri} > "$self->{'reactor'}->{'output_path'}/part-${part_id}.gz"));
                    $part_id++;
                }
            }
            else {
                # OUTPUT -> STDOUT
                for my $mapping (@mappings) {
                    my ($file, $uri, $size) = @$mapping;
                    system("curl -s -H 'Accept-Encoding: gzip' ${uri} | gzip -dc");
                }
            }
        }
    }

    return $self->fail() if $self->{'reactor'}->failed();
    return &SUCCESS;
}

sub polymer {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'polymer',
        'batch_size'        => 4,
        'batch_multiplier'  => 1,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    unless ( $self->{'reactor'}->{'output_path'} ) {
        print STDERR "\nJob failed: No output path specified\n";
        return $self->fail();
    }

    my $log = Cmr::StartupUtils::get_logger();

    print STDERR "Polymer Started\n" if $args{'verbose'};

    my $output_path = $self->{'reactor'}->{'output_path'};

    my $fields        = $args{'field'};
    my @inputs        = @{$args{'input'}//[]};
    my $mapper        = $args{'mapper'}  // undef;
    my $reducer       = $args{'reducer'} // undef;
    my $include       = $args{'include'} // [];
    my $exclude       = $args{'exclude'} // [];
    my $include_range = $args{'include-range'} // [];
    my $exclude_range = $args{'exclude-range'} // [];

    # Translate field names into positions for awk's sake!
    my @print_fields;
    my @filter_only;
    my @collect_fields;
    my @filter_include;
    my @filter_exclude;
    my @filter_include_range;
    my @filter_exclude_range;
    my %field_pos;
    my $awk_pos = 1;

    for my $idx ( 0 .. $#{$fields} ) {
        my ($field) = $fields->[$idx];
        my $pos = $field_pos{$field};
        if ( ! $pos ) {
            $pos = $awk_pos++;
            $field_pos{$field} = $pos;
            push @print_fields, $pos;
            push @collect_fields, $field;
        }
    }

    for my $idx ( 0 .. $#{$include} ) {
        my ($delimiter) = $include->[$idx] =~ /[\d\w\.]+(.)?/o;
        next unless defined($delimiter);
        my ($field, $filter) = split(/$delimiter/, $include->[$idx]);
        next unless defined($field) and defined($filter);
        my $pos = $field_pos{$field};
        if ( ! $pos ) {
            $pos = $awk_pos++;
            $field_pos{$field} = $pos;
            push @filter_only, $pos;
            push @collect_fields, $field;
        }
        push @filter_include, [$pos, $filter];
    }

    for my $idx ( 0 .. $#{$exclude} ) {
        my ($delimiter) = $exclude->[$idx] =~ /[\d\w\.]+(.)?/o;
        next unless defined($delimiter);
        my ($field, $filter) = split(/$delimiter/, $exclude->[$idx]);
        next unless defined($field) and defined($filter);
        my $pos = $field_pos{$field};
        if ( ! $pos ) {
            $pos = $awk_pos++;
            $field_pos{$field} = $pos;
            push @filter_only, $pos;
            push @collect_fields, $field;
        }
        push @filter_exclude, [$pos, $filter];
    }

    for my $idx ( 0 .. $#{$include_range} ) {
        my ($delimiter) = $include_range->[$idx] =~ /[\d\w\.]+(.)?/o;
        next unless defined($delimiter);
        my ($field, $range_low, $range_high) = split(/$delimiter/, $include_range->[$idx]);
        next unless defined($field) and defined($range_low) and defined($range_high);
        my $pos = $field_pos{$field};
        if ( ! $pos ) {
            $pos = $awk_pos++;
            $field_pos{$field} = $pos;
            push @filter_only, $pos;
            push @collect_fields, $field;
        }
        push @filter_include_range, [$pos, $range_low, $range_high];
    }

    for my $idx ( 0 .. $#{$exclude_range} ) {
        my ($delimiter) = $exclude_range->[$idx] =~ /[\d\w\.]+(.)?/o;
        next unless defined($delimiter);
        my ($field, $range_low, $range_high) = split(/$delimiter/, $exclude_range->[$idx]);
        next unless defined($field) and defined($range_low) and defined($range_high);
        my $pos = $field_pos{$field};
        if ( ! $pos ) {
            $pos = $awk_pos++;
            $field_pos{$field} = $pos;
            push @filter_only, $pos;
            push @collect_fields, $field;
        }
        push @filter_exclude_range, [$pos, $range_low, $range_high];
    }
    my $fixed_args = {};

    if ( scalar(@filter_only) > 0 ) {
        # Some fields are needed to filter lines, but aren't wanted in the output
        $fixed_args->{'print-fields'} = \@print_fields;
    }
    else {
        # Whole output line is desired ( awk index 0 )
        $fixed_args->{'print-fields'} = [0];
    }

    $fixed_args->{'type'}                 = &Cmr::Types::CMR_POLYMER;
    $fixed_args->{'fields'}               = \@collect_fields;
    $fixed_args->{'filter-include'}       = \@filter_include if @filter_include;
    $fixed_args->{'filter-exclude'}       = \@filter_exclude if @filter_exclude;
    $fixed_args->{'filter-include-range'} = \@filter_include_range if @filter_include_range;
    $fixed_args->{'filter-exclude-range'} = \@filter_exclude_range if @filter_exclude_range;
    $fixed_args->{'mapper'}               = $mapper  if $mapper;
    $fixed_args->{'reducer'}              = $reducer if $reducer;

    my $part_id = 0;
    my @paths = @{$args{'input'}};

    for my $path (@paths) {
#        my $batchsize = $args{'batch_size'} * $args{'batch_multiplier'};
        my $batchsize = 10;
        for my $globber ('warehouse','user') {
            my $glob    = $self->{globbers}->{$globber}->PosixGlob($path);
            if ( $self->{'config'}->{'verbose'} ) { print STDERR "Submitting tasks\n"; }

            my $num_globs = 0;
            while ( my ($ext, $batch) = $glob->pop($batchsize) ) {
                my $task_args = {
                    'input'  => $batch,
                    'ext'    => 'gz',
                    'output' => sprintf("%s.%s", $args{'prefix'}, $part_id)
                };

                if (!$reducer and $args{'persist'}) {
                    # If there is no reducer, and data is meant to persist in seaweed, then when we complete these tasks we're done processing.
                    $task_args->{'persist'} = $args{'persist'};
                }

                my $cmd = {%$fixed_args, %$task_args};
                $self->{'reactor'}->push($cmd);
                return $self->fail() if $self->{'reactor'}->failed();

                $part_id++;
                $num_globs++;
            }
            last if $num_globs > 0;
        }
    }

    if ( $self->{'config'}->{'verbose'} ) { print STDERR "waiting for all tasks to finish\n"; }
    $self->{'reactor'}->sync();
    my @output = $self->{'reactor'}->get_job_output();
    $self->{'reactor'}->clear_job_output();
    return $self->fail() if $self->{'reactor'}->failed();

    if ($args{'reducer'}) {
        $self->hierarchical_reduce('reducer'=>$args{'reducer'}, 'input'=>\@output);
        unless ($args{'persist'}) {
            my @outputs = $self->{'reactor'}->get_job_output();
            for my $output (@outputs) {
                print STDERR "retrieving data for job $self->{'reactor'}->{'jid'}\n";
                my @results = $self->{'job_index'}->get_pattern("$self->{'reactor'}->{'jid'}/$output");

                if ($self->{'reactor'}->{'output_path'}) {
                    open(OUT, '>'."$self->{'reactor'}->{'output_path'}");
                    for my $result (@results) {
                        print OUT $$result;
                    }
                    close(OUT);
                }
                else {
                    for my $result (@results) {
                        print $$result;
                    }
                }
            }
        }
    } else {
        # MAPPER ONLY
        unless ($args{'persist'}) {
            my @mappings = $self->{'job_index'}->get_mapping("$self->{'reactor'}->{'jid'}/.*");

            if ( $self->{'reactor'}->{'output_path'} ) {
                # OUTPUT -> output_path
                system("mkdir -p $self->{'reactor'}->{'output_path'}");

                my $part_id = 0;
                for my $mapping (@mappings) {
                    my ($file, $uri, $size) = @$mapping;
                    system(qq(curl -s -H 'Accept-Encoding: gzip' ${uri} > "$self->{'reactor'}->{'output_path'}/part-${part_id}.gz"));
                    $part_id++;
                }
            }
            else {
                # OUTPUT -> STDOUT
                for my $mapping (@mappings) {
                    my ($file, $uri, $size) = @$mapping;
                    system("curl -s -H 'Accept-Encoding: gzip' ${uri} | gzip -dc");
                }
            }
        }
    }

    return $self->fail() if $self->{'reactor'}->failed();

    if ( $self->{'config'}->{'verbose'} ) { print STDERR "Polymer Finished\n"; }
    return &SUCCESS;
}


sub cleanup {
    my ($self, %kwargs) = @_;

    my %defaults = (
        'prefix'            => 'cleanup',
        'batch_size'        => 1,
        'batch_multiplier'  => 1,
        'outfile'           => 'output',
    );

    # merge defaults, configuartion and passed args
    my %args = ( %defaults, %{$self->{'config'}}, %kwargs );

    my ($user, undef, undef, undef, undef, undef, undef, undef, undef) = getpwuid($<);

    my $collection = $self->{'user_index'}->get_collection("$args{'cleanup'}/*");
    return unless $collection =~ /^user_${user}/o;

    if ($collection) {
        $self->{'user_index'}->delete_hash($args{'cleanup'});
        system("curl -s $args{seaweed_addr}/col/delete?collection=${collection}");
    }
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
