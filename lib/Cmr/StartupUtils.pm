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

package Cmr::StartupUtils;

## Provides subroutines to simplify script initialization

our $VERSION = '0.2';

use strict;
use warnings;

use Getopt::Long::Descriptive ();
use Config::Tiny ();
use Log::Log4perl ();
use Log::Log4perl::Level ();
use Cwd  ('abs_path');
use Fcntl (':flock');

use File::Basename qw(dirname);
use Cwd qw(abs_path);
use lib dirname (abs_path(__FILE__))."/..";


use Cmr::StartupUtils::StartupLogTrap ();

our $last_config_check;
our $locked_files = {};
our $current_config = {};
our $current_logger;
our $finished = 0;


sub _REQUIRE_INIT {
    unless ( $current_config->{'STARTUP::META'} ) {
        my ( $package, $filename, $line, $subroutine, $hasargs, $wantarray, $evaltext, $is_require, $hits, $bitmask, $hithash ) = caller(1);
        die "${subroutine} called without calling StartupUtils::script_init first";
    }
}


sub get_logger {
    _REQUIRE_INIT;
    # If the logger is cached, return a cached copy to avoid the overhead of Log4perl::get_logger
    if ($current_logger) {
        return $current_logger;
    }
    my $script_name = $current_config->{'STARTUP::META'}->{'script_name'} or die "Failed to determine script name!?";
    my $logger = Log::Log4perl::get_logger( $script_name ) or die "Failed to get logger";
    $current_logger = $logger;
    return $logger;
}


sub warn_caller {
    _REQUIRE_INIT;
    my ($message) = @_;
    my $logger = get_logger();
    $Log::Log4perl::caller_depth+=2;
    $logger->warn($message);
    $Log::Log4perl::caller_depth-=2;
}


sub error_caller {
    _REQUIRE_INIT;
    my ($message) = @_;
    my $logger = get_logger();
    $Log::Log4perl::caller_depth+=2;
    $logger->error($message);
    $Log::Log4perl::caller_depth-=2;
}


sub load_config {
    my ($config_ref) = @_;
    $current_config //= $$config_ref;
    _REQUIRE_INIT;
    my $config = $$config_ref;

    # Don't attempt to load the configuration too freqently (we have to stat it each time we check...)
    my $now = [Time::HiRes::gettimeofday];
    if ( $config->{'STARTUP::META'}->{'config_loaded'} and
         Time::HiRes::tv_interval ($last_config_check, $now) < $config->{'STARTUP::META'}->{'config_check_interval'} )
    {
        return 0;
    }

    $last_config_check = $now;

    my $config_updated = 0;

    my $default_config  = {};
    my $provided_config = {};

    my $script_name   = $config->{'STARTUP::META'}->{'script_name'} or die "Failed to determine script name!?";
    my $config_loaded = $config->{'STARTUP::META'}->{'config_loaded'} || 0;

    unless ( $config_loaded ) {
        # If the configuration hasn't been loaded yet:
        #   Load any config specified by the script (and store its last modified time)
        #   Load any config specified by command line args (and store its last modified time)
        if (       $config->{'STARTUP::META'}->{'arg_config'}
            and -e $config->{'STARTUP::META'}->{'arg_config'})
        {
            my @file_stats = stat($config->{'STARTUP::META'}->{'arg_config'});
            $config->{'STARTUP::META'}->{'arg_config_last_modified'} = $file_stats[9] || -1;
        }

        if (       $config->{'STARTUP::META'}->{'opt_config'}
            and -e $config->{'STARTUP::META'}->{'opt_config'} )
        {
            my @file_stats = stat($config->{'STARTUP::META'}->{'opt_config'});
            $config->{'STARTUP::META'}->{'opt_config_last_modified'} = $file_stats[9] || -1;
        }

        $default_config  = Config::Tiny->read($config->{'STARTUP::META'}->{'arg_config'}) || {};
        $provided_config = Config::Tiny->read($config->{'STARTUP::META'}->{'opt_config'}) || {};

        $config->{'STARTUP::META'}->{'config_loaded'} = 1;
        $config_updated = 1;

    }
    elsif ( $config_loaded ) {
        # If the configuration has been loaded:
        #   Check the modification time of each configuration ( and reload it if it has been modified )
        if ($config->{'STARTUP::META'}->{'arg_config'} && -e $config->{'STARTUP::META'}->{'arg_config'}) {
            my @file_stats = stat($config->{'STARTUP::META'}->{'arg_config'});
            my $mtime =  $file_stats[9] || -1;
            if ($config->{'STARTUP::META'}->{'arg_config_last_modified'} != $mtime) {
                $default_config  = Config::Tiny->read($config->{'STARTUP::META'}->{'arg_config'}) || {};
                $config->{'STARTUP::META'}->{'arg_config_last_modified'} = $mtime;
                $config_updated = 1;
            }
        }

        if ($config->{'STARTUP::META'}->{'opt_config'} && -e $config->{'STARTUP::META'}->{'opt_config'}) {
            my @file_stats = stat($config->{'STARTUP::META'}->{'opt_config'});
            my $mtime =  $file_stats[9] || -1;
            if ($config->{'STARTUP::META'}->{'opt_config_last_modified'} != $mtime) {
                $provided_config  = Config::Tiny->read($config->{'STARTUP::META'}->{'opt_config'}) || {};
                $config->{'STARTUP::META'}->{'opt_config_last_modified'} = $mtime;
                $config_updated = 1;
            }
        }
    }

    if ($config_updated) {
        # If the configuration has been modified
        my $default_script_global      = $default_config->{'global'} || {};
        my $provided_script_global     = $provided_config->{'global'} || {};
        my $default_script_options      = $default_config->{$script_name}  || {};
        my $provided_script_options     = $provided_config->{$script_name} || {};
        my $opts                        = $config->{'STARTUP::META'}->{'opts'} || {};

        unless ( $config->{'STARTUP::META'}->{'cmdline'} ) {
            # First time around, build up a command line...
            # note ~ this isn't identical to the one provided with the script, its merely a reconstruction.
            #   The resulting command line is not guarenteed to be usable, theres a couple of issues
            #   that are caused by nested quotes which need some attention
            my $merged_config = {
                %$config,
                %$default_script_global,
                %$default_script_options,
                %$provided_script_global,
                %$provided_script_options,
                %$default_config,
                %$provided_config,
            };

            my $cmdline = "$0 ";
            for my $key (keys %{$opts}) {
                if ( not exists($merged_config->{'script_defaults'}->{$key})
                     or $merged_config->{'script_defaults'}->{$key} ne $opts->{$key})
                {
                    if ( not ref($opts->{$key}) ) {
                        $cmdline .= "--$key \"$opts->{$key}\" ";
                    } elsif ( ref($opts->{$key}) eq "ARRAY" ) {
                        for my $idx (0..$#{$opts->{$key}}) {
                            $cmdline .= "--$key \"$opts->{$key}->[$idx]\" ";
                        }
                    }
                }
            }
            $config->{'STARTUP::META'}->{'cmdline'} = $cmdline;
        }

        # Merge all the different parts of the configuration...
        my $merged_config = {
            %$config,
            %$default_script_global,
            %$default_script_options,
            %$provided_script_global,
            %$provided_script_options,
            %$default_config,
            %$provided_config,
            %$opts
        };

        # deal with GETOPT_DASH_SUPPORT ( getopt long descriptive and dash characters don't get along )
        for my $key (keys %{$config->{'STARTUP::META'}->{'GETOPT_DASH_SUPPORT'}}) {
            $merged_config->{$config->{'STARTUP::META'}->{'GETOPT_DASH_SUPPORT'}->{$key}} = $merged_config->{$key};
        }

        $$config_ref = $merged_config;

        # Apply defaults now that we've merged everything
        for my $key (keys %{$merged_config->{'script_defaults'}}) {
            $merged_config->{$key} ||= $merged_config->{'script_defaults'}->{$key};
        }

        # Set the current configuartion (package level)
        $current_config = $merged_config;
        my $logger = get_logger();

        if ( $merged_config->{'log_level'} ) {
            # If a logging level is configured, set it
            $merged_config->{'log_level'} = uc($merged_config->{'log_level'});
            unless ( $merged_config->{'log_level'} ~~ ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'] ) {
                $logger->level("WARN");
                $logger->warn ("unknown log level $merged_config->{'log_level'}, defaulted to WARN");
                $merged_config->{'log_level'} = "WARN";
            } else {
                $logger->level($merged_config->{'log_level'});
            }
        }

        # Set finished flag if script is no longer enabled
        if ( defined( $merged_config->{'enabled'} )
             and $merged_config->{'enabled'} != 1 ) {
            $finished = 1;
            $logger->info("Finished! Script is disabled by configuration");
        }

        # Set finished flag if too many instances of the script are running
        if ($merged_config->{'STARTUP::META'}->{'no_lock'} == 0) {
            if ( !defined($merged_config->{'max_instances'}) ) {
                $merged_config->{'max_instances'} = 1;
            }
            if ( defined($merged_config->{'STARTUP::META'}->{'instance_id'}) ) {
                if ( $merged_config->{'STARTUP::META'}->{'instance_id'} > $merged_config->{'max_instances'} ) {
                    $finished = 1;
                    $logger->info("Finished! Script instance id ($merged_config->{'STARTUP::META'}->{'instance_id'}) is greater than the configured maximum");
                }
            }
        }
    }

    return $config_updated;
}


sub script_init {
    my ($args) = @_;
    $args ||= {};
    $args->{'description'} ||= "This script has no description, lets just say it performs magic!";
    $args->{'opts'} ||= [];
    $args->{'allow_leftover_args'} ||= 0;

    my @opts = ();
    my $config = {};

    $config->{'STARTUP::META'}->{'config_check_interval'} = $args->{'config_check_interval'} // 30;

    my $script_path = abs_path($0);

    $script_path =~ /(.*)\/([^\/]*?)(\.p[lm])?$/o;

    my $script_dir  = $1;
    my $script_name = $2;
    my $script_ext  = $3;

    $config->{'STARTUP::META'}->{'script_dir'}  = $script_dir;
    $config->{'STARTUP::META'}->{'script_name'} = $script_name;
    $config->{'STARTUP::META'}->{'script_ext'}  = $script_ext;

    # Default values for default options
    $config->{'script_defaults'} = {
        'log_level' => 'info',
        'debug'            => 0,
        'help'             => 0,
    };

    $config->{'script_defaults'}->{'log'}  = $args->{'log'}  // 'stderr';
    $config->{'script_defaults'}->{'lock'} = $args->{'lock'} // '';

    my $no_lock = 0;
    if ( defined($args->{'no_lock'}) ) {
        $no_lock = $args->{'no_lock'};
    }

    $config->{'STARTUP::META'}->{'no_lock'} = $no_lock;

    # Pick up local configuration automagically
    if ( not $args->{'config'}
         and -e "${script_dir}/config.ini")
    {
        $args->{'config'} = "${script_dir}/config.ini";
    }

    push @opts, ['config=s', 'configuration file', { 'default' => undef }];

    # Pull the defaults out of script specified opts
    $config->{'STARTUP::META'}->{'GETOPT_DASH_SUPPORT'} = {};
    for my $opt (@{$args->{'opts'}}) {
        my $option = $opt->[0];


        $option =~ /([^\|\=]+)\|?([^\=]+)?\=?(.*)$/o || die "Failed to parse opt: $opt->[0]";
        my $real_opt;

        if ($1 && $2) {
            $real_opt = length($1) > length($2) ? $1 : $2;
        } else {
            $real_opt = $1;
        }

        my $original_opt = $real_opt;
        if ( $real_opt =~ s/-/_/o ) {
            $config->{'STARTUP::META'}->{'GETOPT_DASH_SUPPORT'}->{$real_opt} = $original_opt;
        }

        if (scalar(@{$opt}) == 3) {
            $config->{'script_defaults'}->{$original_opt} = pop(@$opt)->{'default'};
        }
    }


    # Push default options, always present
    push @opts, [];
    push @opts, ['lock=s',              'script lock location'] unless ($config->{'STARTUP::META'}->{'no_lock'});
    push @opts, ['log-level=s',         'log level [TRACE, DEBUG, INFO, WARN, ERROR, FATAL]'];
    push @opts, ['log=s',               'script logging location'];
    push @opts, ['stdout',              'suggests script output on stdout'];
    push @opts, ['help|h',              'Print usage information'];

    # Merge provided options with default options
    my @merged_opts = (@{$args->{'opts'}}, @opts);

    # Parse opts
    my ($opt, $usage) = Getopt::Long::Descriptive::describe_options(
        "%c %o - $args->{'description'}",
        @merged_opts
    );

    # Short circuit if help was specified
    if ($opt->{'help'}) {
        print "${usage}\n";
        exit(0);
    }

    # Die if not all arguments were parsed
    if ( @ARGV and not $args->{'allow_leftover_args'} ) {
        die "FATAL: Leftover arguments after finished parsing: ". join(',', @ARGV). "\n\n${usage}\n";
    }

    $config->{'STARTUP::META'}->{'arg_config'} = $args->{'config'};
    $config->{'STARTUP::META'}->{'opt_config'} = $opt->{'config'};
    $config->{'STARTUP::META'}->{'opts'} = $opt;

    # Load and merge configuration
    $current_config = $config;
    &load_config(\$config);

    # Set log level and get a logger
    $config->{'log_level'} = uc($config->{'log_level'});
    $config->{'log_level'} ~~ ['TRACE', 'DEBUG', 'INFO', 'WARN', 'ERROR', 'FATAL'] || die "ERROR: Unknown log level - $config->{'log_level'}\n";

    if ( $config->{'log'} eq 'stdout' ||  $config->{'log'} eq 'stderr') {
        $config->{'log'} = uc($config->{'log'});
    }

    binmode STDIN,  ':utf8';
    binmode STDOUT, ':utf8';
    binmode STDERR, ':utf8';

    # Acquire lock file
    if ( $config->{'STARTUP::META'}->{'no_lock'} == 0 and $config->{'script_defaults'}->{'lock'} ) {
        if ( $current_config->{'max_instances'} > 1 ) {
            if (! ($config->{'log'} =~ /%d/o) ) {
                die "script max instances is configured to >1 and log file pattern missing %d for instance identifier: $config->{'log'}\n";
            }
            if (! ($config->{'lock'} =~ /%d/o) ) {
                die "script max instances is configured to >1 and lock file pattern missing %d for instance identifier: $config->{'lock'}\n";
            }
        }

        my $fd = 0;
        my $id = 1;
        for ($id = 1; $id <= $current_config->{'max_instances'}; $id++) {
            my $lock = $config->{'lock'};
            $lock =~ s/%d/$id/;
            $fd = acquire_lock($lock);
            if ($fd) {
                $current_config->{'STARTUP::META'}->{'instance_id'} = $id;
                last;
            }
        }

        if (!$fd) {
            die "Failed to acquire lock file.\n";
        }

        $config->{'log'} =~ s/%d/$id/;
    }

    # This should probably come from a configuration.
    my $log_conf;
    if ($config->{'debug'} && ! ($config->{'log'} eq 'STDOUT' || $config->{'log'} eq 'STDERR') ) {
        $log_conf = qq (
            log4perl.rootLogger=$config->{'log_level'}, LOGFILE, SCREEN

            log4perl.appender.LOGFILE=Log::Log4perl::Appender::File
            log4perl.appender.LOGFILE.filename=$config->{'log'}
            log4perl.appender.LOGFILE.mode=append
            log4perl.appender.LOGFILE.syswrite=1
            log4perl.appender.LOGFILE.layout=PatternLayout
            log4perl.appender.LOGFILE.layout.ConversionPattern=%d %l - %p: %m{chomp}\\n

            log4perl.appender.SCREEN=Log::Log4perl::Appender::Screen
            log4perl.appender.SCREEN.syswrite=1
            log4perl.appender.SCREEN.stderr=1
            log4perl.appender.SCREEN.layout=PatternLayout
            log4perl.appender.SCREEN.layout.ConversionPattern=%d %l - %p: %m{chomp}\\n
        );
    }
    elsif ( ! ($config->{'log'} eq 'STDOUT' || $config->{'log'} eq 'STDERR')  ) {
        $log_conf = qq (
            log4perl.rootLogger=$config->{'log_level'}, LOGFILE

            log4perl.appender.LOGFILE=Log::Log4perl::Appender::File
            log4perl.appender.LOGFILE.filename=$config->{'log'}
            log4perl.appender.LOGFILE.mode=append
            log4perl.appender.LOGFILE.syswrite=1
            log4perl.appender.LOGFILE.layout=PatternLayout
            log4perl.appender.LOGFILE.layout.ConversionPattern=%d %l - %p: %m{chomp}\\n
        );
        tie *STDERR, "Cmr::StartupUtils::StartupLogTrap"; # if we're supposed to be logging to a file, print stderr through logger
    } else {
        my $log_stderr = ( $config->{'log'} eq 'STDERR' ) ? 1 : 0;
        $log_conf = qq (
            log4perl.rootLogger=$config->{'log_level'}, SCREEN

            log4perl.appender.SCREEN=Log::Log4perl::Appender::Screen
            log4perl.appender.SCREEN.syswrite=1
            log4perl.appender.SCREEN.stderr=${log_stderr}
            log4perl.appender.SCREEN.layout=PatternLayout
            log4perl.appender.SCREEN.layout.ConversionPattern=%d %l - %p: %m{chomp}\\n
        );
    }

    Log::Log4perl->init(\$log_conf);

    my $logger = Log::Log4perl::get_logger($script_name) || die "Failed to get logger";

    # Log if debug mode is enabled
    if ($config->{'debug'}) { $logger->info("DEBUG LOGGING IS ON!"); }

    # Return logger and merged configuration
    return ($logger, $config);
}

sub finish {
    $finished = 1;
}

sub finished {
    return $finished;
}


sub acquire_lock($) {
    # Note: this routine opens files for write, it will clobber a file if it already exists... you've been warned
    _REQUIRE_INIT;
    my ($file) = @_;
    open(my $fd, '>'.$file)      || return 0;
    flock($fd, LOCK_EX | LOCK_NB) || return 0;
    $locked_files->{$file} = $fd;
    return $fd;
}

sub release_lock($) {
    _REQUIRE_INIT;
    my ($file) = @_;
    if ($locked_files->{$file}) {
        close($locked_files->{$file});
        delete $locked_files->{$file};
    }
}

1;
