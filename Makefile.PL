#!perl

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

use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
    'NAME'          => 'Cmr',
    'LICENSE'       => 'GPLv3',
    'AUTHOR'        => 'Paul Laidler <plaidler@chitika.com>',
    'VERSION_FROM'  => 'lib/Cmr/Client.pm',
    'EXE_FILES'     => [
        'script/cget',
        'script/cgrep',
        'script/cmr',
        'script/cmr-caster',
        'script/cmr-comp-watcher',
        'script/cmr-grep',
        'script/cmr-server',
        'script/cmr-status',
        'script/cmr-worker',
        'script/cmr-map-json',
        'script/cmr-reduce',
        'script/cmr-query-watcher',
        'script/cmr-watcher',
        'script/getfield',
        'script/polymer',
        'script/polymer_paste',
        'script/seaweed_get',
        'script/seaweed_set',
        'script/seaweed_merge',
        'script/lw',
        'script/lcat',
    ],
    'PREREQ_PM' => {
        'NanoMsg::Raw'      => 0,
        'JSON::XS'          => 0,
        'Date::Calc'        => 0,
        'Date::Manip'       => 0,
        'IO::Select'        => 0,
        'POSIX'             => 0,
        'List::Util'        => 0,
        'File::Basename'    => 0,
        'Cwd'               => 0,
        'Data::GUID'        => 0,
        'Getopt::Long'      => 0,
    },
    'dist' => { 
        COMPRESS => "gzip-9",
        SUFFIX => "gz"
    },
);

package MY;
sub MY::postamble {

    return q{
all ::
	$(MAKE) -f src/cmr-binaries.mk INST_BIN="$(INST_BIN)"

install:: config_install
pure_install:: config_install
install_vendor:: config_install

config_install:
	install -D config/config.ini $(DESTDIR)/etc/cmr/config.ini
};
}

