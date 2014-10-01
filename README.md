# CMR
CMR is a perl framework built on top of nanomsg for distributing tasks across a clustered environment. Clients for performing parallel distributed grep, map, or map-reduce tasks have been created to show the capabilities of CMR.

# Dependencies
NanoMsg - http://nanomsg.org/
The Debian repositories current provide libnanomsg0 and libnanomsg-raw-perl, both required by the cmr-lib Debian package provided. These nanomsg packages are only available in sid but are in the process of being added to testing and backported to Debian Wheezy.
NanoMsg deb - https://packages.debian.org/libnanomsg0
Perl NanoMsg bindings deb - https://packages.debian.org/libnanomsg-raw-perl

Additionally, gzip and pigz are also required and available via apt as are the various perl packages required. Those being; libdata-guid-perl, libdate-calc-perl, libjson-xs-perl, libgetopt-long-descriptive-perl, libdate-manip-perl, libconfig-tiny-perl, liblog-log4perl-per
l, and libuuid-perl.

CMR requires a coherent view of a data warehouse from the perspective of all nodes. This mandates the use of a networked or clustered file system such as NFS or Gluster.

# Installation
Manual (installs everything):

```
perl Makefile.PL
make
make install
```

Package based (server components):

```
dpkg -i cmr-lib_0.0.1-1_all.deb cmr-server_0.0.1-1_all.deb
```

Package based (worker components):

```
dpkg -i cmr-lib_0.0.1-1_all.deb cmr-worker_0.0.1-1_all.deb cmr-utils_0.0.1-1_amd64.deb
```

Package based (client components):

```
dpkg -i cmr-lib_0.0.1-1_all.deb cmr-client_0.0.1-1_all.deb cmr-utils_0.0.1-1_amd64.deb
```

All components can be installed on the same system. The default configuration is near complete when all components are installed on the same system.

# Tested Installation
CMR has been developed on and has been tested with Debian Wheezy. All dependencies are available directly from Debian repositories.
Gluster was chosen as the clustered file system and is the only one verified to work well with CMR, although, NFS should work too.
Additionally, the network interconnecting all CMR nodes and all Gluster nodes during development of CMR was 40Gb/s Infiniband, known as QDR. As such, some utilities in use by CMR may be out of place on a different file system. Namely, the chunky c binary. It should not ca
use any issues however.

In order to realize the benefits we have seen, a similar environment is recommended.

# Setup & Configuration
See [Configuration](https://github.com/chitika/cmr/wiki/Configuration)

# Usage
See [Examples](https://github.com/chitika/cmr/wiki/Examples)


# Components
```
cmr-server      Provisions cmr-worker instances with cmr client requests
cmr-worker      Handles cmr client requests
cmr-caster      Broadcasts events produced by cmr-components
cmr             Map-Reduce client
cmr-grep        Grep client
```

# cmr-server usage
```bash
cmr-server [--config <config file>]
```
> ###### cmr-server default configuration file is `/etc/cmr/config.ini`.


# cmr-worker usage
```bash
cmr-worker [--config <config file>]
```
> ###### cmr-worker default configuration file is `/etc/cmr/config.ini`.


# cmr-caster usage
```bash
cmr-caster [--config <config file>]
```
> ###### cmr-caster default configuration file is `/etc/cmr/config.ini`.


# cmr usage
```bash
cmr --input "<glob_pattern>" --mapper <mapper> [--reducer <reducer>] [--config <config file>]
```
> ###### cmr default configuration file is `/etc/cmr/config.ini`.
> ###### Glob patterns must be quoted, failure to do so will cause them to be expanded by the shell and be misinterpreted by the client
> ###### Reducer implementation needs to be idempotent, non-idempotent reducers may however be used as a final-reducer

> ##### additional optional arguments
```
    -v --verbose        verbose mode
    -f --final-reducer  reducer to use for final reduce
    -c --cache          cache results [don't cleanup job output when writing to stdout]
    -o --output         output to this location rather than the default output path
    -b --bundle         bundle file with job (places it in scratch space along with job data making it accessible to worker nodes)
    -F --force          force run (overwrite output path)
    --stdout            output on standard out
```

> ##### experimental arguments
```
    -j --join-reducer   reducer to use for join [requires bucket and aggregate parameters to be specified]
    -B --bucket         split job into buckets to parallelize final reduce [requires aggregates]
    -a --aggregates     number of aggregates in mapped data
    -F --force          force run (overwrite output path)
    -S --sort           sort
```


# cmr-grep usage
```bash
cmr-grep --input "<glob_pattern>" --pattern "<grep_pattern>" [--config <config file>]
```
> ###### cmr-grep default configuration file is `/etc/cmr/config.ini`.
> ###### Glob patterns must be quoted, failure to do so will cause them to be expanded by the shell and be misinterpreted by the client


> ##### additional optional arguments
```
    -v --verbose        verbose output
    -o --output         output to this location rather than the default output path
    -f --flags          pass grep flags
    -c --cache          cache results [don't cleanup job output when writing to stdout]
    -F --force          force run (overwrite output path)
    --stdout            output on standard out
```

