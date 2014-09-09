# CMR
CMR is a perl framework built on top of nanomsg for distributing tasks across a clustered environment in a scalable manner.
Clients for performing parallel destributed grep, map, or map-reduce tasks has been created to show the scalability of CMR.

# Dependencies
NanoMsg - http://nanomsg.org/

# Installation
```
perl Makefile.PL
make
make install
```

# Examples
See [Examples](https://github.com/chitika/cmr/wiki/Examples)

# Components
```
cmr-server      Provisions cmr-worker instances with cmr client requests
cmr-worker      Handles cmr client requests
cmr-caster      Broadcasts events produced by cmr-components
cmr             Map-Reduce client
cmr-grep        Grep client
```

# Configuration
Cmr is configured through a single ini configuration file.

```
[global]        configuration options affect all components of cmr and are overriden by configuration options specific to each component
[cmr-server]    configuration options specific to cmr-server
[cmr-worker]    configuration options specific to cmr-worker
[cmr]           configuration options specific to cmr client
[cmr-grep]      configuration options specific to cmr-grep client
```

```
server_in               cmr-server binds to this address, which is used to service cmr client requests
server_out              cmr-server binds to this address, which is used to communicate with worker instances
caster_in               cmr-caster binds to this address, which is used to receive requests to broadcast events
caster_out              cmr-caster binds to this address, which is used to broadcast events
basepath                path to the root of the clustered file system
scratch_path            scratch location (must be within the clustered file system)
output_path             default output location (must be within the clustered file system)
error_path              error log output location (must be within the clustered file system)
bundle_path             script bundle location (must be within the clustered file system)
client_queue_size       client queue's this many tasks locally
max_task_attempts       maximum allowed attempts on a single task before job failure
accept_timeout          time allowed for a worker to acknowledge aquiring a client task
task_timeout            base timeout allowed for a worker to complete a client task
retry_timeout           additional time allotted to a task for susequent attempts upon task failure
deadline_scale_factor   increase task timeout by this amount for each task submitted by a job (scale for large jobs)
max_threads             maximum number of threads to used for parallel crawl of clustered file system
tasks_per_thread        number of tasks that should be queued on a single thread
max_thread_backlog      absolute maximum number of tasks allowed to be queued on a single thread
batch_size              number of files to be included in a single task
log_level               minimum logging severity, one of TRACE, DEBUG, INFO, WARN, ERROR, FATAL
drought_backoff         extra sleep time for server/worker when no work is available to be completed
dispatch_interval       time between client/server/worker dispatch attempts
delete_zerobyte_output  worker removes files that contain no output after completing tasks
work_resend_interval    nanomsg resend interval on request channel
work_request_timeout    nanomsg recieve timeout on server requesting work from a client
smudge_time             when determining if a job is over utilizing the cluster it must exceed the time used by other jobs by this amount
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
    -F --force          force run (overwrite output path)
    --stdout            output on standard out
```

