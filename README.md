# CMR
CMR is a perl framework built on top of nanomsg for distributing tasks across a clustered environment. Clients for performing parallel distributed grep, map, or map-reduce tasks have been created to show the capabilities of CMR.

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

