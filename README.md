# CMR - SeaweedFS edition
CMR is a perl framework built on top of nanomsg for distributing tasks across a clustered environment. Clients for performing parallel distributed grep, map, or map-reduce tasks have been created to show the capabilities of CMR.

# Warehouse requirements
```
SeaweedFS - used as warehouse document store
Redis     - used as warehouse index
```

# Storing files in the warehouse
lib/Cmr/Seaweed.pm provides APIs for accessing the warehouse, the easiest and most reliable way of storing files
in the warehouse is by using its set_from_file method which takes a file from local disk and stores it at the requested 
warehouse key.

```
use lib 'lib/Cmr';
use Seaweed ();
$fs = &Seaweed::new(redis_addr => $redis_addr, seaweed_addr => $seaweed_addr);
$fs->set_from_file($warehouse_key, $file_name);
```

# Using stored files in Cmr Jobs
The --input flag is now used to specify a pattern that is matched against the keys in the warehouse index. If a file is inserted with a key "2015-01-01/important_data_01" it can be used in cmr by specifying the input pattern --input "2015-01-01/important_data_01". Likewise if many files in the form "2015-01-01/important_data_xx" all of them can be used as input by specifying a glob --input "2015-01-01/important_data_*".

# Listing warehouse contents
The scripts lw and lcat can be used to list and cat the contents of the warehouse, they accept the same glob patterns as cmr. bashrc/warehouse-utils.bashrc provides the hooks necessary for lw an lcat to have autocomplete functionality in bash, which is extremely helpful when exploring the warehouse.


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

