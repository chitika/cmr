# POLYMER [experimental]
polymer is cmr client that operates on columnar data stored in the warehouse. By carving the data this way, document parsing time can be mostly avoided, and queries can avoid retreiving data is not relevant. It appears to work, but has not been tested extensively.

# Storing data for use by polymer
Normally cmr expects the leaf nodes of the warehouse to point at files that contain complete documents. Polymer instead expects the leaf nodes of the warehouse to point at files that comprise a single column within a larger document. The process of splitting their document into columnar in order to load the warehouse is as of now not standardized and as such left as an exercise to the user.


# Cmr style warehouse layout
```
category/2015-01-01/01/important_document_1
category/2015-01-01/01/important_document_2
category/2015-01-01/02/important_document_1
```

# Polymer style warehouse layout
```
category/2015-01-01/01/important_document_1/column_a
category/2015-01-01/01/important_document_1/column_b
category/2015-01-01/01/important_document_1/column_c
category/2015-01-01/01/important_document_2/column_a
category/2015-01-01/01/important_document_2/column_b
category/2015-01-01/02/important_document_1/column_a
category/2015-01-01/02/important_document_1/column_b
category/2015-01-01/02/important_document_1/column_c
```

# Usage
```
polymer --input "<glob_pattern>" --field <field> [--field <field> ...] [--mapper <mapper>] [--reducer <reducer>] [--config <config file>]
```

> ##### additional optional arguments
```
    -v --verbose            verbose mode
    -I --include            include on key value pair   - key:value
    -E --exclude            exclude on key value pair   - key:value
    -R --include-range      include on key within range - key:rangelow:rangehigh
    -X --exclude-range      exclude on key within range - key:rangelow:rangehigh
```

