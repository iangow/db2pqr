# db2pq: Export Database Tables to Parquet

Tools for exporting PostgreSQL and WRDS data to Apache Parquet files,
managing a local Parquet data repository, and checking whether local
files are current against source table metadata.

## Configuration

`DATA_DIR` controls the default root directory for Parquet output. WRDS
helpers resolve usernames from explicit arguments, `WRDS_ID`,
`WRDS_USER`, and then the credential store used by the `wrds` package.

## See also

Useful links:

- <https://github.com/iangow/db2pqr>

- <https://iangow.github.io/db2pqr/>

- Report bugs at <https://github.com/iangow/db2pqr/issues>

## Author

**Maintainer**: Ian Gow <iandgow@gmail.com>

Authors:

- Ian Gow <iandgow@gmail.com>
