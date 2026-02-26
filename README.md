# db2pq: export PostgreSQL and WRDS data to Parquet

`db2pq` is an R package for moving data from PostgreSQL into Apache Parquet files.
It is designed for both general PostgreSQL sources and the WRDS PostgreSQL service.

## What it does

- Export a single PostgreSQL table to Parquet.
- Export all tables in a PostgreSQL schema to Parquet.
- Update WRDS Parquet files only when the source table is newer.
- Read and manage `last_modified` metadata embedded in Parquet files.
- Archive and restore historical versions of Parquet files.

## Installation

```r
# install.packages("pak")
pak::pak("iandgow/db2pqr")
```

## WRDS credentials setup

Credentials are stored securely in your system keyring (Keychain on macOS,
Credential Manager on Windows, Secret Service on Linux). Set them up once with:

```r
wrds::wrds_set_credentials()
```

This stores your WRDS username and password, which are used by both
`wrds_connect()` and `wrds_update_pq()`.

## WRDS SSH setup (for `use_sas = TRUE`)

`wrds_update_pq(..., use_sas = TRUE)` retrieves last-modified dates by running
`PROC CONTENTS` on the WRDS SAS server via SSH. This can be useful for tables
whose PostgreSQL comment does not carry a reliable date.

The `processx` package is required for this option:

```r
install.packages("processx")
```

You also need to configure SSH key access to your WRDS account. WRDS provides
a dedicated SSH endpoint for key-based authentication:
`wrds-cloud-sshkey.wharton.upenn.edu`

#### Step 1: Generate an SSH key

We recommend the modern ed25519 key type:

```bash
ssh-keygen -t ed25519 -C "your_wrds_id@wrds"
```

Accept the default location (`~/.ssh/id_ed25519`). You may use a passphrase if
your SSH agent is running. For unattended scripts, an empty passphrase may be
required.

#### Step 2: Copy the public key to WRDS

```bash
cat ~/.ssh/id_ed25519.pub | \
  ssh your_wrds_id@wrds-cloud-sshkey.wharton.upenn.edu \
  "mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
   cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

#### Step 3: (Recommended) Configure SSH

Add an entry to `~/.ssh/config`:

```
Host wrds
    HostName wrds-cloud-sshkey.wharton.upenn.edu
    User your_wrds_id
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```

You can then verify the connection with:

```bash
ssh wrds
```

#### Troubleshooting

If SSH still prompts for a password, run:

```bash
ssh -vvv wrds
```

and confirm that `publickey` appears in the list of authentication methods.

## Quickstart

### Update a WRDS table

```r
library(db2pq)

wrds_update_pq("dsi", "crsp")
```

### Force a re-download

```r
wrds_update_pq("dsi", "crsp", force = TRUE)
```

### Use SAS metadata to check for updates

```r
wrds_update_pq("dsi", "crsp", use_sas = TRUE)
```

### Update all tables in a schema

```r
wrds_schema_to_pq("crsp")
```

### Check when local Parquet files were last updated

```r
pq_last_modified(schema = "crsp")
```

## Parquet layout

Files are organized as:

```
<DATA_DIR>/<schema>/<table>.parquet
```

For example:

```
~/pq_data/crsp/dsi.parquet
```

The `DATA_DIR` environment variable sets the root directory. It can also be
passed directly as `data_dir` to any function.

When `archive = TRUE`, replaced files are moved to:

```
<DATA_DIR>/<schema>/archive/<table>_<timestamp>.parquet
```

## License

MIT License. See `LICENSE.md`.
