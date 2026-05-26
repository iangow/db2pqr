# WRDS: Setting up SSH for SAS metadata

Most `db2pq` WRDS work uses WRDS PostgreSQL credentials. SSH access is a
separate setup used only when
[`wrds_update_pq()`](https://iangow.github.io/db2pqr/reference/wrds_update_pq.md)
needs metadata from the WRDS SAS side:

``` r

wrds_update_pq("dsi", "crsp", use_sas = TRUE)
```

With `use_sas = TRUE`, `db2pq` retrieves last-modified dates by running
`PROC CONTENTS` on the WRDS SAS server over SSH. This can be useful for
tables whose PostgreSQL comment does not carry a reliable date.

## Before you start

The `processx` package is required for the SAS metadata option:

``` r

install.packages("processx")
```

Configure PostgreSQL credentials separately for ordinary WRDS table
downloads. See the [authentication
article](https://iangow.github.io/db2pqr/articles/authentication.md) for
the `WRDS_ID`, `.pgpass`, and
[`wrds::wrds_set_credentials()`](https://rdrr.io/pkg/wrds/man/wrds_set_credentials.html)
paths supported by `db2pq`.

## Configure WRDS SSH

WRDS provides a dedicated SSH endpoint for key-based authentication:
`wrds-cloud-sshkey.wharton.upenn.edu`.

### Generate an SSH key

The ed25519 key type is a reasonable default:

``` sh
ssh-keygen -t ed25519 -C "your_wrds_id@wrds"
```

Accept the default location (`~/.ssh/id_ed25519`). You may use a
passphrase if your SSH agent is running. For unattended scripts, an
empty passphrase may be required.

### Copy the public key to WRDS

``` sh
cat ~/.ssh/id_ed25519.pub | \
  ssh your_wrds_id@wrds-cloud-sshkey.wharton.upenn.edu \
  "mkdir -p ~/.ssh && chmod 700 ~/.ssh && \
   cat >> ~/.ssh/authorized_keys && chmod 600 ~/.ssh/authorized_keys"
```

### Configure SSH

Add an entry to `~/.ssh/config`:

``` text
Host wrds
    HostName wrds-cloud-sshkey.wharton.upenn.edu
    User your_wrds_id
    IdentityFile ~/.ssh/id_ed25519
    IdentitiesOnly yes
```

Verify the connection:

``` sh
ssh wrds
```

## Troubleshooting

If SSH still prompts for a password, inspect the connection:

``` sh
ssh -vvv wrds
```

Confirm that `publickey` appears in the list of authentication methods.
Also check that `~/.ssh/config` uses the WRDS SSH endpoint and the key
you copied to WRDS.

## Related pages

- [Authentication](https://iangow.github.io/db2pqr/articles/authentication.md)
- [WRDS to
  Parquet](https://iangow.github.io/db2pqr/articles/wrds-to-parquet.md)
