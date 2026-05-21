# ADBC PostgreSQL SSL support options for db2pq

If users install `adbcpostgresql` from CRAN binaries and those binaries
were built without SSL-enabled `libpq`, then SSL/TLS cannot be turned on
from `db2pq` code alone. Your package can only pass connection options
(for example `sslmode=require`), but cannot retrofit SSL support into a
driver binary that lacks it.

## Practical options

1.  **Fail fast with a clear diagnostic**
    - Try connecting with `sslmode=require` (or your policy equivalent).
    - If the connection fails with an SSL-related `libpq`/driver error,
      return a targeted message explaining that the installed
      `adbcpostgresql` build lacks SSL support.
2.  **Support a fallback backend in your package**
    - Allow users to choose `RPostgres` as an alternate backend when SSL
      is required.
    - Keep your SQL/data-layer code backend-agnostic where possible.
3.  **Document a supported install path for SSL-capable ADBC**
    - Recommend an installation source/build of `adbcpostgresql` that
      links against SSL-capable `libpq`.
    - If your users are in managed environments, provide a short setup
      guide per OS.
4.  **Expose SSL controls explicitly in your API**
    - Parameters such as `sslmode`, `sslrootcert`, `sslcert`, and
      `sslkey` should be pass-through settings so users can configure
      TLS policy.

## Recommendation

For `db2pq`, the most reliable approach is: - require SSL in connection
settings, - detect and explain missing SSL capability early, and -
offer/advertise `RPostgres` fallback for users who cannot rebuild
`adbcpostgresql`.

This keeps CRAN-binary users unblocked without forcing a source build.
