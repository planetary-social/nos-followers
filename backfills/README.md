# Backfills Directory

This directory contains **backfill scripts** for manual database changes. These scripts are idempotent and should be applied manually, not through automated migrations.

## Conventions

- **Date Prefix**: Start each script with the date in `YYYYMMDD` format to indicate when it was created.
- **Descriptive Name**: Follow the date with a brief, clear description of the script's purpose.

- **Comments**: Add a brief comment at the top of each script explaining its purpose.


## Additional Queries

We will include any interesting queries that don't modify the database too. This can evolve into a valuable collection for analysis and reporting.

