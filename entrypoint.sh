#!/bin/sh

set -e

echo "Running migrations"
export DATABASE_URL="postgres://${APP__PG_USER}:${APP__PG_PASSWORD}@${APP__PG_HOST}/${APP__PG_DBNAME}"
sqlx database create
sqlx migrate run

echo "Starting the server"
exec /bin/server
