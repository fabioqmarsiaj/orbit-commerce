#!/bin/bash
# Creates one database per microservice on first container startup.
# The official postgres image automatically runs *.sh files placed in
# /docker-entrypoint-initdb.d/ the first time the data directory is empty.
set -e

DATABASES=(order_db payment_db inventory_db shipping_db query_db)

for db in "${DATABASES[@]}"; do
  echo "Creating database: ${db}"
  psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" <<-EOSQL
    SELECT 'CREATE DATABASE ${db}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${db}')\gexec
EOSQL
done

echo "All orbit-commerce databases created."
