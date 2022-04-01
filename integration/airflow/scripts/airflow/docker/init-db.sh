#!/bin/bash
#
# SPDX-License-Identifier: Apache-2.0.
#
# Usage: $ ./init-db.sh

set -eu

# STEP 1: Add users, databases, etc
psql -v ON_ERROR_STOP=1 --username "${POSTGRES_USER}" > /dev/null <<-EOSQL
  CREATE USER ${AIRFLOW_USER};
  ALTER USER ${AIRFLOW_USER} WITH PASSWORD '${AIRFLOW_PASSWORD}';
  CREATE DATABASE ${AIRFLOW_DB};
  GRANT ALL PRIVILEGES ON DATABASE ${AIRFLOW_DB} TO ${AIRFLOW_USER};
EOSQL
