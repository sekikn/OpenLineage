#!/usr/bin/env bash

set -e

project_root=$(git rev-parse --show-toplevel)
cd "${project_root}"/integration/

rm -r ./python
cp -R ../client/python ./python
docker build -f airflow/Dockerfile.tests -t openlineage-airflow-base .

# maybe overkill
OPENLINEAGE_AIRFLOW_WHL=$(docker run openlineage-airflow-base:latest sh -c "ls /whl/openlineage*")

# Add revision to requirements.txt
cat > airflow/requirements.txt <<EOL
${OPENLINEAGE_AIRFLOW_WHL}
EOL

docker-compose -f airflow/scripts/docker-compose.yml down
docker-compose -f airflow/scripts/docker-compose.yml up --build --force-recreate --abort-on-container-exit airflow_init postgres
docker-compose -f airflow/scripts/docker-compose.yml up --build --scale airflow_init=0
