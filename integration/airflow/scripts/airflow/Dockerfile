ARG AIRFLOW_IMAGE=apache/airflow:2.2.4-python3.7
FROM openlineage-airflow-base:latest AS build

FROM $AIRFLOW_IMAGE AS airflow
COPY --from=build /app/wheel /whl
COPY requirements.txt requirements.txt
RUN pip install --user -r requirements.txt
