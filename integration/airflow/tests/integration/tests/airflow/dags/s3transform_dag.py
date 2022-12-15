# Copyright 2018-2022 contributors to the OpenLineage project
# SPDX-License-Identifier: Apache-2.0
import os

from airflow import DAG
from airflow.providers.amazon.aws.operators.s3 import S3FileTransformOperator
from airflow.utils.dates import days_ago


with DAG(dag_id="s3transform_dag", start_date=days_ago(7), schedule_interval="@once") as dag:
    S3FileTransformOperator(
        task_id="s3transform_task",
        aws_conn_id="aws_conn",
        source_s3_key=os.environ["AWS_S3_TRANSFORM_INPUT_LOCATION"],
        dest_s3_key=os.environ["AWS_S3_TRANSFORM_OUTPUT_LOCATION"],
        select_expression="SELECT UPPER(_1) FROM S3Object[*]",
    )
