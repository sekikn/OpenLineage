# SPDX-License-Identifier: Apache-2.0

from airflow.operators.jdbc_operator import JdbcOperator
from airflow.utils.dates import days_ago

from openlineage.client import set_producer
set_producer("https://github.com/OpenLineage/OpenLineage/tree/0.0.1/integration/airflow")

from airflow.version import version as AIRFLOW_VERSION
from pkg_resources import parse_version
if parse_version(AIRFLOW_VERSION) < parse_version("2.0.0"):
    from openlineage.airflow import DAG
else:
    from airflow import DAG


default_args = {
    'owner': 'datascience',
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}

dag = DAG(
    'jdbc_orders_popular_day_of_week',
    schedule_interval='@once',
    default_args=default_args,
    description='Determines the popular day of week orders are placed.'
)


t1 = JdbcOperator(
    task_id='jdbc_if_not_exists',
    jdbc_conn_id='jdbc_conn',
    sql='''
    CREATE TABLE IF NOT EXISTS popular_orders_day_of_week (
      order_day_of_week VARCHAR(64) NOT NULL,
      order_placed_on   TIMESTAMP NOT NULL,
      orders_placed     INTEGER NOT NULL
    );''',
    dag=dag
)

t2 = JdbcOperator(
    task_id='jdbc_insert',
    jdbc_conn_id='jdbc_conn',
    sql='''
    INSERT INTO popular_orders_day_of_week (order_day_of_week, order_placed_on,orders_placed)
    SELECT EXTRACT(ISODOW FROM order_placed_on) AS order_day_of_week,
           order_placed_on,
           COUNT(*) AS orders_placed
      FROM top_delivery_times
     GROUP BY order_placed_on;
    ''',
    dag=dag
)

t1 >> t2
