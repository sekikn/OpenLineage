# SPDX-License-Identifier: Apache-2.0.

import os
from unittest import mock

import pytest
from airflow.models import Connection
from airflow.utils.dates import days_ago
from airflow import DAG

from openlineage.airflow.utils import safe_import_airflow, get_connection
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import DbTableMeta
from openlineage.common.dataset import Source, Dataset
from openlineage.airflow.extractors.jdbc_extractor import JdbcExtractor

JdbcOperator = safe_import_airflow(
    airflow_1_path="airflow.operators.jdbc_operator.JdbcOperator",
    airflow_2_path="airflow.providers.jdbc.operators.jdbc.JdbcOperator"
)

JdbcHook = safe_import_airflow(
    airflow_1_path="airflow.hooks.jdbc_hook.JdbcHook",
    airflow_2_path="airflow.providers.jdbc.hooks.jdbc.JdbcHook"
)

CONN_ID = 'jdbc_conn'
DB_NAME = 'food_delivery'

CONN_URI = f'jdbc:postgres://user:pass@localhost:5432/{DB_NAME}'
CONN_URI_WITHOUT_USERPASS = f'jdbc:postgres://localhost:5432/{DB_NAME}'

DB_SCHEMA_NAME = 'public'
DB_TABLE_NAME = DbTableMeta('discounts')
DB_TABLE_COLUMNS = [
    DbColumn(
        name='id',
        type='int4',
        ordinal_position=1
    ),
    DbColumn(
        name='amount_off',
        type='int4',
        ordinal_position=2
    ),
    DbColumn(
        name='customer_email',
        type='varchar',
        ordinal_position=3
    ),
    DbColumn(
        name='starts_on',
        type='timestamp',
        ordinal_position=4
    ),
    DbColumn(
        name='ends_on',
        type='timestamp',
        ordinal_position=5
    )
]
DB_TABLE_SCHEMA = DbTableSchema(
    schema_name=DB_SCHEMA_NAME,
    table_name=DB_TABLE_NAME,
    columns=DB_TABLE_COLUMNS
)
NO_DB_TABLE_SCHEMA = []

SQL = f"SELECT * FROM {DB_TABLE_NAME.name};"

DAG_ID = 'email_discounts'
DAG_OWNER = 'datascience'
DAG_DEFAULT_ARGS = {
    'owner': DAG_OWNER,
    'depends_on_past': False,
    'start_date': days_ago(7),
    'email_on_failure': False,
    'email_on_retry': False,
    'email': ['datascience@example.com']
}
DAG_DESCRIPTION = \
    'Email discounts to customers that have experienced order delays daily'

DAG = dag = DAG(
    DAG_ID,
    schedule_interval='@weekly',
    default_args=DAG_DEFAULT_ARGS,
    description=DAG_DESCRIPTION
)

TASK_ID = 'select'
TASK = JdbcOperator(
    task_id=TASK_ID,
    jdbc_conn_id=CONN_ID,
    sql=SQL,
    dag=DAG,
)


@mock.patch('openlineage.airflow.extractors.jdbc_extractor.JdbcExtractor._get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.jdbc_extractor.get_connection')
def test_extract(get_connection, mock_get_table_schemas):
    mock_get_table_schemas.side_effect = \
        [[DB_TABLE_SCHEMA], NO_DB_TABLE_SCHEMA]

    conn = Connection(
        conn_id=CONN_ID,
        conn_type='jdbc',
        host='jdbc:postgres://localhost:5432/food_delivery',
    )

    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=Source(
                scheme='jdbc:postgres',
                authority='localhost:5432',
                connection_url=CONN_URI_WITHOUT_USERPASS
            ),
            fields=[]
        ).to_openlineage_dataset()]

    # Set the environment variable for the connection
    os.environ[f"AIRFLOW_CONN_{CONN_ID.upper()}"] = CONN_URI

    task_metadata = JdbcExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@mock.patch('openlineage.airflow.extractors.jdbc_extractor.JdbcExtractor._get_table_schemas')  # noqa
@mock.patch('openlineage.airflow.extractors.jdbc_extractor.get_connection')
def test_extract_authority_uri(get_connection, mock_get_table_schemas):

    mock_get_table_schemas.side_effect = \
        [[DB_TABLE_SCHEMA], NO_DB_TABLE_SCHEMA]

    conn = Connection()
    conn.parse_from_uri(uri=CONN_URI)
    get_connection.return_value = conn

    expected_inputs = [
        Dataset(
            name=f"{DB_NAME}.{DB_SCHEMA_NAME}.{DB_TABLE_NAME.name}",
            source=Source(
                scheme='jdbc:postgres',
                authority='localhost:5432',
                connection_url=CONN_URI_WITHOUT_USERPASS
            ),
            fields=[]
        ).to_openlineage_dataset()]

    task_metadata = JdbcExtractor(TASK).extract()

    assert task_metadata.name == f"{DAG_ID}.{TASK_ID}"
    assert task_metadata.inputs == expected_inputs
    assert task_metadata.outputs == []


@mock.patch('jaydebeapi.connect')
def test_get_table_schemas(mock_conn):
    # (1) Define a simple hook class for testing
    class TestJdbcHook(JdbcHook):
        conn_name_attr = 'test_conn_id'

        def __init__(self, *args, **kwargs):
            super(TestJdbcHook, self).__init__(*args, **kwargs)
            self.schema = ''

    # (2) Mock calls to jdbc
    rows = [
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'id', 1, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'amount_off', 2, 'int4'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'customer_email', 3, 'varchar'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'starts_on', 4, 'timestamp'),
        (DB_SCHEMA_NAME, DB_TABLE_NAME.name, 'ends_on', 5, 'timestamp')
    ]

    mock_conn.return_value \
        .cursor.return_value \
        .fetchall.return_value = rows

    # (3) Mock conn for hook
    hook = TestJdbcHook()
    hook.conn = mock_conn

    # (4) Extract table schemas for task
    extractor = JdbcExtractor(TASK)
    table_schemas = extractor._get_table_schemas(table_names=[DB_TABLE_NAME])

    assert table_schemas == [DB_TABLE_SCHEMA]


def test_get_connection_import_returns_none_if_not_exists():
    assert get_connection("does_not_exist") is None
    assert get_connection("does_exist") is None


@pytest.fixture
def create_connection():
    create_session = safe_import_airflow(
        airflow_1_path="airflow.utils.db.create_session",
        airflow_2_path="airflow.utils.session.create_session",
    )

    conn = Connection("does_exist", conn_type="jdbc")
    with create_session() as session:
        session.add(conn)
        session.commit()

    yield conn

    with create_session() as session:
        session.delete(conn)
        session.commit()


def test_get_connection_returns_one_if_exists(create_connection):
    conn = Connection("does_exist")
    assert get_connection("does_exist").conn_id == conn.conn_id
