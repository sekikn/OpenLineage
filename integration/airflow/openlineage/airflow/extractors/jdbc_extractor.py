# SPDX-License-Identifier: Apache-2.0.
import logging
from contextlib import closing
from typing import Optional, List
from urllib.parse import urlparse

from openlineage.airflow.utils import (
    get_connection,
    safe_import_airflow
)
from openlineage.airflow.extractors.base import (
    BaseExtractor,
    TaskMetadata
)
from openlineage.client.facet import SqlJobFacet
from openlineage.common.models import (
    DbTableSchema,
    DbColumn
)
from openlineage.common.sql import SqlMeta, parse, DbTableMeta
from openlineage.common.dataset import Source, Dataset

# We use the commonly available columns on all of the following databases:
# BigQuery: https://cloud.google.com/bigquery/docs/information-schema-tables#columns_view
# MySQL: https://dev.mysql.com/doc/refman/8.0/en/information-schema-columns-table.html
# PostgreSQL: https://www.postgresql.org/docs/current/infoschema-columns.html
# Snowflake: https://docs.snowflake.com/en/sql-reference/info-schema/columns.html#columns
_TABLE_SCHEMA = 0
_TABLE_NAME = 1
_COLUMN_NAME = 2
_ORDINAL_POSITION = 3
_DATA_TYPE = 4

logger = logging.getLogger(__name__)


class JdbcExtractor(BaseExtractor):
    default_schema = None

    def __init__(self, operator):
        super().__init__(operator)
        self.conn = None

    @classmethod
    def get_operator_classnames(cls) -> List[str]:
        return ['JdbcOperator']

    def extract(self) -> TaskMetadata:
        # (1) Parse sql statement to obtain input / output tables.
        sql_meta: SqlMeta = parse(self.operator.sql, self.default_schema)

        # (2) Get database connection
        self.conn = get_connection(self._conn_id())

        # (3) Default all inputs / outputs to current connection.
        # NOTE: We'll want to look into adding support for the `database`
        # property that is used to override the one defined in the connection.
        # NOTE: Airflow's JdbcHook takes whole JDBC URI via the "host" field.
        # https://airflow.apache.org/docs/apache-airflow-providers-jdbc/stable/_api/airflow/providers/jdbc/hooks/jdbc/index.html#airflow.providers.jdbc.hooks.jdbc.JdbcHook
        # So we have to parse that field manually to get the following information.
        uri = self.conn.host
        pos = uri.find("://")
        scheme = uri[:pos]
        protocols = scheme.split(':')
        parsed = urlparse(protocols[-1] + uri[pos:])
        source = Source(
            scheme=scheme,
            authority=f'{parsed.hostname}:{parsed.port}',
            connection_url=f'{scheme}://{parsed.hostname}:{parsed.port}{parsed.path}',
        )
        database = parsed.path.lstrip('/')

        # (4) Map input / output tables to dataset objects with source set
        # as the current connection. We need to also fetch the schema for the
        # input tables to format the dataset name as:
        # {schema_name}.{table_name}
        inputs = [
            Dataset.from_table(
                source=source,
                table_name=in_table_schema.table_name.name,
                schema_name=in_table_schema.schema_name,
                database_name=database
            ) for in_table_schema in self._get_table_schemas(
                sql_meta.in_tables
            )
        ]
        outputs = [
            Dataset.from_table_schema(
                source=source,
                table_schema=out_table_schema,
                database_name=database
            ) for out_table_schema in self._get_table_schemas(
                sql_meta.out_tables
            )
        ]

        task_name = f"{self.operator.dag_id}.{self.operator.task_id}"
        run_facets = {}
        job_facets = {
            'sql': SqlJobFacet(self.operator.sql)
        }

        return TaskMetadata(
            name=task_name,
            inputs=[ds.to_openlineage_dataset() for ds in inputs],
            outputs=[ds.to_openlineage_dataset() for ds in outputs],
            run_facets=run_facets,
            job_facets=job_facets
        )

    def _conn_id(self):
        return self.operator.jdbc_conn_id

    def _information_schema_query(self, table_names: str) -> str:
        return f"""
        SELECT table_schema,
        table_name,
        column_name,
        ordinal_position,
        data_type
        FROM information_schema.columns
        WHERE table_name IN ({table_names});
        """

    def _get_hook(self):
        JdbcHook = safe_import_airflow(
            airflow_1_path="airflow.hooks.jdbc_hook.JdbcHook",
            airflow_2_path="airflow.providers.jdbc.hooks.jdbc.JdbcHook"
        )
        return JdbcHook(
            jdbc_conn_id=self.operator.jdbc_conn_id,
        )

    def _get_table_schemas(
            self, table_names: [DbTableMeta]
    ) -> [DbTableSchema]:
        # Avoid querying jdbc by returning an empty array
        # if no table names have been provided.
        if not table_names:
            return []

        # Keeps tack of the schema by table.
        schemas_by_table = {}

        hook = self._get_hook()
        with closing(hook.get_conn()) as conn:
            with closing(conn.cursor()) as cursor:
                table_names_as_str = ",".join(map(
                    lambda name: f"'{name.name}'", table_names
                ))
                cursor.execute(
                    self._information_schema_query(table_names_as_str)
                )
                for row in cursor.fetchall():
                    table_schema_name: str = row[_TABLE_SCHEMA]
                    table_name: DbTableMeta = DbTableMeta(row[_TABLE_NAME])
                    table_column: DbColumn = DbColumn(
                        name=row[_COLUMN_NAME],
                        type=row[_DATA_TYPE],
                        ordinal_position=row[_ORDINAL_POSITION]
                    )

                    # Attempt to get table schema
                    table_key: str = f"{table_schema_name}.{table_name}"
                    table_schema: Optional[DbTableSchema] = schemas_by_table.get(table_key)

                    if table_schema:
                        # Add column to existing table schema.
                        schemas_by_table[table_key].columns.append(table_column)
                    else:
                        # Create new table schema with column.
                        schemas_by_table[table_key] = DbTableSchema(
                            schema_name=table_schema_name,
                            table_name=table_name,
                            columns=[table_column]
                        )

        return list(schemas_by_table.values())
