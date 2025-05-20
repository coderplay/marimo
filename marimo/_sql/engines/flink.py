from __future__ import annotations

import time
from typing import Any, Literal, Optional, Union

import requests

from marimo import _loggers
from marimo._data.models import (
    Database,
    DataTable,
    DataTableColumn,
    DataType,
    Schema,
)
from marimo._sql.engines.types import (
    InferenceConfig,
    SQLEngine,
    register_engine,
)
from marimo._types.ids import VariableName

LOGGER = _loggers.marimo_logger()


@register_engine
class FlinkSQLEngine(SQLEngine):
    """Flink SQL Gateway engine that connects via REST API."""

    def __init__(
        self,
        connection: str = "http://localhost:8083/v1/sessions",
        engine_name: Optional[VariableName] = None
    ) -> None:
        self._session_url = connection
        self._engine_name = engine_name

    @property
    def source(self) -> str:
        return "FlinkSQL"

    @property
    def dialect(self) -> str:
        return "flinksql"

    @property
    def inference_config(self) -> InferenceConfig:
        return InferenceConfig(
            auto_discover_schemas=True,
            auto_discover_tables="auto",
            auto_discover_columns=False,
        )

    def _execute_statement(self, query: str) -> str:
        """Execute a SQL statement and return the operation handle."""
        response = requests.post(
            f"{self._session_url}/statements",
            json={"statement": query}
        ).json()
        operation_handle = response.get("operationHandle")
        if not operation_handle:
            raise ValueError("No operation handle returned from Flink SQL Gateway")
        return operation_handle

    def _wait_for_results(self, operation_handle: str) -> dict:
        """Wait for query results to be ready."""
        while True:
            response = requests.get(
                f"{self._session_url}/operations/{operation_handle}/result/0"
            ).json()
            # print(f"Flink SQL Gateway response: {response}")
            if response.get("resultType", "") != "NOT_READY":
                return response
            time.sleep(0.1)

    def _process_results_page(self, results: dict, columns: list[str]) -> list[dict]:
        """Process a single page of results."""
        if not results.get("data"):
            return []

        return [
            dict(zip(columns, row["fields"]))
            for row in results["data"]
            if "fields" in row
        ]

    def _parse_next_token(self, next_result_uri: Optional[str]) -> Optional[int]:
        """Parse the next token from the result URI."""
        if not next_result_uri:
            return None
        parts = next_result_uri.split("/")
        token = parts[-1].split("?")[0]  # Remove query string
        return int(token)

    def _fetch_results(self, initial_response: dict, operation_handle: str, max_results: int = 50) -> tuple[list[str], list[dict]]:
        """Fetch and process results up to max_results or until end."""
        # print(f"Initial response: {initial_response}")
        if "results" not in initial_response:
            return [], []

        results = initial_response["results"]
        columns = [col["name"] for col in results.get("columns", [])]
        data = self._process_results_page(results, columns)

        # For non-query results, return immediately
        if not initial_response.get("isQueryResult", False):
            return columns, data

        # For query results, handle pagination
        response = initial_response
        while len(data) < max_results:
            next_token = self._parse_next_token(response.get("nextResultUri"))
            if next_token is None:
                break
            next_url = f"{self._session_url}/operations/{operation_handle}/result/{next_token}"
            response = requests.get(next_url).json()
            if "results" in response and "data" in response["results"]:
                new_data = self._process_results_page(response["results"], columns)
                data.extend(new_data)

        return columns, data[:max_results]

    def _convert_to_dataframe(self, data: list[dict], output_format: str) -> Any:
        """Convert result data to specified output format."""
        if not data:
            return None

        if output_format == "native":
            return data

        if output_format == "polars":
            import polars as pl
            return pl.DataFrame(data)
        if output_format == "lazy-polars":
            import polars as pl
            return pl.DataFrame(data).lazy()
        if output_format == "pandas":
            import pandas as pd
            return pd.DataFrame(data)

        # Auto format handling
        from marimo._dependencies.dependencies import DependencyManager

        if DependencyManager.polars.has():
            import polars as pl
            try:
                return pl.DataFrame(data)
            except (pl.exceptions.PanicException, pl.exceptions.ComputeError):
                LOGGER.info("Failed to convert to polars, falling back to pandas")

        if DependencyManager.pandas.has():
            import pandas as pd
            try:
                return pd.DataFrame(data)
            except Exception as e:
                LOGGER.warning("Failed to convert dataframe", exc_info=e)
                return None

        from marimo._sql.utils import raise_df_import_error
        raise_df_import_error("polars[pyarrow]")

    def _execute_and_get_raw_results(self, query: str, max_results: int = 50) -> list[Any]:
        """Execute a SQL query and return raw results without dataframe conversion."""
        operation_handle = self._execute_statement(query)
        response = self._wait_for_results(operation_handle)
        columns, data = self._fetch_results(response, operation_handle, max_results)
        return data

    def execute(self, query: str) -> Any:
        """Execute a SQL query via Flink SQL Gateway REST API."""
        data = self._execute_and_get_raw_results(query)
        return self._convert_to_dataframe(data, self.sql_output_format())

    @staticmethod
    def is_compatible(var: Any) -> bool:
        if not isinstance(var, str):
            return False
        import re
        pattern = r'^https?://[^:]+:8083/v1/sessions/[^/]+'
        return bool(re.match(pattern, var))

    def _extract_name(self, entry: Union[str, dict], key: str) -> str:
        """Extract name from either a string or a dictionary response."""
        if isinstance(entry, dict):
            return str(entry.get(key, ""))
        return str(entry)

    def get_default_database(self) -> Optional[str]:
        # Flink catalogs map to marimo databases
        results = self._execute_and_get_raw_results("SHOW CATALOGS")
        if not results:
            return None
        return self._extract_name(results[0], "catalog name")

    def get_default_schema(self) -> Optional[str]:
        # In Flink, databases correspond to marimo schemas
        results = self._execute_and_get_raw_results("SHOW DATABASES")
        if not results:
            return None
        return self._extract_name(results[0], "database name")

    def get_databases(
        self,
        *,
        include_schemas: Union[bool, Literal["auto"]],
        include_tables: Union[bool, Literal["auto"]],
        include_table_details: Union[bool, Literal["auto"]],
    ) -> list[Database]:
        """Fetch all databases (Flink catalogs) from Flink SQL Gateway."""
        databases = []
        for catalog_entry in self._execute_and_get_raw_results("SHOW CATALOGS"):
            catalog_name = self._extract_name(catalog_entry, "catalog name")
            schemas = []
            if include_schemas:
                # Get databases from catalog (which are marimo schemas)
                for db_entry in self._execute_and_get_raw_results(f"SHOW DATABASES FROM {catalog_name}"):
                    db_name = self._extract_name(db_entry, "database name")
                    tables = []
                    if include_tables:
                        # Now we use catalog_name.db_name to get tables
                        for table_entry in self._execute_and_get_raw_results(f"SHOW TABLES FROM {catalog_name}.{db_name}"):
                            table_name = self._extract_name(table_entry, "table name")
                            table = DataTable(
                                source_type="connection",
                                source=self.dialect,
                                name=table_name,
                                num_rows=None,
                                num_columns=None,
                                variable_name=None,
                                engine=self._engine_name,
                                columns=[],
                                primary_keys=[],
                                indexes=[],
                            )
                            if include_table_details:
                                table = self.get_table_details(table_name, db_name, catalog_name) or table
                            tables.append(table)
                    schemas.append(Schema(name=db_name, tables=tables))
            databases.append(Database(
                name=catalog_name,
                schemas=schemas,
                dialect=self.dialect
            ))
        return databases

    def get_tables_in_schema(
        self, *, schema: str, database: str, include_table_details: bool
    ) -> list[DataTable]:
        """Return all tables in a schema (Flink database)."""
        tables = []
        # Here database parameter is actually catalog name, and schema is database name in Flink
        for table_entry in self._execute_and_get_raw_results(f"SHOW TABLES FROM {database}.{schema}"):
            table_name = self._extract_name(table_entry, "table name")
            table = DataTable(
                source_type="connection",
                source=self.dialect,
                name=table_name,
                num_rows=None,
                num_columns=None,
                variable_name=None,
                engine=self._engine_name,
                columns=[],
                primary_keys=[],
                indexes=[],
            )
            if include_table_details:
                table = self.get_table_details(table_name, schema, database) or table
            tables.append(table)
        return tables

    def _map_flink_type_to_marimo(self, flink_type: str) -> DataType:
        """Map Flink SQL type to marimo DataType."""
        flink_type = flink_type.upper().split('(')[0]  # Remove precision/scale
        # Integer types
        if flink_type in ('TINYINT', 'SMALLINT', 'INT', 'INTEGER', 'BIGINT'):
            return "integer"
        # Floating point types
        if flink_type in ('FLOAT', 'DOUBLE', 'DECIMAL', 'NUMERIC'):
            return "number"
        # String types
        if flink_type in ('CHAR', 'VARCHAR', 'STRING', 'TEXT'):
            return "string"
        # Boolean type
        if flink_type == 'BOOLEAN':
            return "boolean"
        # Date/Time types
        if flink_type == 'DATE':
            return "date"
        if flink_type == 'TIME':
            return "time"
        if flink_type in ('TIMESTAMP', 'DATETIME'):
            return "datetime"
        # Default to unknown for unsupported types
        return "unknown"

    def get_table_details(
        self, table_name: str, schema_name: str, database_name: str
    ) -> Optional[DataTable]:
        """Get details for a specific table.

        Args:
            table_name: Name of the table
            schema_name: Name of the schema (Flink database)
            database_name: Name of the database (Flink catalog)
        """
        results = self._execute_and_get_raw_results(
            f"DESCRIBE {database_name}.{schema_name}.{table_name}"
        )

        primary_keys = []
        columns = []

        for row in results:
            # Extract primary key information from the 'key' field
            key_info = str(row.get("key", ""))
            if key_info and key_info.startswith("PRI"):
                # Extract column name from PRI(column_name)
                key_match = key_info.strip("PRI()").strip()
                if key_match:
                    primary_keys.append(key_match)

            flink_type = str(row.get("type", ""))
            columns.append(
                DataTableColumn(
                    name=str(row.get("name", "")),
                    type=self._map_flink_type_to_marimo(flink_type),
                    external_type=flink_type,
                    sample_values=[]  # We don't have sample values from DESCRIBE
                )
            )

        return DataTable(
            source_type="connection",
            source=self.dialect,
            name=table_name,
            num_rows=None,
            num_columns=len(columns),
            variable_name=None,
            engine=self._engine_name,
            columns=columns,
            primary_keys=primary_keys,
            indexes=[],  # Flink doesn't expose index information in DESCRIBE TABLE
        )
