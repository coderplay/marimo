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
        return "flinksql"

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
            print(f"Flink SQL Gateway response: {response}")
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

    def _fetch_results(self, initial_response: dict, operation_handle: str, max_results: int = 200) -> tuple[list[str], list[dict]]:
        """Fetch and process results up to max_results or until end."""
        print(f"Initial response: {initial_response}")
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

    def execute(self, query: str) -> Any:
        """Execute a SQL query via Flink SQL Gateway REST API."""
        try:
            # Execute and wait for results
            operation_handle = self._execute_statement(query)
            response = self._wait_for_results(operation_handle)
            # Fetch up to 200 results or until end
            columns, data = self._fetch_results(response, operation_handle, max_results=200)
            # Convert to requested format
            return self._convert_to_dataframe(data, self.sql_output_format())
        except ModuleNotFoundError as e:
            raise ModuleNotFoundError(str(e)) from e
        except Exception as e:
            raise ValueError(f"Failed to execute query: {str(e)}") from e

    @staticmethod
    def is_compatible(var: Any) -> bool:
        if not isinstance(var, str):
            return False
        import re
        pattern = r'^https?://[^:]+:8083/v1/sessions/[^/]+'
        return bool(re.match(pattern, var))

    def get_default_database(self) -> Optional[str]:
        try:
            result = self.execute("SHOW DATABASES")
            return result[0] if result else None
        except Exception:
            LOGGER.warning("Failed to get default database", exc_info=True)
            return None

    def get_default_schema(self) -> Optional[str]:
        try:
            result = self.execute("SHOW SCHEMAS")
            return result[0] if result else None
        except Exception:
            LOGGER.warning("Failed to get default schema", exc_info=True)
            return None

    def get_databases(
        self,
        *,
        include_schemas: Union[bool, Literal["auto"]],
        include_tables: Union[bool, Literal["auto"]],
        include_table_details: Union[bool, Literal["auto"]],
    ) -> list[Database]:
        """Fetch all databases from Flink SQL Gateway."""
        try:
            databases = []
            for db_name in self.execute("SHOW DATABASES"):
                schemas = []
                if include_schemas:
                    for schema_name in self.execute(f"SHOW SCHEMAS FROM {db_name}"):
                        tables = []
                        if include_tables:
                            for table_name in self.execute(f"SHOW TABLES FROM {db_name}.{schema_name}"):
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
                                    table = self.get_table_details(table_name, schema_name, db_name) or table
                                tables.append(table)
                        schemas.append(Schema(name=schema_name, tables=tables))
                databases.append(Database(name=db_name, schemas=schemas))
            return databases
        except Exception:
            LOGGER.warning("Failed to get databases", exc_info=True)
            return []

    def get_tables_in_schema(
        self, *, schema: str, database: str, include_table_details: bool
    ) -> list[DataTable]:
        """Return all tables in a schema."""
        try:
            tables = []
            for table_name in self.execute(f"SHOW TABLES FROM {database}.{schema}"):
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
        except Exception:
            LOGGER.warning(
                f"Failed to get tables in schema {schema} of database {database}",
                exc_info=True
            )
            return []

    def get_table_details(
        self, table_name: str, schema_name: str, database_name: str
    ) -> Optional[DataTable]:
        """Get details for a specific table."""
        try:
            result = self.execute(f"DESCRIBE {database_name}.{schema_name}.{table_name}")
            columns = [
                DataTableColumn(
                    name=col["name"],
                    type=DataType.STRING,  # Flink types need to be mapped
                    nullable=col.get("nullable", True),
                )
                for col in result
            ]
            return DataTable(
                source_type="connection",
                source=self.dialect,
                name=table_name,
                num_rows=None,
                num_columns=len(columns),
                variable_name=None,
                engine=self._engine_name,
                columns=columns,
                primary_keys=[],
                indexes=[],
            )
        except Exception:
            LOGGER.warning(
                f"Failed to get table details for {table_name}",
                exc_info=True
            )
            return None