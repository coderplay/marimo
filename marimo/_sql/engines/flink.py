from __future__ import annotations

from typing import Any, Literal, Optional, Union
from urllib.parse import urljoin

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
from marimo._sql.sql import ConnectionString
from marimo._types.ids import VariableName

LOGGER = _loggers.marimo_logger()


@register_engine
class FlinkSQLEngine(SQLEngine):
    """Flink SQL Gateway engine that connects via REST API."""

    def __init__(
        self,
        connection: ConnectionString = "http://localhost:8083",
        engine_name: Optional[VariableName] = None
    ) -> None:
        self._base_url = connection
        self._engine_name = engine_name
        self._session_id = None

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

    def _create_session(self) -> None:
        """Create a new Flink SQL Gateway session."""
        LOGGER.info("Creating a new Flink SQL Gateway session")
        response = requests.post(
            urljoin(self._base_url, "/v1/sessions"),
            json={"sessionName": "marimo_session"}
        ).json()

        LOGGER.info(f"Session creation response: {response}")

        if "sessionHandle" in response:
            self._session_id = response["sessionHandle"]
        else:
            raise ValueError("Failed to get sessionHandle from Flink SQL Gateway response")

    def execute(self, query: str) -> Any:
        """Execute a SQL query via Flink SQL Gateway REST API."""
        try:
            if not self._session_id:
                self._create_session()

            LOGGER.info(f"Executing query with session ID: {self._session_id}")
            
            # Step 1: Execute the statement to get the operation handle
            statement_url = urljoin(self._base_url, f"/v1/sessions/{self._session_id}/statements")
            LOGGER.info(f"Submitting statement to: {statement_url}")
            
            statement_response = requests.post(
                statement_url, 
                json={"statement": query}
            )
            statement_response.raise_for_status()
            statement_data = statement_response.json()
            
            if "operationHandle" not in statement_data:
                raise ValueError(f"Missing operationHandle in response: {statement_data}")
                
            operation_handle = statement_data["operationHandle"]
            LOGGER.info(f"Got operation handle: {operation_handle}")
            
            # Step 2: Fetch the results (starting with page 0)
            results_url = urljoin(
                self._base_url, 
                f"/v1/sessions/{self._session_id}/operations/{operation_handle}/result/0"
            )
            LOGGER.info(f"Fetching results from: {results_url}")
            
            results_response = requests.get(results_url)
            results_response.raise_for_status()
            results_data = results_response.json()
            
            # Step 3: Handle pagination if there are more results
            all_data = []
            if "results" in results_data and "data" in results_data["results"]:
                all_data.extend(results_data["results"]["data"])
            
            # Follow pagination links if available
            while "nextResultUri" in results_data and results_data["nextResultUri"]:
                next_url = results_data["nextResultUri"]
                LOGGER.info(f"Fetching next page from: {next_url}")
                
                next_response = requests.get(next_url)
                next_response.raise_for_status()
                results_data = next_response.json()
                
                if "results" in results_data and "data" in results_data["results"]:
                    all_data.extend(results_data["results"]["data"])
            
            # Process the results - extract column info from the first response
            if "results" in results_data and "columns" in results_data["results"]:
                columns = results_data["results"]["columns"]
                column_names = [col["name"] for col in columns]
                
                # Convert results to a DataFrame
                import polars as pl
                
                # Extract field values from each row
                processed_data = []
                for row in all_data:
                    if "fields" in row:
                        processed_data.append(dict(zip(column_names, row["fields"])))
                
                if processed_data:
                    return pl.DataFrame(processed_data)
            
            # If we can't convert to a DataFrame, return the raw data
            return all_data
            
        except Exception as e:
            LOGGER.error(f"Error executing query: {e}", exc_info=True)
            raise

    @staticmethod
    def is_compatible(var: Any) -> bool:
        if not isinstance(var, str):
            return False
        import re
        pattern = r'^https?://[^:]+:8083'
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