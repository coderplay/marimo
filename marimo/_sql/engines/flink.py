from __future__ import annotations

from typing import Any, Literal, Optional, Union
from urllib.parse import urljoin

import requests

from marimo._data.models import (
    Database,
    DataTable,
    DataTableColumn,
    DataType,
    Schema,
)
from marimo._dependencies.dependencies import DependencyManager
from marimo._sql.engines.registry import register_engine
from marimo._sql.engines.types import InferenceConfig, SQLEngine
from marimo._utils.logging import get_logger

LOGGER = get_logger(__name__)

class FlinkSQLEngine(SQLEngine):
    """Flink SQL Gateway engine that connects via REST API."""

    def __init__(
        self, 
        connection: Any, 
        engine_name: Optional[str] = None,
        base_url: str = "http://localhost:8083",
        session_id: Optional[str] = None
    ) -> None:
        self._base_url = base_url
        self._session_id = session_id
        self._engine_name = engine_name or "flink"
        self._connection = connection

    @property
    def source(self) -> str:
        return "flink"

    @property
    def dialect(self) -> str:
        return "flink"

    @property
    def inference_config(self) -> InferenceConfig:
        return InferenceConfig(
            auto_discover_schemas=True,
            auto_discover_tables="auto",
            auto_discover_columns=False,
        )

    def execute(self, query: str) -> Any:
        """Execute a SQL query via Flink SQL Gateway REST API."""
        if not self._session_id:
            # Create a new session if none exists
            response = requests.post(
                urljoin(self._base_url, "/v1/sessions"),
                json={"session_name": "marimo_session"}
            )
            response.raise_for_status()
            self._session_id = response.json()["session_id"]

        # Execute the query
        response = requests.post(
            urljoin(self._base_url, f"/v1/sessions/{self._session_id}/statements"),
            json={"statement": query}
        )
        response.raise_for_status()

        # Get the results
        result = response.json()
        if "results" in result:
            return result["results"]
        return None

    @staticmethod
    def is_compatible(var: Any) -> bool:
        return isinstance(var, FlinkSQLEngine)

    def get_default_database(self) -> Optional[str]:
        try:
            result = self.execute("SHOW DATABASES")
            if result and len(result) > 0:
                return result[0]
            return None
        except Exception:
            LOGGER.warning("Failed to get default database", exc_info=True)
            return None

    def get_default_schema(self) -> Optional[str]:
        try:
            result = self.execute("SHOW SCHEMAS")
            if result and len(result) > 0:
                return result[0]
            return None
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
            result = self.execute("SHOW DATABASES")

            for db_name in result:
                schemas = []
                if include_schemas:
                    schema_result = self.execute(f"SHOW SCHEMAS FROM {db_name}")
                    for schema_name in schema_result:
                        tables = []
                        if include_tables:
                            table_result = self.execute(
                                f"SHOW TABLES FROM {db_name}.{schema_name}"
                            )
                            for table_name in table_result:
                                table_details = None
                                if include_table_details:
                                    table_details = self.get_table_details(
                                        table_name, schema_name, db_name
                                    )
                                tables.append(
                                    DataTable(
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
                                )
                        schemas.append(
                            Schema(
                                name=schema_name,
                                tables=tables,
                            )
                        )
                databases.append(
                    Database(
                        name=db_name,
                        schemas=schemas,
                    )
                )
            return databases
        except Exception:
            LOGGER.warning("Failed to get databases", exc_info=True)
            return []

    def get_table_details(
        self, table_name: str, schema_name: str, database_name: str
    ) -> Optional[DataTable]:
        """Get details for a specific table."""
        try:
            result = self.execute(
                f"DESCRIBE {database_name}.{schema_name}.{table_name}"
            )

            columns = []
            for col in result:
                columns.append(
                    DataTableColumn(
                        name=col["name"],
                        type=DataType.STRING,  # Flink types need to be mapped
                        nullable=col.get("nullable", True),
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
                primary_keys=[],
                indexes=[],
            )
        except Exception:
            LOGGER.warning(
                f"Failed to get table details for {table_name}", 
                exc_info=True
            )
            return None 