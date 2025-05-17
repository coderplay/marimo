export type DBLogoName =
  | "postgres"
  | "mysql"
  | "sqlite"
  | "duckdb"
  | "motherduck"
  | "snowflake"
  | "clickhouse"
  | "timeplus"
  | "flink";

export const DatabaseLogos: Record<DBLogoName, string> = {
  postgres: "https://www.postgresql.org/media/img/about/press/elephant.png",
  mysql: "https://www.mysql.com/common/logos/logo-mysql-170x115.png",
  sqlite: "https://www.sqlite.org/images/sqlite370_banner.gif",
  duckdb: "https://duckdb.org/images/duckdb_logo_icon.svg",
  motherduck: "https://motherduck.com/images/motherduck-logo.svg",
  snowflake: "https://www.snowflake.com/wp-content/themes/snowflake/assets/img/snowflake-logo.svg",
  clickhouse: "https://clickhouse.com/images/clickhouse-logo.svg",
  timeplus: "https://www.timeplus.com/images/timeplus-logo.svg",
  flink: "https://flink.apache.org/img/flink-logo.svg",
}; 