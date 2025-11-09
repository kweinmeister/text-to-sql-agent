import psycopg2
from psycopg2 import sql
from psycopg2.extensions import connection

from .dialect import DatabaseDialect


class PostgreSQLDialect(DatabaseDialect):
    """PostgreSQL implementation."""

    @property
    def name(self) -> str:
        return "postgresql"

    def get_connection(self, db_uri: str) -> connection:
        return psycopg2.connect(db_uri)

    def get_sqlglot_dialect(self) -> str:
        return "postgres"

    def quote_identifier(self, name: str) -> str:
        return f'"{name}"'

    def _get_ddl_from_db(self, db_uri: str) -> str:
        """Generates the DDL for the schema by querying information_schema."""
        with self.get_connection(db_uri) as conn:
            with conn.cursor() as cursor:
                return self._build_ddl_from_info_schema(cursor)

    def _build_ddl_from_info_schema(self, cursor: psycopg2.extensions.cursor) -> str:
        """
        Helper that queries catalog tables and assembles CREATE TABLE strings.
        """
        cursor.execute(
            "SELECT table_name FROM information_schema.tables WHERE table_schema = 'public' AND table_type = 'BASE TABLE';"
        )
        tables = [row[0] for row in cursor.fetchall()]
        if not tables:
            return ""

        ddl_parts: list[str] = []

        # We can still fetch PKs and FKs in bulk, but column info is easier per table to get the right types.
        # Alternatively, you could do this in bulk too and process it, but this is clear and correct.
        for table_name in tables:
            cols_for_table: list[str] = []

            # --- 1. Fetch Columns ---
            query = sql.SQL("""
                SELECT column_name, udt_name, is_nullable
                FROM information_schema.columns
                WHERE table_name = {tbl} AND table_schema = 'public';
            """).format(tbl=sql.Literal(table_name))
            cursor.execute(query)
            columns_info = cursor.fetchall()

            for col_name, col_type, is_nullable in columns_info:
                # First, convert the specific PG type to a generic one
                generic_type = self._postgres_type_to_generic(col_type)
                # Then, map that generic type to the DDL type string we want the LLM to see
                col_type_ddl = self.map_type_to_ddl(generic_type)
                not_null_str = " NOT NULL" if is_nullable == "NO" else ""
                cols_for_table.append(
                    f"  {self.quote_identifier(col_name)} {col_type_ddl}{not_null_str}"
                )

            # --- 2. Fetch Primary Keys ---
            query = sql.SQL("""
                SELECT kcu.column_name
                FROM information_schema.table_constraints AS tc
                JOIN information_schema.key_column_usage AS kcu
                ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_name = {tbl} AND tc.table_schema = 'public';
            """).format(tbl=sql.Literal(table_name))
            cursor.execute(query)
            primary_keys = [row[0] for row in cursor.fetchall()]

            if primary_keys:
                pk_cols: list[str] = [self.quote_identifier(pk) for pk in primary_keys]
                cols_for_table.append(f"  PRIMARY KEY ({', '.join(pk_cols)})")

            # --- 3. Fetch Foreign Keys ---
            query = sql.SQL("""
                SELECT
                    kcu.column_name,
                    ccu.table_name AS foreign_table_name,
                    ccu.column_name AS foreign_column_name
                FROM
                    information_schema.table_constraints AS tc
                    JOIN information_schema.key_column_usage AS kcu
                    ON tc.constraint_name = kcu.constraint_name AND tc.table_schema = kcu.table_schema
                    JOIN information_schema.constraint_column_usage AS ccu
                    ON ccu.constraint_name = tc.constraint_name AND ccu.table_schema = tc.table_schema
                WHERE tc.constraint_type = 'FOREIGN KEY' AND tc.table_name = {tbl} AND tc.table_schema = 'public';
            """).format(tbl=sql.Literal(table_name))
            cursor.execute(query)
            fks = cursor.fetchall()
            for from_col, to_table, to_col in fks:
                cols_for_table.append(
                    f"  FOREIGN KEY ({self.quote_identifier(from_col)}) REFERENCES {self.quote_identifier(to_table)} ({self.quote_identifier(to_col)})"
                )

            # --- 4. Assemble the CREATE TABLE statement ---
            ddl_parts.append(
                f"CREATE TABLE {self.quote_identifier(table_name)} (\n"
                + ",\n".join(cols_for_table)
                + "\n);"
            )

        return "\n\n".join(ddl_parts)

    def map_type_to_ddl(self, sql_type: str) -> str:
        mapping: dict[str, str] = {
            "text": "TEXT",
            "number": "NUMERIC",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",
            "timestamp": "TIMESTAMP",
            "date": "DATE",
        }
        return mapping.get(sql_type.lower(), "TEXT")

    def _postgres_type_to_generic(self, postgres_type: str) -> str:
        pg_type = postgres_type.lower().strip()

        if "int" in pg_type:
            return "INTEGER"
        if any(t in pg_type for t in ["char", "text"]):
            return "TEXT"
        if any(t in pg_type for t in ["numeric", "decimal", "real", "float", "double"]):
            return "NUMBER"
        if pg_type.startswith("timestamp"):
            return "TIMESTAMP"
        if "date" in pg_type:
            return "DATE"
        if "bool" in pg_type:
            return "BOOLEAN"

        return "TEXT"
