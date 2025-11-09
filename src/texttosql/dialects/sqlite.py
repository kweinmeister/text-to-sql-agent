import sqlite3
from sqlite3 import Connection
from typing import Any

from .dialect import DatabaseDialect


class SQLiteDialect(DatabaseDialect):
    """SQLite implementation."""

    @property
    def name(self) -> str:
        return "sqlite"

    def get_connection(self, db_uri: str) -> Connection:
        return sqlite3.connect(db_uri)

    def get_sqlglot_dialect(self) -> str:
        return "sqlite"

    def quote_identifier(self, name: str) -> str:
        """Quotes an identifier with standard double quotes."""
        return f'"{name}"'

    def _get_ddl_from_db(self, db_uri: str) -> str:
        """
        Generates the DDL for all tables in the SQLite database by querying
        the sqlite_master table and using PRAGMA statements. This aligns with
        the metadata-driven approach used in the PostgreSQL dialect.
        """
        ddl_parts: list[str] = []
        with self.get_connection(db_uri) as conn:
            cursor = conn.cursor()

            # 1. Get all table names
            cursor.execute(
                "SELECT name FROM sqlite_master WHERE type='table' AND name NOT LIKE 'sqlite_%';"
            )
            tables = [row[0] for row in cursor.fetchall()]
            if not tables:
                return ""

            for table_name in tables:
                cols_for_table: list[str] = []

                # --- 2. Fetch Columns and Primary Keys using PRAGMA table_info ---
                cursor.execute(f'PRAGMA table_info("{table_name}")')
                columns_info = cursor.fetchall()

                pk_order_map: dict[int, str] = {}

                for col_info in columns_info:
                    # col_info structure: (cid, name, type, notnull, dflt_value, pk)
                    _, col_name, col_type, notnull, _, pk = col_info

                    generic_type = self._sqlite_type_to_generic(col_type)
                    col_type_ddl = self.map_type_to_ddl(generic_type)
                    not_null_str = " NOT NULL" if notnull else ""

                    cols_for_table.append(
                        f"  {self.quote_identifier(col_name)} {col_type_ddl}{not_null_str}"
                    )

                    if pk > 0:
                        pk_order_map[pk] = col_name

                # Add primary key constraint if it exists (handles composite keys)
                if pk_order_map:
                    sorted_pks = [pk_order_map[k] for k in sorted(pk_order_map.keys())]
                    pk_cols_quoted = [self.quote_identifier(pk) for pk in sorted_pks]
                    cols_for_table.append(
                        f"  PRIMARY KEY ({', '.join(pk_cols_quoted)})"
                    )

                # --- 3. Fetch Foreign Keys using PRAGMA foreign_key_list ---
                cursor.execute(f'PRAGMA foreign_key_list("{table_name}")')
                fks_info = cursor.fetchall()

                # Group FKs by their ID to handle composite foreign keys
                fk_constraints: dict[int, dict[str, Any]] = {}
                for fk_info in fks_info:
                    # fk_info: (id, seq, table, from, to, on_update, on_delete, match)
                    fk_id, _, to_table, from_col, to_col, _, _, _ = fk_info
                    if fk_id not in fk_constraints:
                        fk_constraints[fk_id] = {
                            "to_table": to_table,
                            "from_cols": [],
                            "to_cols": [],
                        }
                    fk_constraints[fk_id]["from_cols"].append(from_col)
                    fk_constraints[fk_id]["to_cols"].append(to_col)

                for fk in fk_constraints.values():
                    from_cols_quoted = [
                        self.quote_identifier(c) for c in fk["from_cols"]
                    ]
                    to_cols_quoted = [self.quote_identifier(c) for c in fk["to_cols"]]
                    to_table_quoted = self.quote_identifier(fk["to_table"])

                    cols_for_table.append(
                        f"  FOREIGN KEY ({', '.join(from_cols_quoted)}) REFERENCES {to_table_quoted} ({', '.join(to_cols_quoted)})"
                    )

                # --- 4. Assemble the CREATE TABLE statement ---
                ddl_parts.append(
                    f"CREATE TABLE {self.quote_identifier(table_name)} (\n"
                    + ",\n".join(cols_for_table)
                    + "\n);"
                )

        return "\n\n".join(ddl_parts)

    def map_type_to_ddl(self, sql_type: str) -> str:
        """Map a generic type to a database-specific DDL type for LLM consumption."""
        mapping: dict[str, str] = {
            "text": "TEXT",
            "number": "REAL",
            "integer": "INTEGER",
            "boolean": "BOOLEAN",  # LLMs expect this — SQLite will still store as 0/1
        }
        return mapping.get(sql_type.lower(), "TEXT")

    def _sqlite_type_to_generic(self, sqlite_type: str) -> str:
        """
        Maps a specific SQLite type to a generic category used by map_type_to_ddl,
        based on SQLite's Type Affinity rules.
        """
        sqlite_type_lower = sqlite_type.lower().strip()

        if "int" in sqlite_type_lower:
            return "integer"
        if "bool" in sqlite_type_lower:
            return "boolean"
        if any(t in sqlite_type_lower for t in ["char", "clob", "text"]):
            return "text"
        if any(
            t in sqlite_type_lower
            for t in ["real", "floa", "doub", "numeric", "decimal"]
        ):
            return "number"

        # BLOB has no affinity and is tricky for text-based models.
        # DATETIME types also have no affinity (usually stored as TEXT, REAL, or INTEGER).
        # Defaulting to 'text' is a safe bet for unknown/other types.
        return "text"
