# 🤖 Text-to-SQL Agent

A multi-agent system that converts natural language questions into **accurate, executable SQL queries** using Google’s Gemini, Flash, SQLGlot validation, and a self-correcting loop — supporting both SQLite and PostgreSQL databases.

> ✅ **Supports SQLite and PostgreSQL** — with an extensible architecture that can be easily extended to MySQL, SQL Server, and other SQL dialects via the `DatabaseDialect` interface.

Perfect for building AI-powered data exploration tools, internal dashboards, or conversational analytics — **no SQL knowledge required**.

---

## 🚀 Quick Start: Use with Sakila Database via ADK

Try it in under 5 minutes with the **Sakila video rental database**.

### 1. Download the Sakila SQLite Database

```bash
cd src/texttosql
curl -O https://raw.githubusercontent.com/bradleygrant/sakila-sqlite3/main/sakila_master.db
```

> 💡 Or [download it manually](https://github.com/bradleygrant/sakila-sqlite3/blob/main/sakila_master.db)

### 2. Configure Your `.env`

Verify that `src/texttosql/.env` includes your database path:

```env
DB_DIALECT=sqlite
DB_URI=src/texttosql/sakila_master.db
MODEL_NAME=gemini-2.5-flash
```

> ✅ Confirm the path is correct: `ls -la sakila_master.db`

> 💡 You can change the Gemini model by updating `MODEL_NAME` in the `.env` file.

### 3. Run Your First Query!

> 🐘 **PostgreSQL Support**: To use with PostgreSQL instead of SQLite, set `DB_DIALECT=postgresql` and update `DB_URI` to your PostgreSQL connection string (e.g., `postgresql://user:password@localhost:5432/your_database`) in your `.env` file. The same command works for both databases.

Ask a natural language question — no SQL needed:

```bash
echo "How many films are in the action category?" | adk run src/texttosql
```

> 💡 For a cleaner output showing just the final SQL query, use:
> ```bash
> echo "How many films are in the action category?" | adk run src/texttosql 2>&1 | awk '/\[/ {f=0} /\[SQLCorrectionLoop\]:/{f=1; sub(/.*: /,"")} f && !/^\[user\]/ {print}'
> ```

### ✅ Output:

```sql
SELECT
  COUNT(f.film_id)
FROM film AS f
JOIN film_category AS fc
  ON f.film_id = fc.film_id
JOIN category AS c
  ON fc.category_id = c.category_id
WHERE
  c.name = 'Action';
```

### 🔍 Verify It Works

The command will output the SQL query directly. You can also run the query against the database to verify the results:

```bash
sqlite3 src/texttosql/sakila_master.db
```

```sql
SELECT
  COUNT(f.film_id)
FROM film AS f
JOIN film_category AS fc
  ON f.film_id = fc.film_id
JOIN category AS c
  ON fc.category_id = c.category_id
WHERE
  c.name = 'Action';
```

You’ll see the answer — proof your AI just wrote correct SQL!

---

## 💬 Interactive Testing with `adk web`

For interactive testing and debugging, you can use the `adk web` command to launch a local web server with a chat interface.

```bash
adk web src
```

This will start a server (usually on `http://127.0.0.1:8000`) where you can ask questions in a conversational format.

> ✅ **Note on State**: This agent is designed for **stateless operations**. Each time you send a message, the agent starts fresh. It clears the previous SQL query and results to ensure every question is treated as a new, independent request. This is by design to make the agent reliable for single-shot API-like calls.

---

## 🧠 How It Works: Agent Architecture

Here’s how your question becomes SQL — step by step:

```mermaid
graph TD
    A[User Question<br>Top films in action category?] --> B[Root Agent]
    B --> C[Schema Extractor Agent]
    C --> D[Extract DB Schema<br>Tables, Columns, FKs]
    D --> E[SQL Generator Agent]
    E --> F[Generate Initial SQL<br>Using Gemini + Schema]
    F --> G[SQL Processor Loop]
    G --> H[Validate SQL<br>SQLGlot: Syntax & Semantics]
    H --> I{Valid?}
    I -- Yes --> J[Execute SQL<br>Against SQLite DB]
    J --> K[Return Final SQL<br>& Exit Loop]
    I -- No --> L[Error Router Agent]
    L --> M[SQL Corrector Agent]
    M --> N[Retry with Error Context<br>Max 3 attempts]
    N --> G
    style A fill:#4CAF50,stroke:#333,color:#fff
    style K fill:#2196F3,stroke:#333,color:#fff
    style G fill:#FF9800,stroke:#333,color:#000
    style H fill:#F44336,stroke:#333,color:#fff
    style I fill:#9E9E9E,stroke:#333,color:#fff
    style L fill:#607D8B,stroke:#333,color:#fff
    style M fill:#607D8B,stroke:#333,color:#fff
```

### 🔍 Breakdown

| Agent | Role |
|-------|------|
| **Schema Extractor** | Reads `sakila_master.db` and converts schema into LLM-readable DDL |
| **SQL Generator** | Uses **Gemini** to generate SQL from question + schema |
| **SQL Validator** | Uses **SQLGlot** to check if tables/columns exist and syntax is valid |
| **SQL Executor** | Runs query against SQLite and captures results/errors |
| **CorrectionLoopAgent** | Detects failures and triggers correction |
| **SQL Corrector** | Re-generates SQL with error context |

> ✅ The system **self-corrects** — even if the first SQL is wrong, it learns from errors.

---

## 🧠 Agent State Schema

This document outlines the schema of the `ctx.session.state` object, which is used to maintain state across the various agents in the text-to-SQL pipeline. Understanding this schema is crucial for debugging, extending, and maintaining the system.

### State Object Structure

The `state` object is a dictionary that evolves as the agent pipeline executes. Below are the keys that are set at different stages of the process.

| Key | Type | Set By | Description |
|---|---|---|---|
| `message` | `str` | `capture_user_message_to_state` (in `agent.py`) | The user's original natural language query. |
| `db_schema_dict` | `dict` | `load_schema_into_state` (tool) | A dictionary representation of the database schema, extracted by `extract_sqlite_schema`. |
| `schema_ddl` | `str` | `schema_extractor_agent` | The `CREATE TABLE` statements for the database schema, formatted for the LLM. |
| `sql_query` | `str` | `sql_generator_agent`, `sql_corrector_agent` | The generated SQL query. This value is overwritten by the corrector agent upon a failed attempt. |
| `validation_result` | `dict` | `validate_sql` (tool) | A dictionary containing the status of the SQL validation (`"status": "success"` or `"status": "error"`) and any `errors` from SQLGlot. |
| `execution_result` | `dict` | `execute_sql` (tool) | A dictionary containing the status of the SQL execution (`"status": "success"` or `"status": "error"`), the query `result` data, or an `error_message`. |
| `final_sql_query` | `str` | `sql_processing_agent` | The final, successfully validated and executed SQL query returned to the user. |

### Example State Flow

1.  **Initial State**: `{}` (empty)
2.  **After `capture_user_message_to_state`**: `{"message": "How many customers..."}`
3.  **After `schema_extractor_agent`**: `{"message": "...", "db_schema_dict": {...}, "schema_ddl": "CREATE TABLE..."}`
4.  **After `sql_generator_agent`**: `{"message": "...", "schema_ddl": "...", "sql_query": "SELECT..."}`
5.  **After `sql_processing_agent` (successful run)**: `{"message": "...", "sql_query": "...", "validation_result": {"status": "success"}, "execution_result": {"status": "success", "result": [...]}}`
6.  **After `sql_processing_agent` (failed run)**: `{"message": "...", "sql_query": "...", "validation_result": {"status": "error", "errors": [...]}}`
7.  **After `sql_corrector_agent` (on failure)**: The `sql_query` key is updated with the corrected query. The loop then repeats.
8.  **After `sql_processing_agent` (on success)**: The `final_sql_query` key is set, and the loop exits. This is the final output.

---

## 📁 Project Structure

```bash
src/texttosql/
├── __init__.py           # Exports root_agent
├── .env                  # Database path + model config
├── agent.py              # Root agent assembly
├── agents.py             # Individual agents (generator, validator, corrector)
├── dialects/             # Database dialect implementations
├── engine.py             # SQL validation and execution logic
├── tools.py              # Tools for agents: extract, validate, execute
tests/                    # Unit and integration tests
pyproject.toml            # Project metadata, deps, linters
```

---

## ⚙️ Setup & Installation

### 1. Prerequisites

- Python 3.10+
- SQLite3 installed (`sqlite3 --version` in terminal)

### 2. Install Dependencies

```bash
uv pip install -e ".[dev]"
```

### 3. Verify Your Setup

```bash
python3 -c "import sqlite3; print('SQLite OK')"
```

---

## 🧪 Testing

Run tests to verify agent flow:

```bash
pytest
```

---

## 👥 Acknowledgments

Built with ❤️ using Google ADK, [SQLGlot](https://github.com/tobymao/sqlglot), and the [Sakila database](https://github.com/bradleygrant/sakila-sqlite3).
