import os

# Centralized configuration for the agent
MODEL_NAME = os.getenv("MODEL_NAME", "gemini-3.7-flash")

# Database configuration
DB_DIALECT = os.getenv("DB_DIALECT", "sqlite").lower()
DB_URI = os.getenv("DB_URI", "src/texttosql/sakila_master.db")
