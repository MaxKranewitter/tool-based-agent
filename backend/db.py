from sqlalchemy import create_engine
from langchain_community.utilities import SQLDatabase
import os

def get_engine() -> "Engine":
    from streamlit import secrets
    db_url = secrets.get("SUPABASE_DB_URL") or os.environ["SUPABASE_DB_URL"]
    return create_engine(db_url)

def get_sql_database() -> SQLDatabase:
    engine = get_engine()
    db = SQLDatabase(engine=engine, schema="public", include_tables=["KBBEs"])
    return db