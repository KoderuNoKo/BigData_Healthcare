# streamlit/components/db_connection.py
import psycopg2
import pandas as pd
import streamlit as st

from config import DBConfig

@st.cache_resource
def get_connection():
    """Create and cache a PostgreSQL connection."""
    return psycopg2.connect(
        host=DBConfig.host,
        port=DBConfig.port,
        database=DBConfig.database,
        user=DBConfig.user,
        password=DBConfig.password,
    )


def read_table(query: str) -> pd.DataFrame:
    """Execute a SQL query and return a DataFrame."""
    conn = get_connection()
    return pd.read_sql(query, conn)
