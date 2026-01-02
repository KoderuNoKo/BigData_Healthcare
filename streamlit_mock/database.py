"""
database.py
Database connection module for PostgreSQL with connection pooling
"""

import os
import streamlit as st
import psycopg2
from psycopg2 import pool, extras
from sqlalchemy import create_engine
from contextlib import contextmanager
import logging
from typing import Optional, Dict, Any, List
import pandas as pd

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class DatabaseConfig:
    """Database configuration class"""
    
    def __init__(self):
        """Initialize database configuration from environment or Streamlit secrets"""
        # if "postgresql" in st.secrets:
        #     self.config = dict(st.secrets["postgresql"])
        # else:
        self.config = {
            "host": os.getenv("PG_HOST", "localhost"),
            "port": os.getenv("PG_PORT", "5432"),
            "database": os.getenv("PG_DATABASE", "mimic_dw"),
            "user": os.getenv("PG_USER", "dev"),
            "password": os.getenv("PG_PASSWORD", "devpassword")
        }
        
        # Connection pool settings
        self.min_connections = int(os.getenv("PG_MIN_CONNECTIONS", "1"))
        self.max_connections = int(os.getenv("PG_MAX_CONNECTIONS", "20"))
        
    @property
    def connection_string(self) -> str:
        """Get PostgreSQL connection string"""
        return (
            f"postgresql://{self.config['user']}:{self.config['password']}@"
            f"{self.config['host']}:{self.config['port']}/{self.config['database']}"
        )
    
    @property
    def connection_params(self) -> Dict[str, Any]:
        """Get connection parameters as dictionary"""
        return self.config


class DatabaseConnection:
    """PostgreSQL database connection manager with pooling"""
    
    _instance = None
    _connection_pool = None
    _engine = None
    
    def __new__(cls):
        """Singleton pattern for database connection"""
        if cls._instance is None:
            cls._instance = super(DatabaseConnection, cls).__new__(cls)
        return cls._instance
    
    def __init__(self):
        """Initialize database connection"""
        if not hasattr(self, 'initialized'):
            self.config = DatabaseConfig()
            self.initialized = True
    
    @property
    def connection_pool(self) -> pool.SimpleConnectionPool:
        """Get or create connection pool"""
        if self._connection_pool is None:
            try:
                self._connection_pool = psycopg2.pool.SimpleConnectionPool(
                    self.config.min_connections,
                    self.config.max_connections,
                    **self.config.connection_params
                )
                logger.info("Connection pool created successfully")
            except Exception as e:
                logger.error(f"Failed to create connection pool: {e}")
                raise
        return self._connection_pool
    
    @property
    def engine(self) -> create_engine:
        """Get or create SQLAlchemy engine"""
        if self._engine is None:
            try:
                self._engine = create_engine(
                    self.config.connection_string,
                    pool_size=10,
                    max_overflow=20,
                    pool_pre_ping=True,  # Verify connections before using
                    pool_recycle=3600,   # Recycle connections after 1 hour
                )
                logger.info("SQLAlchemy engine created successfully")
            except Exception as e:
                logger.error(f"Failed to create SQLAlchemy engine: {e}")
                raise
        return self._engine
    
    @contextmanager
    def get_connection(self):
        """Context manager for getting a connection from the pool"""
        connection = None
        try:
            connection = self.connection_pool.getconn()
            yield connection
            connection.commit()
        except Exception as e:
            if connection:
                connection.rollback()
            logger.error(f"Database error: {e}")
            raise
        finally:
            if connection:
                self.connection_pool.putconn(connection)
    
    @contextmanager
    def get_cursor(self, cursor_factory=None):
        """Context manager for getting a cursor"""
        with self.get_connection() as conn:
            cursor = conn.cursor(cursor_factory=cursor_factory)
            try:
                yield cursor
            finally:
                cursor.close()
    
    def execute_query(self, query: str, params: tuple = None) -> List[tuple]:
        """Execute a SELECT query and return results"""
        with self.get_cursor(cursor_factory=extras.RealDictCursor) as cursor:
            cursor.execute(query, params)
            return cursor.fetchall()
    
    def execute_update(self, query: str, params: tuple = None) -> int:
        """Execute an INSERT/UPDATE/DELETE query and return affected rows"""
        with self.get_cursor() as cursor:
            cursor.execute(query, params)
            return cursor.rowcount
    
    def execute_many(self, query: str, params_list: List[tuple]) -> int:
        """Execute multiple INSERT/UPDATE queries"""
        with self.get_cursor() as cursor:
            cursor.executemany(query, params_list)
            return cursor.rowcount
    
    def fetch_dataframe(self, query: str, params: tuple = None) -> pd.DataFrame:
        """Execute query and return results as pandas DataFrame"""
        try:
            with self.get_connection() as conn:
                return pd.read_sql_query(query, conn, params=params)
        except Exception as e:
            logger.error(f"Failed to fetch dataframe: {e}")
            return pd.DataFrame()
    
    def fetch_dataframe_with_cache(self, query: str, params: tuple = None, ttl: int = 300) -> pd.DataFrame:
        """Execute query with caching (for Streamlit)"""
        @st.cache_data(ttl=ttl)
        def _fetch(q, p):
            return self.fetch_dataframe(q, p)
        return _fetch(query, params)
    
    def test_connection(self) -> bool:
        """Test database connection"""
        try:
            with self.get_connection() as conn:
                cursor = conn.cursor()
                cursor.execute("SELECT 1")
                result = cursor.fetchone()
                cursor.close()
                return result[0] == 1
        except Exception as e:
            logger.error(f"Connection test failed: {e}")
            return False
    
    def close_all_connections(self):
        """Close all connections in the pool"""
        if self._connection_pool:
            self._connection_pool.closeall()
            logger.info("All connections closed")
    
    def __del__(self):
        """Clean up connections on deletion"""
        self.close_all_connections()


# Singleton instance
@st.cache_resource
def get_database_connection() -> DatabaseConnection:
    """Get or create database connection instance"""
    return DatabaseConnection()


# Utility functions for common operations
def run_query(query: str, params: tuple = None) -> pd.DataFrame:
    """Quick function to run a query and get DataFrame"""
    db = get_database_connection()
    return db.fetch_dataframe(query, params)


def run_cached_query(query: str, params: tuple = None, ttl: int = 300) -> pd.DataFrame:
    """Run query with caching"""
    db = get_database_connection()
    return db.fetch_dataframe_with_cache(query, params, ttl)


def execute_update(query: str, params: tuple = None) -> int:
    """Execute an update query"""
    db = get_database_connection()
    return db.execute_update(query, params)


def test_connection() -> bool:
    """Test database connection"""
    db = get_database_connection()
    return db.test_connection()


# Transaction helper
@contextmanager
def transaction():
    """Context manager for database transactions"""
    db = get_database_connection()
    with db.get_connection() as conn:
        try:
            yield conn
            conn.commit()
        except Exception as e:
            conn.rollback()
            logger.error(f"Transaction rolled back: {e}")
            raise


# Batch operations helper
def batch_insert(table: str, columns: List[str], data: List[tuple], batch_size: int = 1000):
    """Batch insert data into a table"""
    db = get_database_connection()
    
    placeholders = ', '.join(['%s'] * len(columns))
    columns_str = ', '.join(columns)
    query = f"INSERT INTO {table} ({columns_str}) VALUES ({placeholders})"
    
    total_inserted = 0
    for i in range(0, len(data), batch_size):
        batch = data[i:i + batch_size]
        total_inserted += db.execute_many(query, batch)
    
    return total_inserted


# Database health check
def get_database_stats() -> Dict[str, Any]:
    """Get database statistics and health information"""
    db = get_database_connection()
    
    stats = {}
    
    # Database version
    version = db.execute_query("SELECT version()")[0]
    stats['version'] = version['version']
    
    # Database size
    size_query = """
        SELECT pg_database.datname,
               pg_size_pretty(pg_database_size(pg_database.datname)) AS size
        FROM pg_database
        WHERE datname = current_database()
    """
    size_result = db.execute_query(size_query)[0]
    stats['database_size'] = size_result['size']
    
    # Connection count
    conn_query = """
        SELECT count(*) as connections
        FROM pg_stat_activity
        WHERE datname = current_database()
    """
    conn_result = db.execute_query(conn_query)[0]
    stats['active_connections'] = conn_result['connections']
    
    # Table count
    table_query = """
        SELECT count(*) as tables
        FROM information_schema.tables
        WHERE table_schema = 'public'
    """
    table_result = db.execute_query(table_query)[0]
    stats['table_count'] = table_result['tables']
    
    return stats