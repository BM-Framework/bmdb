"""
Core BMDB class
"""

import sqlite3
from pathlib import Path
from typing import Any, Dict, List
from .utils import logger

class BMDB:
    """Main BMDB class"""
    
    def __init__(self, connection_string: str = "sqlite:///:memory:"):
        """
        Initialize BMDB connection
        
        Args:
            connection_string: Database connection string
                sqlite:///path/to/database.db
                sqlite:///:memory: (in-memory database)
        """
        self.connection_string = connection_string
        self.connection = None
        self.connect()
        
    def connect(self):
        """Establish database connection"""
        if self.connection_string.startswith("sqlite:///"):
            db_path = self.connection_string.replace("sqlite:///", "")
            if db_path != ":memory:":
                Path(db_path).parent.mkdir(parents=True, exist_ok=True)
            
            self.connection = sqlite3.connect(db_path)
            self.connection.row_factory = sqlite3.Row
            logger.info(f"Connected to SQLite database: {db_path}")
        else:
            # Add support for other databases here
            raise ValueError(f"Unsupported database: {self.connection_string}")
    
    def execute(self, sql: str, params: tuple = None):
        """
        Execute SQL statement
        
        Args:
            sql: SQL statement
            params: Parameters for the SQL statement
            
        Returns:
            Cursor object
        """
        cursor = self.connection.cursor()
        try:
            if params:
                cursor.execute(sql, params)
            else:
                cursor.execute(sql)
            self.connection.commit()
            logger.debug(f"Executed SQL: {sql}")
            return cursor
        except Exception as e:
            self.connection.rollback()
            logger.error(f"SQL execution failed: {sql} - {e}")
            raise
    
    def create_table(self, table_name: str, schema: Dict[str, str]):
        """
        Create a new table
        
        Args:
            table_name: Name of the table
            schema: Dictionary of column_name: column_type
        """
        columns = []
        for col_name, col_type in schema.items():
            columns.append(f"{col_name} {col_type}")
        
        sql = f"CREATE TABLE IF NOT EXISTS {table_name} ({', '.join(columns)})"
        self.execute(sql)
        logger.info(f"Created table: {table_name}")
    
    def drop_table(self, table_name: str):
        """Drop a table"""
        sql = f"DROP TABLE IF EXISTS {table_name}"
        self.execute(sql)
        logger.info(f"Dropped table: {table_name}")
    
    def insert(self, table_name: str, data: Dict[str, Any]):
        """
        Insert data into table
        
        Args:
            table_name: Name of the table
            data: Dictionary of column_name: value
        """
        columns = list(data.keys())
        placeholders = ["?" for _ in columns]
        values = list(data.values())
        
        sql = f"INSERT INTO {table_name} ({', '.join(columns)}) VALUES ({', '.join(placeholders)})"
        self.execute(sql, tuple(values))
        logger.debug(f"Inserted into {table_name}: {data}")
    
    def select(self, table_name: str, columns: List[str] = None, 
               where: str = None, params: tuple = None, 
               limit: int = None, order_by: str = None):
        """
        Select data from table
        
        Args:
            table_name: Name of the table
            columns: List of columns to select (None for all)
            where: WHERE clause
            params: Parameters for WHERE clause
            limit: Maximum number of rows
            order_by: ORDER BY clause
            
        Returns:
            List of rows as dictionaries
        """
        if columns:
            cols = ", ".join(columns)
        else:
            cols = "*"
        
        sql = f"SELECT {cols} FROM {table_name}"
        
        if where:
            sql += f" WHERE {where}"
        
        if order_by:
            sql += f" ORDER BY {order_by}"
        
        if limit:
            sql += f" LIMIT {limit}"
        
        cursor = self.execute(sql, params)
        rows = cursor.fetchall()
        
        # Convert to list of dictionaries
        result = []
        for row in rows:
            result.append(dict(row))
        
        return result
    
    def update(self, table_name: str, data: Dict[str, Any], where: str, params: tuple = None):
        """
        Update data in table
        
        Args:
            table_name: Name of the table
            data: Dictionary of column_name: new_value
            where: WHERE clause
            params: Parameters for WHERE clause
        """
        set_clause = ", ".join([f"{k} = ?" for k in data.keys()])
        values = list(data.values())
        
        if params:
            values.extend(params)
        
        sql = f"UPDATE {table_name} SET {set_clause} WHERE {where}"
        self.execute(sql, tuple(values))
        logger.debug(f"Updated {table_name}: {data}")
    
    def delete(self, table_name: str, where: str, params: tuple = None):
        """
        Delete data from table
        
        Args:
            table_name: Name of the table
            where: WHERE clause
            params: Parameters for WHERE clause
        """
        sql = f"DELETE FROM {table_name} WHERE {where}"
        self.execute(sql, params)
        logger.debug(f"Deleted from {table_name}")
    
    def get_tables(self) -> List[str]:
        """Get list of all tables in database"""
        cursor = self.execute("SELECT name FROM sqlite_master WHERE type='table'")
        tables = [row[0] for row in cursor.fetchall()]
        return tables
    
    def get_table_columns(self, table_name: str) -> List[tuple]:
        """
        Get column information for a table
        
        Returns:
            List of (column_name, data_type, nullable, default_value)
        """
        cursor = self.execute(f"PRAGMA table_info({table_name})")
        columns = cursor.fetchall()
        return columns
    
    def close(self):
        """Close database connection"""
        if self.connection:
            self.connection.close()
            logger.info("Database connection closed")
    
    def __enter__(self):
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
        self.close()