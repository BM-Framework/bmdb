"""
Database abstraction layer
"""

from typing import Any, Dict
from .core import BMDB

class Database:
    """Database abstraction with ORM-like features"""
    
    def __init__(self, connection_string: str = "sqlite:///:memory:"):
        self.db = BMDB(connection_string)
        self.tables = {}
    
    def table(self, table_name: str):
        """Get a table object for query building"""
        return Table(self.db, table_name)
    
    def query(self, sql: str, params: tuple = None):
        """Execute raw SQL query"""
        return self.db.execute(sql, params)
    
    def create_model(self, model_name: str, fields: Dict[str, str]):
        """Create a model class dynamically"""
        # Dynamic model creation logic
        pass
    
    def migrate(self, migration_sql: str):
        """Execute migration SQL"""
        self.db.execute(migration_sql)

class Table:
    """Table query builder"""
    
    def __init__(self, db: BMDB, name: str):
        self.db = db
        self.name = name
        self._where = None
        self._order_by = None
        self._limit = None
        self._params = []
    
    def where(self, condition: str, *params):
        """Add WHERE condition"""
        self._where = condition
        self._params.extend(params)
        return self
    
    def order_by(self, column: str, direction: str = "ASC"):
        """Add ORDER BY clause"""
        self._order_by = f"{column} {direction}"
        return self
    
    def limit(self, count: int):
        """Add LIMIT clause"""
        self._limit = count
        return self
    
    def get(self):
        """Execute SELECT query"""
        return self.db.select(
            self.name,
            where=self._where,
            params=tuple(self._params) if self._params else None,
            order_by=self._order_by,
            limit=self._limit
        )
    
    def first(self):
        """Get first matching row"""
        self._limit = 1
        results = self.get()
        return results[0] if results else None
    
    def count(self) -> int:
        """Count matching rows"""
        sql = f"SELECT COUNT(*) FROM {self.name}"
        if self._where:
            sql += f" WHERE {self._where}"
        
        cursor = self.db.execute(sql, tuple(self._params) if self._params else None)
        return cursor.fetchone()[0]
    
    def insert(self, data: Dict[str, Any]):
        """Insert data"""
        return self.db.insert(self.name, data)
    
    def update(self, data: Dict[str, Any]):
        """Update matching rows"""
        if not self._where:
            raise ValueError("WHERE condition required for update")
        return self.db.update(self.name, data, self._where, tuple(self._params))
    
    def delete(self):
        """Delete matching rows"""
        if not self._where:
            raise ValueError("WHERE condition required for delete")
        return self.db.delete(self.name, self._where, tuple(self._params))