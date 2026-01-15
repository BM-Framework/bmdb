"""
ORM Models
"""

from typing import Any, Dict, List, Optional, Type, TypeVar
from dataclasses import dataclass
import json

T = TypeVar('T', bound='Model')

@dataclass
class Field:
    """Field descriptor for models"""
    name: str
    type: str
    primary_key: bool = False
    nullable: bool = True
    default: Any = None
    unique: bool = False
    auto_increment: bool = False
    
    def to_sql(self) -> str:
        """Convert field to SQL definition"""
        parts = [self.name, self.type]
        
        if self.primary_key:
            parts.append("PRIMARY KEY")
        
        if not self.nullable:
            parts.append("NOT NULL")
        
        if self.unique:
            parts.append("UNIQUE")
        
        if self.default is not None:
            if isinstance(self.default, str):
                parts.append(f"DEFAULT '{self.default}'")
            else:
                parts.append(f"DEFAULT {self.default}")
        
        if self.auto_increment:
            parts.append("AUTOINCREMENT")
        
        return " ".join(parts)

class ModelMeta(type):
    """Metaclass for Model"""
    
    def __new__(mcs, name, bases, attrs):
        # Collect fields
        fields = {}
        for key, value in attrs.items():
            if isinstance(value, Field):
                value.name = key
                fields[key] = value
        
        attrs['_fields'] = fields
        attrs['_table_name'] = attrs.get('__tablename__', name.lower() + 's')
        
        return super().__new__(mcs, name, bases, attrs)

class Model(metaclass=ModelMeta):
    """Base Model class"""
    
    _fields: Dict[str, Field]
    _table_name: str
    _db = None
    
    def __init__(self, **kwargs):
        for field_name, field_def in self._fields.items():
            value = kwargs.get(field_name, field_def.default)
            setattr(self, field_name, value)
    
    @classmethod
    def set_database(cls, db):
        """Set database connection for all models"""
        cls._db = db
    
    @classmethod
    def create_table(cls):
        """Create table for this model"""
        if not cls._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        columns = [field.to_sql() for field in cls._fields.values()]
        sql = f"CREATE TABLE IF NOT EXISTS {cls._table_name} ({', '.join(columns)})"
        cls._db.execute(sql)
    
    def save(self):
        """Save model to database"""
        if not self._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        data = {}
        for field_name in self._fields:
            value = getattr(self, field_name, None)
            if value is not None:
                data[field_name] = value
        
        # Check if this is an update or insert
        pk_field = next((f for f in self._fields.values() if f.primary_key), None)
        
        if pk_field and getattr(self, pk_field.name, None) is not None:
            # Update existing record
            self._db.update(
                self._table_name,
                data,
                f"{pk_field.name} = ?",
                (getattr(self, pk_field.name),)
            )
        else:
            # Insert new record
            self._db.insert(self._table_name, data)
            
            # Get auto-incremented ID if applicable
            if pk_field and pk_field.auto_increment:
                cursor = self._db.execute("SELECT last_insert_rowid()")
                setattr(self, pk_field.name, cursor.fetchone()[0])
    
    @classmethod
    def get(cls: Type[T], **kwargs) -> Optional[T]:
        """Get a single record by primary key or other field"""
        if not cls._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        if not kwargs:
            raise ValueError("Query parameters required")
        
        where = " AND ".join([f"{k} = ?" for k in kwargs.keys()])
        params = tuple(kwargs.values())
        
        results = cls._db.select(
            cls._table_name,
            where=where,
            params=params,
            limit=1
        )
        
        if results:
            return cls(**results[0])
        return None
    
    @classmethod
    def all(cls: Type[T]) -> List[T]:
        """Get all records"""
        if not cls._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        results = cls._db.select(cls._table_name)
        return [cls(**row) for row in results]
    
    @classmethod
    def where(cls: Type[T], condition: str, *params) -> List[T]:
        """Get records with condition"""
        if not cls._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        results = cls._db.select(
            cls._table_name,
            where=condition,
            params=params
        )
        return [cls(**row) for row in results]
    
    def delete(self):
        """Delete this record"""
        if not self._db:
            raise ValueError("Database not set. Call Model.set_database() first")
        
        pk_field = next((f for f in self._fields.values() if f.primary_key), None)
        if not pk_field:
            raise ValueError("Model has no primary key field")
        
        pk_value = getattr(self, pk_field.name)
        if pk_value is None:
            raise ValueError("Record has no primary key value")
        
        self._db.delete(
            self._table_name,
            f"{pk_field.name} = ?",
            (pk_value,)
        )
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert model to dictionary"""
        result = {}
        for field_name in self._fields:
            value = getattr(self, field_name, None)
            result[field_name] = value
        return result
    
    def to_json(self) -> str:
        """Convert model to JSON"""
        return json.dumps(self.to_dict(), indent=2)
    
    @classmethod
    def from_dict(cls: Type[T], data: Dict[str, Any]) -> T:
        """Create model from dictionary"""
        return cls(**data)
    
    @classmethod
    def from_json(cls: Type[T], json_str: str) -> T:
        """Create model from JSON"""
        data = json.loads(json_str)
        return cls(**data)