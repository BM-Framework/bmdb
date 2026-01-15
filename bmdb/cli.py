from pathlib import Path
import os
import sys
import click
import yaml
from dotenv import load_dotenv
from sqlalchemy import create_engine, inspect

MODELS_FILE = Path.cwd() / "bmdb" / "models" / "models.bmdb"
OUT_DIR = Path.cwd() / "bmdb" / "models" / "generated"

@click.group()
def main():
    """BMDB - minimal schema manager"""
    pass

def load_models():
    if not Path(MODELS_FILE).exists():
        return {"models": {}}
    with open(MODELS_FILE, "r", encoding="utf-8") as f:
        return yaml.safe_load(f) or {"models": {}}

def save_models(data):
    with open(MODELS_FILE, "w", encoding="utf-8") as f:
        yaml.safe_dump(data, f, sort_keys=False, allow_unicode=True)

def generate_models():
    load_dotenv()
    db_url = os.getenv("DB_CONNECTION")
    if not db_url:
        print("Error: DB_CONNECTION missing in .env")
        return

    data = load_models()
    if not data.get("models"):
        print("No models found")
        return

    OUT_DIR.mkdir(parents=True, exist_ok=True)

    # Generate models with CRUD methods
    code = [
        "# -*- coding: utf-8 -*-",
        "from sqlalchemy import Column, Integer, String, Text, Float, Boolean, Date, DateTime, ForeignKey, create_engine",
        "from sqlalchemy.orm import declarative_base, sessionmaker, Session",
        "from sqlalchemy.orm import relationship",
        "import os",
        "from dotenv import load_dotenv",
        "",
        "Base = declarative_base()",
        "",
        "# Load DB connection from .env at runtime",
        "load_dotenv()",
        "DB_URL = os.getenv('DB_CONNECTION', '').strip('\"')",
        "engine = create_engine(DB_URL, echo=False) if DB_URL else None",
        "SessionLocal = sessionmaker(bind=engine) if engine else None",
        "",
        "class ModelMixin:",
        "    '''Mixin to add CRUD methods to models'''",
        "    ",
        "    def save(self):",
        "        '''Create or update this instance'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            session.add(self)",
        "            session.commit()",
        "            session.refresh(self)",
        "            return self",
        "        except Exception as e:",
        "            session.rollback()",
        "            raise e",
        "        finally:",
        "            session.close()",
        "    ",
        "    def delete(self):",
        "        '''Delete this instance'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            session.delete(self)",
        "            session.commit()",
        "            return True",
        "        except Exception as e:",
        "            session.rollback()",
        "            raise e",
        "        finally:",
        "            session.close()",
        "    ",
        "    @classmethod",
        "    def get(cls, id):",
        "        '''Get record by ID'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            return session.query(cls).filter(cls.id == id).first()",
        "        finally:",
        "            session.close()",
        "    ",
        "    @classmethod",
        "    def all(cls):",
        "        '''Get all records'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            return session.query(cls).all()",
        "        finally:",
        "            session.close()",
        "    ",
        "    @classmethod",
        "    def filter(cls, **kwargs):",
        "        '''Filter records by field values'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            query = session.query(cls)",
        "            for key, value in kwargs.items():",
        "                if hasattr(cls, key):",
        "                    query = query.filter(getattr(cls, key) == value)",
        "            return query.all()",
        "        finally:",
        "            session.close()",
        "    ",
        "    @classmethod",
        "    def first(cls, **kwargs):",
        "        '''Get first record matching filters'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            query = session.query(cls)",
        "            for key, value in kwargs.items():",
        "                if hasattr(cls, key):",
        "                    query = query.filter(getattr(cls, key) == value)",
        "            return query.first()",
        "        finally:",
        "            session.close()",
        "    ",
        "    @classmethod",
        "    def count(cls, **kwargs):",
        "        '''Count records matching filters'''",
        "        if not SessionLocal:",
        "            raise RuntimeError('Database not configured')",
        "        session = SessionLocal()",
        "        try:",
        "            query = session.query(cls)",
        "            for key, value in kwargs.items():",
        "                if hasattr(cls, key):",
        "                    query = query.filter(getattr(cls, key) == value)",
        "            return query.count()",
        "        finally:",
        "            session.close()",
        "    ",
        "    def to_dict(self):",
        "        '''Convert model instance to dictionary'''",
        "        result = {}",
        "        for column in self.__table__.columns:",
        "            result[column.name] = getattr(self, column.name)",
        "        return result",
        "",
    ]

    type_map = {
        "String": "String",
        "Text": "Text",
        "Int": "Integer",
        "Integer": "Integer",
        "Float": "Float",
        "Boolean": "Boolean",
        "Date": "Date",
        "DateTime": "DateTime",
        "JSON": "JSON"
    }

    # Generate model classes
    for m_name, m_data in data["models"].items():
        code.append(f"class {m_name}(Base, ModelMixin):")
        code.append(f'    __tablename__ = "{m_name.lower()}s"')
        code.append("    id = Column(Integer, primary_key=True, autoincrement=True)")
        for f_name, f_type in m_data.get("fields", {}).items():
            base_type = f_type.split()[0].strip()
            col_type = type_map.get(base_type, "String")
            code.append(f"    {f_name} = Column({col_type})  # {f_type}")
        code.append("")

    # Write models.py
    (OUT_DIR / "models.py").write_text("\n".join(code), encoding="utf-8")
    (OUT_DIR / "__init__.py").write_text("", encoding="utf-8")

    # Write migrate.py
    migrate_code = [
        "# -*- coding: utf-8 -*-",
        "# migrate.py - run manually or use bmdb migrate",
        "from models import Base, engine",
        "",
        "if engine:",
        "    Base.metadata.create_all(engine)",
        "    print('Tables created')",
        "else:",
        "    print('Error: DB_CONNECTION not set')"
    ]
    (OUT_DIR / "migrate.py").write_text("\n".join(migrate_code), encoding="utf-8")

    click.echo("Generated -> bmdb/models/generated/")

@main.command("create-model")
@click.argument("name")
def create_model(name):
    """Create a new model"""
    data = load_models()
    if name in data["models"]:
        click.echo(f"Model {name} already exists")
        return
    data["models"][name] = {"fields": {}}
    save_models(data)
    click.echo(f"Model {name} created")

@main.command("add-fields")
@click.argument("model")
@click.argument("fields", nargs=-1)
@click.option("--unique", multiple=True)
def add_fields(model, fields, unique):
    """Add fields to a model"""
    if len(fields) % 2 != 0:
        click.echo("Fields must come in name-type pairs")
        return
    data = load_models()
    if model not in data["models"]:
        click.echo(f"Model {model} not found")
        return
    uniques = set(unique)
    for i in range(0, len(fields), 2):
        fname = fields[i]
        ftype = fields[i+1]
        defn = ftype + (" @unique" if fname in uniques else "")
        data["models"][model]["fields"][fname] = defn
    save_models(data)
    click.echo(f"Fields added to {model}")

@main.command("generate")
def generate():
    """Generate Python models from models.bmdb"""
    generate_models()

@main.command("migrate")
def migrate():
    """Create database tables from generated models"""
    try:
        load_dotenv()
        db_url = os.getenv("DB_CONNECTION", "").strip('"')
        if not db_url:
            click.echo("Error: DB_CONNECTION not found in .env")
            return
        
        click.echo(f"DB URL loaded: {db_url[:30]}...")
        
        # Look for models in current directory
        models_path = Path.cwd() / "bmdb" / "models" / "generated" / "models.py"
        if not models_path.exists():
            click.echo("Error: Generated models not found in current directory.")
            click.echo(f"Looked for: {models_path}")
            click.echo("Make sure you're in the project root and ran 'bmdb generate'")
            return
        
        # Clear sys.modules cache
        for module in list(sys.modules.keys()):
            if module.startswith('bmdb.models'):
                del sys.modules[module]
        
        # Add current directory to Python path
        sys.path.insert(0, str(Path.cwd()))
        
        click.echo("Importing models from current directory...")
        
        # Dynamic import to avoid cache issues
        import importlib.util
        spec = importlib.util.spec_from_file_location(
            "generated_models", 
            str(models_path)
        )
        module = importlib.util.module_from_spec(spec)
        spec.loader.exec_module(module)
        
        Base = module.Base
        click.echo(f"Successfully imported Base with {len(Base.metadata.tables)} tables")
        
        # Show what tables will be created
        for table_name in Base.metadata.tables.keys():
            click.echo(f"  - {table_name}")
        
        click.echo("Creating engine...")
        engine = create_engine(db_url, echo=True)  # echo=True to see SQL
        
        # Check existing tables
        inspector = inspect(engine)
        existing_tables = inspector.get_table_names()
        click.echo(f"Existing tables: {existing_tables}")
        
        click.echo("Creating tables...")
        Base.metadata.create_all(engine)
        
        # Verify
        new_tables = inspector.get_table_names()
        click.echo(f"All tables now: {new_tables}")
        
        # Show newly created tables
        created = set(new_tables) - set(existing_tables)
        if created:
            click.echo(f"New tables created: {list(created)}")
        else:
            click.echo("Note: No new tables were created (they might already exist)")
        
        click.echo("Migration done!")
        
    except Exception as e:
        click.echo(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()

@main.command("list-models")
def list_models():
    """List all defined models"""
    data = load_models()
    if not data.get("models"):
        click.echo("No models defined")
        return
    
    click.echo("Defined models:")
    for model_name, model_data in data["models"].items():
        click.echo(f"\n{model_name}:")
        for field_name, field_type in model_data.get("fields", {}).items():
            click.echo(f"  - {field_name}: {field_type}")

@main.command("init")
def init():
    """Initialize a new BMDB project"""
    if Path(MODELS_FILE).exists():
        click.echo(f"{MODELS_FILE} already exists")
    else:
        save_models({"models": {}})
        click.echo(f"Created {MODELS_FILE}")
    
    # Create .env example if not exists
    if not Path(".env").exists():
        with open(".env.example", "w") as f:
            f.write("# Database connection\n")
            f.write("DB_CONNECTION=postgresql://user:password@localhost:5432/dbname\n")
        click.echo("Created .env.example")
    
    click.echo("\nNext steps:")
    click.echo("1. Copy .env.example to .env and edit with your database details")
    click.echo("2. Create a model: bmdb create-model ModelName")
    click.echo("3. Add fields: bmdb add-fields ModelName field1 String field2 Integer")
    click.echo("4. Generate models: bmdb generate")
    click.echo("5. Migrate database: bmdb migrate")

if __name__ == "__main__":
    main()