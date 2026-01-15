import inspect
from pathlib import Path
import os
import click
import yaml
from dotenv import load_dotenv
from sqlalchemy.orm import declarative_base

MODELS_FILE = "models.bmdb"
OUT_DIR = Path("bmdb/models/generated")

Base = declarative_base()

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
    generate_models()

@main.command("migrate")
def migrate():
    try:
        # First, let's check if .env can be read properly
        load_dotenv()
        db_url = os.getenv("DB_CONNECTION", "").strip('"')
        if not db_url:
            click.echo("Error: DB_CONNECTION not found in .env")
            return
        
        click.echo(f"DB URL loaded: {db_url[:30]}...")
        
        # IMPORTANT: We need to import from the generated models
        # First, ensure the generated models exist
        models_file = Path("bmdb/models/generated/models.py")
        if not models_file.exists():
            click.echo("Error: Generated models not found. Run 'bmdb generate' first.")
            return
        
        # Clear any cached imports
        import sys
        if 'bmdb.models.generated.models' in sys.modules:
            del sys.modules['bmdb.models.generated.models']
        
        # Add current directory to path to import generated models
        current_dir = Path.cwd()
        if str(current_dir) not in sys.path:
            sys.path.insert(0, str(current_dir))
        
        click.echo("Importing generated models...")
        
        try:
            # Import the Base from generated models
            from bmdb.models.generated.models import Base # type: ignore
        except ImportError as e:
            click.echo(f"Error importing generated models: {e}")
            click.echo("Make sure you ran 'bmdb generate' first")
            return
        
        from sqlalchemy import create_engine

        click.echo("Creating engine...")
        engine = create_engine(db_url, echo=True, client_encoding='utf8')  # echo=True to see SQL
        
        click.echo("Creating tables...")
        
        # Get all table names before creation
        inspector = inspect(engine)
        tables_before = inspector.get_table_names()
        click.echo(f"Tables before: {tables_before}")
        
        # Create tables
        Base.metadata.create_all(engine)
        
        # Get all table names after creation
        tables_after = inspector.get_table_names()
        click.echo(f"Tables after: {tables_after}")
        
        new_tables = set(tables_after) - set(tables_before)
        if new_tables:
            click.echo(f"New tables created: {list(new_tables)}")
        else:
            click.echo("No new tables were created. Check the SQL output above.")
        
        click.echo("Migration done!")
        
    except Exception as e:
        click.echo(f"Migration failed: {e}")
        import traceback
        traceback.print_exc()

if __name__ == "__main__":
    main()