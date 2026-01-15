# BMDB - Minimal Schema Manager

A lightweight SQLAlchemy schema manager with YAML-based model definitions.

## Installation

### From GitHub

```bash
pip install git+https://github.com/BM-Framework/bmdb.git
```

### From source (development)

```bash
git clone https://github.com/BM-Framework/bmdb.git
cd bmdb
pip install -e .
```

## Usage

```bash
# Create a model
bmdb create-model User

# Add fields
bmdb add-fields User name String email String age Int

# Generate models
bmdb generate

# Run migration
bmdb migrate
```

## Requirements

- Python 3.7+
- SQLAlchemy
- PostgreSQL (or other supported database)

## Configuration

Create a `.env` file:

```.env
DB_CONNECTION="postgresql://user:password@localhost:5433/dbname"
```

## License

MIT License

## 5. Add `.gitignore`

```.gitignore
# Python
__pycache__/
*.py[cod]
*$py.class
*.so
.Python
.venv/
venv/
ENV/
env/

# BMDB specific
models.bmdb
.env
bmdb/models/generated/*.py
!bmdb/models/generated/__init__.py

# IDE
.vscode/
.idea/
*.swp
*.swo

# Distribution
dist/
build/
*.egg-info/
```
