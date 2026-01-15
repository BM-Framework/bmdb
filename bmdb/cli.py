#!/usr/bin/env python3
"""
BMDB Command Line Interface
"""

import argparse
import datetime
import sys
from pathlib import Path
from . import __version__
from .core import BMDB
import json

def main():
    """Main CLI entry point"""
    parser = argparse.ArgumentParser(
        description="BM Database Framework CLI",
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
Examples:
  bmdb init mydatabase.db
  bmdb shell sqlite:///test.db
  bmdb query sqlite:///test.db "SELECT * FROM users"
  bmdb migration create add_users_table
        """
    )
    
    parser.add_argument(
        '--version', 
        action='store_true',
        help='Show version information'
    )
    
    subparsers = parser.add_subparsers(
        dest='command',
        title='commands',
        description='Available commands',
        metavar='COMMAND'
    )
    
    # Init command
    init_parser = subparsers.add_parser(
        'init',
        help='Initialize a new database'
    )
    init_parser.add_argument(
        'database',
        help='Database connection string or filename'
    )
    init_parser.add_argument(
        '--driver',
        choices=['sqlite', 'mysql', 'postgresql'],
        default='sqlite',
        help='Database driver (default: sqlite)'
    )
    
    # Shell command
    shell_parser = subparsers.add_parser(
        'shell',
        help='Open database interactive shell'
    )
    shell_parser.add_argument(
        'database',
        help='Database connection string or filename'
    )
    
    # Query command
    query_parser = subparsers.add_parser(
        'query',
        help='Execute SQL query'
    )
    query_parser.add_argument(
        'database',
        help='Database connection string or filename'
    )
    query_parser.add_argument(
        'sql',
        help='SQL query to execute'
    )
    query_parser.add_argument(
        '--output',
        choices=['table', 'json', 'csv'],
        default='table',
        help='Output format (default: table)'
    )
    
    # Migration commands
    migration_parser = subparsers.add_parser(
        'migration',
        help='Database migration tools'
    )
    migration_subparsers = migration_parser.add_subparsers(
        dest='migration_command',
        title='migration commands'
    )
    
    # Migration create
    migrate_create = migration_subparsers.add_parser(
        'create',
        help='Create a new migration'
    )
    migrate_create.add_argument(
        'name',
        help='Migration name'
    )
    
    # Migration run
    migrate_run = migration_subparsers.add_parser(
        'run',
        help='Run pending migrations'
    )
    migrate_run.add_argument(
        'database',
        help='Database connection string or filename'
    )
    
    # Migration list
    migration_subparsers.add_parser(
        'list',
        help='List all migrations'
    )
    
    # Config command
    config_parser = subparsers.add_parser(
        'config',
        help='Manage configuration'
    )
    config_parser.add_argument(
        'action',
        choices=['show', 'set', 'reset'],
        help='Configuration action'
    )
    config_parser.add_argument(
        'key',
        nargs='?',
        help='Configuration key'
    )
    config_parser.add_argument(
        'value',
        nargs='?',
        help='Configuration value'
    )
    
    # Table command
    table_parser = subparsers.add_parser(
        'table',
        help='Table operations'
    )
    table_parser.add_argument(
        'database',
        help='Database connection string or filename'
    )
    table_parser.add_argument(
        'action',
        choices=['list', 'create', 'drop', 'describe'],
        help='Table action'
    )
    table_parser.add_argument(
        'name',
        nargs='?',
        help='Table name'
    )
    table_parser.add_argument(
        '--schema',
        help='Table schema (JSON string or @filename.json)'
    )
    
    args = parser.parse_args()
    
    # Handle version flag
    if args.version:
        print(f"BMDB Version {__version__}")
        return 0
    
    # Handle no command
    if not args.command:
        parser.print_help()
        return 0
    
    try:
        # Execute command
        if args.command == 'init':
            return handle_init(args)
        elif args.command == 'shell':
            return handle_shell(args)
        elif args.command == 'query':
            return handle_query(args)
        elif args.command == 'migration':
            return handle_migration(args)
        elif args.command == 'config':
            return handle_config(args)
        elif args.command == 'table':
            return handle_table(args)
        else:
            print(f"Unknown command: {args.command}")
            return 1
            
    except Exception as e:
        print(f"Error: {e}", file=sys.stderr)
        return 1

def handle_init(args):
    """Handle init command"""
    print(f"Initializing database: {args.database}")
    
    # Create SQLite database file if needed
    if args.driver == 'sqlite' and not args.database.startswith('sqlite://'):
        db_path = Path(args.database)
        if not db_path.suffix:
            db_path = db_path.with_suffix('.db')
        
        # Create directory if it doesn't exist
        db_path.parent.mkdir(parents=True, exist_ok=True)
        
        connection_string = f"sqlite:///{db_path}"
    else:
        connection_string = args.database
    
    try:
        # Initialize database
        db = BMDB(connection_string)
        
        # Create migrations table
        db.execute("""
            CREATE TABLE IF NOT EXISTS migrations (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                name TEXT UNIQUE NOT NULL,
                applied_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP
            )
        """)
        
        print(f"Database initialized successfully at: {connection_string}")
        return 0
    except Exception as e:
        print(f"Failed to initialize database: {e}", file=sys.stderr)
        return 1

def handle_shell(args):
    """Handle shell command"""
    print(f"Opening shell for: {args.database}")
    print("Type 'exit' to quit, 'help' for help")
    
    try:
        db = BMDB(args.database)
        
        while True:
            try:
                command = input("bmdb> ").strip()
                
                if command.lower() in ['exit', 'quit', 'q']:
                    break
                elif command.lower() in ['help', '?']:
                    print_help()
                elif command:
                    try:
                        result = db.execute(command)
                        if result:
                            # Try to fetch results
                            try:
                                rows = result.fetchall()
                                if rows:
                                    # Print as table
                                    headers = [desc[0] for desc in result.description]
                                    print(format_as_table(headers, rows))
                                else:
                                    print("Query executed successfully (no rows returned)")
                            except:  # noqa: E722
                                print("Query executed successfully")
                    except Exception as e:
                        print(f"Error: {e}")
                        
            except KeyboardInterrupt:
                print("\nUse 'exit' to quit")
            except EOFError:
                break
                
    except Exception as e:
        print(f"Failed to open shell: {e}", file=sys.stderr)
        return 1
    
    return 0

def handle_query(args):
    """Handle query command"""
    try:
        db = BMDB(args.database)
        result = db.execute(args.sql)
        
        # Try to fetch results
        try:
            rows = result.fetchall()
            headers = [desc[0] for desc in result.description]
            
            if args.output == 'json':
                data = []
                for row in rows:
                    data.append(dict(zip(headers, row)))
                print(json.dumps(data, indent=2))
            elif args.output == 'csv':
                import csv
                import sys
                writer = csv.writer(sys.stdout)
                writer.writerow(headers)
                writer.writerows(rows)
            else:  # table
                print(format_as_table(headers, rows))
                
        except:  # noqa: E722
            print("Query executed successfully")
            
        return 0
    except Exception as e:
        print(f"Query failed: {e}", file=sys.stderr)
        return 1

def handle_migration(args):
    """Handle migration commands"""
    if args.migration_command == 'create':
        name = args.name.strip().replace(' ', '_').lower()
        timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
        filename = f"{timestamp}_{name}.sql"
        
        migrations_dir = Path("migrations")
        migrations_dir.mkdir(exist_ok=True)
        
        migration_file = migrations_dir / filename
        
        with open(migration_file, 'w') as f:
            f.write(f"-- Migration: {args.name}\n")
            f.write(f"-- Created: {datetime.now().isoformat()}\n\n")
            f.write("-- Up migration\n")
            f.write("-- Add your SQL statements here\n\n")
            f.write("-- Down migration\n")
            f.write("-- Add rollback SQL statements here\n")
        
        print(f"Migration created: {migration_file}")
        return 0
        
    elif args.migration_command == 'list':
        migrations_dir = Path("migrations")
        if migrations_dir.exists():
            migrations = sorted(migrations_dir.glob("*.sql"))
            if migrations:
                print("Available migrations:")
                for migration in migrations:
                    print(f"  {migration.name}")
            else:
                print("No migrations found")
        else:
            print("Migrations directory does not exist")
        return 0
        
    elif args.migration_command == 'run':
        print(f"Running migrations for: {args.database}")
        # Implementation for running migrations
        print("Migration run command not fully implemented yet")
        return 0
        
    else:
        print(f"Unknown migration command: {args.migration_command}")
        return 1

def handle_config(args):
    """Handle config commands"""
    config_file = Path.home() / ".bmdb_config.json"
    
    if args.action == 'show':
        if config_file.exists():
            with open(config_file, 'r') as f:
                config_data = json.load(f)
                print(json.dumps(config_data, indent=2))
        else:
            print("No configuration found")
            
    elif args.action == 'set' and args.key and args.value:
        if config_file.exists():
            with open(config_file, 'r') as f:
                config_data = json.load(f)
        else:
            config_data = {}
        
        config_data[args.key] = args.value
        
        with open(config_file, 'w') as f:
            json.dump(config_data, f, indent=2)
        
        print(f"Set {args.key} = {args.value}")
        
    elif args.action == 'reset':
        if config_file.exists():
            config_file.unlink()
            print("Configuration reset")
        else:
            print("No configuration found")
            
    else:
        print("Invalid config command")
        return 1
        
    return 0

def handle_table(args):
    """Handle table commands"""
    try:
        db = BMDB(args.database)
        
        if args.action == 'list':
            tables = db.get_tables()
            if tables:
                print("Tables:")
                for table in tables:
                    print(f"  {table}")
            else:
                print("No tables found")
                
        elif args.action == 'create' and args.name:
            if not args.schema:
                print("Error: Schema required for table creation")
                return 1
            
            # Parse schema
            if args.schema.startswith('@'):
                schema_file = Path(args.schema[1:])
                if not schema_file.exists():
                    print(f"Error: Schema file not found: {schema_file}")
                    return 1
                with open(schema_file, 'r') as f:
                    schema = json.load(f)
            else:
                schema = json.loads(args.schema)
            
            # Create table
            db.create_table(args.name, schema)
            print(f"Table '{args.name}' created successfully")
            
        elif args.action == 'drop' and args.name:
            confirm = input(f"Are you sure you want to drop table '{args.name}'? (yes/no): ")
            if confirm.lower() == 'yes':
                db.drop_table(args.name)
                print(f"Table '{args.name}' dropped successfully")
            else:
                print("Operation cancelled")
                
        elif args.action == 'describe' and args.name:
            columns = db.get_table_columns(args.name)
            if columns:
                print(f"Table: {args.name}")
                print(format_as_table(["Column", "Type", "Nullable", "Default"], columns))
            else:
                print(f"Table '{args.name}' not found or has no columns")
                
        else:
            print("Invalid table command")
            return 1
            
        return 0
    except Exception as e:
        print(f"Table operation failed: {e}", file=sys.stderr)
        return 1

def print_help():
    """Print shell help"""
    print("""
Shell Commands:
  SQL queries     - Execute any SQL statement
  exit/quit/q     - Exit the shell
  help/?          - Show this help
  
Examples:
  SELECT * FROM users;
  CREATE TABLE test (id INTEGER, name TEXT);
  INSERT INTO users (name) VALUES ('John');
    """)

def format_as_table(headers, rows):
    """Format data as a table"""
    if not rows:
        return "No data"
    
    # Calculate column widths
    col_widths = []
    for i, header in enumerate(headers):
        max_width = len(str(header))
        for row in rows:
            cell = str(row[i] if i < len(row) else "")
            max_width = max(max_width, len(cell))
        col_widths.append(max_width)
    
    # Create format string
    format_str = " | ".join([f"{{:<{w}}}" for w in col_widths])
    
    # Build table
    lines = []
    lines.append(format_str.format(*headers))
    lines.append("-+-".join(["-" * w for w in col_widths]))
    
    for row in rows:
        lines.append(format_str.format(*row))
    
    return "\n".join(lines)

if __name__ == "__main__":
    sys.exit(main())