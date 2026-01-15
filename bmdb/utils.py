"""
Utility functions and helpers
"""

import json
import logging
import sys
from pathlib import Path
from typing import Any, Dict

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s',
    handlers=[
        logging.StreamHandler(sys.stdout),
        logging.FileHandler('bmdb.log', mode='a')
    ]
)

logger = logging.getLogger('bmdb')

# Configuration management
class Config:
    """Configuration manager"""
    
    def __init__(self):
        self.config_path = Path.home() / ".bmdb_config.json"
        self._config = self._load()
    
    def _load(self) -> Dict[str, Any]:
        """Load configuration from file"""
        if self.config_path.exists():
            try:
                with open(self.config_path, 'r') as f:
                    return json.load(f)
            except json.JSONDecodeError:
                logger.warning("Config file corrupted, creating new one")
                return {}
        return {}
    
    def save(self):
        """Save configuration to file"""
        with open(self.config_path, 'w') as f:
            json.dump(self._config, f, indent=2)
    
    def get(self, key: str, default: Any = None) -> Any:
        """Get configuration value"""
        return self._config.get(key, default)
    
    def set(self, key: str, value: Any):
        """Set configuration value"""
        self._config[key] = value
        self.save()
    
    def delete(self, key: str):
        """Delete configuration value"""
        if key in self._config:
            del self._config[key]
            self.save()
    
    def reset(self):
        """Reset all configuration"""
        self._config = {}
        if self.config_path.exists():
            self.config_path.unlink()

# Global config instance
config = Config()

# Helper functions
def parse_connection_string(connection_string: str) -> Dict[str, str]:
    """
    Parse database connection string
    
    Args:
        connection_string: e.g., "sqlite:///path.db" or "mysql://user:pass@host/db"
        
    Returns:
        Dictionary with parsed components
    """
    if "://" not in connection_string:
        return {"type": "sqlite", "database": connection_string}
    
    protocol, rest = connection_string.split("://", 1)
    
    if protocol == "sqlite":
        return {"type": "sqlite", "database": rest}
    
    # For other protocols (simplified parsing)
    return {"type": protocol, "connection_string": connection_string}

def format_results(results: list, output_format: str = "table") -> str:
    """Format query results"""
    if not results:
        return "No results"
    
    if output_format == "json":
        return json.dumps(results, indent=2)
    
    # Default table format
    headers = list(results[0].keys()) if results else []
    rows = [list(row.values()) for row in results]
    
    # Simple table formatting
    if not headers:
        return str(results)
    
    col_widths = []
    for i, header in enumerate(headers):
        max_width = len(str(header))
        for row in rows:
            max_width = max(max_width, len(str(row[i])))
        col_widths.append(max_width)
    
    format_str = " | ".join([f"{{:<{w}}}" for w in col_widths])
    
    lines = []
    lines.append(format_str.format(*headers))
    lines.append("-+-".join(["-" * w for w in col_widths]))
    
    for row in rows:
        lines.append(format_str.format(*row))
    
    return "\n".join(lines)

def create_migration(name: str, up_sql: str, down_sql: str = ""):
    """Create a migration file"""
    from datetime import datetime
    
    timestamp = datetime.now().strftime("%Y%m%d_%H%M%S")
    filename = f"{timestamp}_{name}.sql"
    
    migrations_dir = Path("migrations")
    migrations_dir.mkdir(exist_ok=True)
    
    migration_file = migrations_dir / filename
    
    content = f"""-- Migration: {name}
-- Created: {datetime.now().isoformat()}

-- Up Migration
{up_sql}

-- Down Migration
{down_sql}
"""
    
    with open(migration_file, 'w') as f:
        f.write(content)
    
    return migration_file

def load_migration(file_path: Path) -> Dict[str, str]:
    """Load migration from file"""
    content = file_path.read_text()
    
    # Simple parsing (can be enhanced)
    sections = content.split("-- ")
    up_sql = ""
    down_sql = ""
    
    for section in sections:
        if "Up Migration" in section:
            up_sql = section.split("Up Migration")[1].strip()
        elif "Down Migration" in section:
            down_sql = section.split("Down Migration")[1].strip()
    
    return {
        "name": file_path.stem,
        "up": up_sql,
        "down": down_sql
    }