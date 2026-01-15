"""
Example usage of BMDB
"""

from bmdb import BMDB, Database, Model, Field

def example_basic():
    """Basic usage example"""
    print("=== Basic BMDB Example ===")
    
    # Create in-memory database
    db = BMDB("sqlite:///:memory:")
    
    # Create table
    db.create_table("users", {
        "id": "INTEGER PRIMARY KEY AUTOINCREMENT",
        "name": "TEXT NOT NULL",
        "email": "TEXT UNIQUE",
        "age": "INTEGER",
        "created_at": "TIMESTAMP DEFAULT CURRENT_TIMESTAMP"
    })
    
    # Insert data
    db.insert("users", {
        "name": "John Doe",
        "email": "john@example.com",
        "age": 30
    })
    
    db.insert("users", {
        "name": "Jane Smith",
        "email": "jane@example.com",
        "age": 25
    })
    
    # Query data
    users = db.select("users")
    print("All users:")
    for user in users:
        print(f"  {user}")
    
    # Query with condition
    adult_users = db.select("users", where="age >= ?", params=(18,))
    print(f"\nAdult users (age >= 18): {len(adult_users)}")
    
    # Update data
    db.update("users", {"age": 31}, "name = ?", ("John Doe",))
    
    # Delete data
    db.delete("users", "name = ?", ("Jane Smith",))
    
    print("\nFinal users:")
    final_users = db.select("users")
    for user in final_users:
        print(f"  {user}")
    
    db.close()

def example_database():
    """Database abstraction example"""
    print("\n=== Database Abstraction Example ===")
    
    db = Database("sqlite:///example.db")
    
    # Using query builder
    users_table = db.table("users")
    
    # Get all users
    users = users_table.get()
    print(f"Total users: {len(users)}")
    
    # Filter users
    adults = users_table.where("age >= ?", 18).get()
    print(f"Adult users: {len(adults)}")
    
    # Count
    count = users_table.where("email LIKE ?", "%@example.com").count()
    print(f"Users with example.com email: {count}")

def example_models():
    """ORM Models example"""
    print("\n=== ORM Models Example ===")
    
    # Define a model
    class User(Model):
        __tablename__ = "users"
        
        id = Field("INTEGER", primary_key=True, auto_increment=True)
        name = Field("TEXT", nullable=False)
        email = Field("TEXT", unique=True)
        age = Field("INTEGER")
    
    # Set up database
    db = BMDB("sqlite:///models.db")
    Model.set_database(db)
    
    # Create table
    User.create_table()
    
    # Create and save users
    user1 = User(name="Alice", email="alice@example.com", age=28)
    user1.save()
    
    user2 = User(name="Bob", email="bob@example.com", age=35)
    user2.save()
    
    # Query users
    users = User.all()
    print(f"All users: {len(users)}")
    
    # Find user by email
    alice = User.get(email="alice@example.com")
    if alice:
        print(f"Found Alice: {alice.to_dict()}")
    
    # Update user
    if alice:
        alice.age = 29
        alice.save()
        print(f"Updated Alice's age to: {alice.age}")
    
    # Delete user
    bob = User.get(email="bob@example.com")
    if bob:
        bob.delete()
        print("Bob deleted")
    
    db.close()

def example_cli():
    """CLI usage example"""
    print("\n=== CLI Usage Example ===")
    print("""
Available commands:
  bmdb init database.db           # Initialize database
  bmdb shell database.db          # Open interactive shell
  bmdb query database.db "SELECT * FROM users"  # Execute query
  bmdb migration create add_users # Create migration
  bmdb table database.db list     # List tables
    """)

if __name__ == "__main__":
    example_basic()
    example_database()
    example_models()
    example_cli()