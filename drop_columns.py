import sqlite3
from datetime import datetime

def drop_columns():
    """
    Drop unwanted columns from tables by recreating them
    Creates a backup before making changes
    """
    
    db_path = 'quotes.db'
    backup_path = f'quotes_backup_{datetime.now().strftime("%Y%m%d_%H%M%S")}.db'
    
    # Create backup
    print("Creating backup...")
    import shutil
    shutil.copy(db_path, backup_path)
    print(f"  ✓ Backup saved to {backup_path}")
    
    conn = sqlite3.connect(db_path)
    cursor = conn.cursor()
    
    try:
        # ====================================================================
        # 1. DROP COLUMNS FROM AUTHORS TABLE
        # ====================================================================
        print("\nProcessing authors table...")
        
        cursor.execute("PRAGMA table_info(authors)")
        columns = cursor.fetchall()
        keep_columns = ['id', 'name', 'needs_review']
        
        column_names = [col[1] for col in columns]
        keep_indices = [i for i, name in enumerate(column_names) if name in keep_columns]
        
        # Create new table with only columns we want to keep
        cursor.execute("""
            CREATE TABLE authors_new (
                id INTEGER PRIMARY KEY,
                name VARCHAR(200) UNIQUE NOT NULL,
                needs_review BOOLEAN DEFAULT TRUE
            )
        """)
        
        # Copy data
        cursor.execute(f"""
            INSERT INTO authors_new (id, name, needs_review)
            SELECT id, name, needs_review FROM authors
        """)
        
        # Drop old table and rename
        cursor.execute("DROP TABLE authors")
        cursor.execute("ALTER TABLE authors_new RENAME TO authors")
        print("  ✓ Dropped: birth_year, death_year, nationality, profession, bio, edit")
        
        # ====================================================================
        # 2. DROP COLUMNS FROM QUOTES TABLE
        # ====================================================================
        print("\nProcessing quotes table...")
        
        cursor.execute("PRAGMA table_info(quotes)")
        columns = cursor.fetchall()
        keep_columns = ['id', 'text', 'author_id', 'source', 'tag_list', 'needs_review']
        
        column_names = [col[1] for col in columns]
        keep_indices = [i for i, name in enumerate(column_names) if name in keep_columns]
        
        cursor.execute("""
            CREATE TABLE quotes_new (
                id INTEGER PRIMARY KEY,
                text TEXT NOT NULL,
                author_id INTEGER NOT NULL,
                source VARCHAR(300),
                tag_list TEXT,
                needs_review BOOLEAN DEFAULT TRUE,
                FOREIGN KEY(author_id) REFERENCES authors(id)
            )
        """)
        
        cursor.execute(f"""
            INSERT INTO quotes_new (id, text, author_id, source, tag_list, needs_review)
            SELECT id, text, author_id, source, tag_list, needs_review FROM quotes
        """)
        
        cursor.execute("DROP TABLE quotes")
        cursor.execute("ALTER TABLE quotes_new RENAME TO quotes")
        print("  ✓ Dropped: year, context, created_at, verified, edit")
        
        # ====================================================================
        # 3. DROP COLUMN FROM USERS TABLE
        # ====================================================================
        print("\nProcessing users table...")
        
        cursor.execute("PRAGMA table_info(users)")
        columns = cursor.fetchall()
        keep_columns = ['id', 'name', 'email', 'password_hash', 'created_at', 'last_login', 'needs_review']
        
        column_names = [col[1] for col in columns]
        keep_indices = [i for i, name in enumerate(column_names) if name in keep_columns]
        
        cursor.execute("""
            CREATE TABLE users_new (
                id INTEGER PRIMARY KEY,
                name VARCHAR(80) UNIQUE NOT NULL,
                email VARCHAR(120) UNIQUE NOT NULL,
                password_hash VARCHAR(255) NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                last_login DATETIME,
                needs_review BOOLEAN DEFAULT TRUE
            )
        """)
        
        cursor.execute(f"""
            INSERT INTO users_new (id, name, email, password_hash, created_at, last_login, needs_review)
            SELECT id, name, email, password_hash, created_at, last_login, needs_review FROM users
        """)
        
        cursor.execute("DROP TABLE users")
        cursor.execute("ALTER TABLE users_new RENAME TO users")
        print("  ✓ Dropped: is_active")
        
        # ====================================================================
        # 4. DROP COLUMNS FROM TAGS TABLE
        # ====================================================================
        print("\nProcessing tags table...")
        
        cursor.execute("PRAGMA table_info(tags)")
        columns = cursor.fetchall()
        keep_columns = ['id', 'name', 'created_at', 'needs_review']
        
        column_names = [col[1] for col in columns]
        keep_indices = [i for i, name in enumerate(column_names) if name in keep_columns]
        
        cursor.execute("""
            CREATE TABLE tags_new (
                id INTEGER PRIMARY KEY,
                name VARCHAR(100) UNIQUE NOT NULL,
                created_at DATETIME DEFAULT CURRENT_TIMESTAMP,
                needs_review BOOLEAN DEFAULT TRUE
            )
        """)
        
        cursor.execute(f"""
            INSERT INTO tags_new (id, name, created_at, needs_review)
            SELECT id, name, created_at, needs_review FROM tags
        """)
        
        cursor.execute("DROP TABLE tags")
        cursor.execute("ALTER TABLE tags_new RENAME TO tags")
        print("  ✓ Dropped: quote_id, author_id, user_id")
        
        # Commit changes
        conn.commit()
        
        print("\n" + "=" * 60)
        print("✅ All columns dropped successfully!")
        print("=" * 60)
        print(f"Backup saved to: {backup_path}")
        
        return True
    
    except Exception as e:
        conn.rollback()
        print(f"\n❌ Error: {str(e)}")
        print(f"Your data is safe in: {backup_path}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        conn.close()


if __name__ == "__main__":
    success = drop_columns()
    
    if success:
        print("\nVerifying new schema...")
        conn = sqlite3.connect('quotes.db')
        cursor = conn.cursor()
        
        tables = ['authors', 'quotes', 'users', 'tags']
        for table in tables:
            cursor.execute(f"PRAGMA table_info({table})")
            columns = cursor.fetchall()
            print(f"\n{table}:")
            for col in columns:
                print(f"  {col[1]} ({col[2]})")
        
        conn.close()
