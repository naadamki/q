from sqlalchemy import text
from db import DB

db = DB()
try:
    db.session.execute(text("ALTER TABLE tags ADD COLUMN needs_review BOOLEAN DEFAULT 0"))
    db.session.execute(text("ALTER TABLE quotes ADD COLUMN needs_review BOOLEAN DEFAULT 0"))
    db.session.execute(text("ALTER TABLE authors ADD COLUMN needs_review BOOLEAN DEFAULT 0"))
    db.session.execute(text("ALTER TABLE users ADD COLUMN needs_review BOOLEAN DEFAULT 0"))
    db.commit()
    print("Columns added successfully")
except Exception as e:
    print(f"Error: {e}")
    db.rollback()
finally:
    db.close()