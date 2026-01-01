from models import Session, Quote, Tag
import json


def migrate_tags_column_to_table():
    """
    Migrate tags from quotes.tags_data column to tags table and quote_tags junction table.
    
    This script:
    1. Gets all quotes with tag data in the tags_data column
    2. For each tag name, creates a Tag entry if it doesn't exist
    3. Associates the quote with each tag via the quote_tags junction table
    4. Optionally clears the old tags_data column
    """
    
    session = Session()
    
    try:
        # Get all quotes
        quotes = session.query(Quote).all()
        
        tags_created = 0
        associations_created = 0
        skipped = 0
        
        print(f"Starting migration for {len(quotes)} quotes...")
        
        for quote in quotes:
            # Check if quote has tag data
            if not quote.tag_list:
                skipped += 1
                continue
            
            try:
                # Parse the JSON array of tag names
                tag_names = json.loads(quote.tag_list)
            except (json.JSONDecodeError, TypeError):
                print(f"⚠️  Skipped quote {quote.id}: Invalid JSON in tag_list")
                skipped += 1
                continue
            
            # Break down multi-word tags into single words
            single_words = set()
            for tag_name in tag_names:
                tag_name = tag_name.strip().lower()
                
                if not tag_name:
                    continue
                
                # Split by spaces and hyphens, keeping only non-empty words
                words = []
                for part in tag_name.split():
                    words.extend(part.split('-'))
                
                # Add all non-empty words to the set (automatically removes duplicates)
                single_words.update([w for w in words if w])
            
            # Process each single word as a tag
            for tag_name in single_words:
                tag_name = tag_name.strip()
                
                if not tag_name:
                    continue
                
                # Check if tag already exists
                tag = session.query(Tag).filter_by(name=tag_name).first()
                
                if not tag:
                    # Create new tag
                    tag = Tag(name=tag_name)
                    session.add(tag)
                    tags_created += 1
                    print(f"  ✓ Created tag: '{tag_name}'")
                
                # Associate tag with quote if not already associated
                if tag not in quote.tags:
                    quote.tags.append(tag)
                    associations_created += 1
        
        # Commit all changes
        session.commit()
        
        print("\n" + "="*50)
        print("Migration completed!")
        print("="*50)
        print(f"Tags created: {tags_created}")
        print(f"Associations created: {associations_created}")
        print(f"Quotes skipped (no tags): {skipped}")
        print(f"Total quotes processed: {len(quotes) - skipped}")
        
        return True
        
    except Exception as e:
        session.rollback()
        print(f"\n❌ Error during migration: {str(e)}")
        return False
    
    finally:
        session.close()


def migrate_tags_and_clear_column():
    """
    Same as migrate_tags_column_to_table() but also clears the old tags_data column after migration.
    Use this only after verifying the migration worked correctly!
    """
    
    session = Session()
    
    try:
        # Run the migration first
        success = migrate_tags_column_to_table()
        
        if not success:
            print("Migration failed, not clearing old column")
            return False
        
        # Clear the old tags_data column
        session = Session()
        quotes = session.query(Quote).all()
        
        for quote in quotes:
            quote.tag_list = None
        
        session.commit()
        print("\n✓ Cleared old tags_data column from all quotes")
        return True
        
    except Exception as e:
        session.rollback()
        print(f"\n❌ Error clearing column: {str(e)}")
        return False
    
    finally:
        session.close()


def verify_migration():
    """Verify the migration was successful"""
    
    session = Session()
    
    try:
        quotes = session.query(Quote).all()
        
        print("\n" + "="*50)
        print("Migration Verification")
        print("="*50)
        
        total_tags = 0
        quotes_with_tags = 0
        
        for quote in quotes:
            if quote.tags:
                quotes_with_tags += 1
                total_tags += len(quote.tags)
                tag_names = [tag.name for tag in quote.tags]
                print(f"Quote {quote.id}: {tag_names}")
        
        print("\n" + "-"*50)
        print(f"Total quotes with tags: {quotes_with_tags}")
        print(f"Total tag associations: {total_tags}")
        
        # Check for orphaned tags
        all_tags = session.query(Tag).all()
        print(f"Total tags in database: {len(all_tags)}")
        
        orphaned = [tag for tag in all_tags if len(tag.quotes) == 0 and 
                                                  len(tag.authors) == 0 and 
                                                  len(tag.users) == 0]
        if orphaned:
            print(f"⚠️  Found {len(orphaned)} orphaned tags (not associated with anything)")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during verification: {str(e)}")
        return False
    
    finally:
        session.close()


if __name__ == "__main__":
    # Step 1: Run the migration
    print("Step 1: Migrating tags from column to table...")
    migrate_tags_column_to_table()
    
    # Step 2: Verify it worked
    print("\nStep 2: Verifying migration...")
    verify_migration()
    
    # Step 3: If everything looks good, uncomment below to clear the old column
    # print("\nStep 3: Clearing old tags_data column...")
    # migrate_tags_and_clear_column()
    
    print("\n✅ Migration script completed!")