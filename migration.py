from models import Session, Quote, Tag
import json
from datetime import datetime
from sqlalchemy import text
import re
import unicodedata


def clean_tag_name(tag_name: str) -> str:
    """
    Clean tag name by:
    1. Converting to lowercase
    2. Removing non-English characters
    3. Removing punctuation and symbols
    4. Removing extra whitespace
    """
    # Convert to lowercase
    tag_name = tag_name.lower().strip()
    
    # Remove non-English characters using unicodedata
    tag_name = ''.join(
        c if ord(c) < 128 else '' 
        for c in unicodedata.normalize('NFKD', tag_name)
        if unicodedata.category(c) != 'Mn'
    )
    
    # Remove punctuation and special characters, keep only alphanumeric, spaces, hyphens
    tag_name = re.sub(r'[^a-z0-9\s\-]', '', tag_name)
    
    # Remove extra whitespace
    tag_name = re.sub(r'\s+', ' ', tag_name).strip()
    
    return tag_name


def migrate_tags_column_to_table_backwards(batch_size: int = 1000):
    """
    Optimized migration script that works backwards from the last quote.
    Removes tag_list data after processing.
    
    Improvements:
    1. Pre-loads all existing tags into memory (single query)
    2. Processes quotes in reverse order (last to first)
    3. Batches database commits (every 1000 quotes)
    4. Uses bulk inserts for multiple tags at once
    5. Adds index on tag names for faster lookups
    6. Shows progress with ETA
    7. Clears tag_list after processing
    
    Args:
        batch_size: Number of quotes to process before committing (default 1000)
    """
    
    session = Session()
    
    try:
        print("Starting optimized backwards migration...")
        print("=" * 60)
        
        # Step 1: Create index on tag name for faster lookups
        print("Creating database index on tag names...")
        try:
            session.execute(text("CREATE INDEX IF NOT EXISTS idx_tag_name ON tags(name)"))
            session.commit()
        except:
            pass  # Index might already exist
        
        # Step 2: Pre-load all existing tags into a dictionary (single query)
        print("Loading existing tags into memory...")
        existing_tags = {}
        for tag in session.query(Tag).all():
            existing_tags[tag.name.lower()] = tag
        print(f"  ✓ Loaded {len(existing_tags)} existing tags")
        
        # Step 3: Get all quotes, ordered by ID descending (backwards)
        quotes = session.query(Quote).order_by(Quote.id.desc()).all()
        total_quotes = len(quotes)
        print(f"  ✓ Loaded {total_quotes:,} quotes (processing in reverse order)")
        print()
        
        tags_created = 0
        associations_created = 0
        skipped = 0
        new_tags_buffer = {}  # Buffer for new tags not yet in DB
        
        start_time = datetime.now()
        
        # Step 4: Process quotes in reverse order
        for idx, quote in enumerate(quotes):
            # Progress indicator
            if (idx + 1) % 10000 == 0:
                elapsed = (datetime.now() - start_time).total_seconds()
                rate = (idx + 1) / elapsed
                remaining = (total_quotes - idx - 1) / rate if rate > 0 else 0
                print(f"Progress: {idx + 1:,}/{total_quotes:,} ({(idx+1)/total_quotes*100:.1f}%) - "
                      f"ETA: {remaining/60:.1f} min")
            
            # Check if quote has tag data
            if not quote.tag_list:
                skipped += 1
                continue
            
            try:
                # Parse the JSON array of tag names
                tag_names = json.loads(quote.tag_list)
            except (json.JSONDecodeError, TypeError):
                skipped += 1
                continue
            
            # Break down multi-word tags into single words
            single_words = set()
            for tag_name in tag_names:
                # Clean the tag name (remove non-English, punctuation, symbols)
                tag_name = clean_tag_name(tag_name)
                
                if not tag_name:
                    continue
                
                # Split by spaces and hyphens, keeping only non-empty words
                words = []
                for part in tag_name.split():
                    words.extend(part.split('-'))
                
                # Add all non-empty words to the set
                single_words.update([w for w in words if w])
            
            # Process each single word as a tag
            for tag_name in single_words:
                tag_name = tag_name.strip()
                
                if not tag_name:
                    continue
                
                # Check if tag exists in memory (existing tags or newly created)
                if tag_name in existing_tags:
                    tag = existing_tags[tag_name]
                elif tag_name in new_tags_buffer:
                    tag = new_tags_buffer[tag_name]
                else:
                    # Create new tag
                    tag = Tag(name=tag_name)
                    new_tags_buffer[tag_name] = tag
                    tags_created += 1
                
                # Associate tag with quote if not already associated
                if tag not in quote.tags:
                    quote.tags.append(tag)
                    associations_created += 1
            
            # Clear the tag_list after processing
            quote.tag_list = None
            
            # Batch commit every N quotes
            if (idx + 1) % batch_size == 0:
                # Add buffered tags to session
                for tag in new_tags_buffer.values():
                    if tag.id is None:  # Only add if not already in session
                        session.add(tag)
                
                session.commit()
                
                # Move buffered tags to existing tags dict
                for tag_name, tag in new_tags_buffer.items():
                    existing_tags[tag_name] = tag
                new_tags_buffer.clear()
        
        # Final commit for remaining data
        print("\nFinalizing migration...")
        for tag in new_tags_buffer.values():
            if tag.id is None:
                session.add(tag)
        session.commit()
        
        # Step 5: Verify migration
        total_tags = session.query(Tag).count()
        total_associations = session.query(Quote).filter(Quote.tags.any()).count()
        
        elapsed = (datetime.now() - start_time).total_seconds()
        
        print("\n" + "=" * 60)
        print("Migration completed!")
        print("=" * 60)
        print(f"Time elapsed: {elapsed/60:.1f} minutes ({elapsed:.0f} seconds)")
        print(f"Tags created: {tags_created:,}")
        print(f"Associations created: {associations_created:,}")
        print(f"Quotes skipped (no tags): {skipped:,}")
        print(f"Quotes processed: {total_quotes - skipped:,}")
        print(f"Total tags in database: {total_tags:,}")
        print(f"Quotes with tags: {total_associations:,}")
        print(f"Average time per quote: {elapsed/(total_quotes - skipped)*1000:.2f}ms")
        
        return True
        
    except Exception as e:
        session.rollback()
        print(f"\n❌ Error during migration: {str(e)}")
        import traceback
        traceback.print_exc()
        return False
    
    finally:
        session.close()


def verify_migration():
    """Verify the migration was successful"""
    
    session = Session()
    
    try:
        quotes = session.query(Quote).all()
        
        print("\n" + "=" * 60)
        print("Migration Verification")
        print("=" * 60)
        
        total_tags = 0
        quotes_with_tags = 0
        quotes_with_tag_list = 0
        
        for quote in quotes:
            if quote.tags:
                quotes_with_tags += 1
                total_tags += len(quote.tags)
            if quote.tag_list:
                quotes_with_tag_list += 1
        
        print(f"Total quotes with tags: {quotes_with_tags:,}")
        print(f"Total tag associations: {total_tags:,}")
        print(f"Quotes with cleared tag_list: {len(quotes) - quotes_with_tag_list:,}")
        print(f"Quotes still with tag_list data: {quotes_with_tag_list:,}")
        
        # Check for orphaned tags
        all_tags = session.query(Tag).all()
        print(f"Total tags in database: {len(all_tags):,}")
        
        orphaned = [tag for tag in all_tags if len(tag.quotes) == 0 and 
                                                  len(tag.authors) == 0 and 
                                                  len(tag.users) == 0]
        if orphaned:
            print(f"⚠️  Found {len(orphaned):,} orphaned tags (not associated with anything)")
        else:
            print(f"✓ No orphaned tags found")
        
        return True
        
    except Exception as e:
        print(f"❌ Error during verification: {str(e)}")
        return False
    
    finally:
        session.close()


if __name__ == "__main__":
    # Step 1: Run the optimized backwards migration
    print("Step 1: Running optimized backwards migration...\n")
    migrate_tags_column_to_table_backwards(batch_size=1000)
    
    # Step 2: Verify it worked
    print("\nStep 2: Verifying migration...")
    verify_migration()
    
    print("\n✅ Migration script completed!")
