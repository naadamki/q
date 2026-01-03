
import unicodedata
import re
from typing import Optional, Union
from sqlalchemy.orm import Session
from models import Quote, Author, Tag, User, Category
from errors import ValidationError, DuplicateError


class Validator:
    """Validates and sanitizes domain objects before persistence"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def validate(self, obj: Union[Quote, Author, Tag, User, Category], 
                 exclude_id: Optional[int] = None) -> Optional[Union[Quote, Author, Tag, User, Category]]:
        """
        Validate and sanitize an object
        
        Args:
            obj: Domain object to validate (Quote, Author, Tag, User, or Category)
            exclude_id: ID to exclude from duplicate check (for updates)
        
        Returns:
            Sanitized object if valid, False if duplicate exists
            
        Raises:
            ValidationError: If validation fails
        """
        if isinstance(obj, Quote):
            return self._validate_quote(obj, exclude_id=exclude_id)
        elif isinstance(obj, Author):
            return self._validate_author(obj, exclude_id=exclude_id)
        elif isinstance(obj, Tag):
            return self._validate_tag(obj, exclude_id=exclude_id)
        elif isinstance(obj, User):
            return self._validate_user(obj, exclude_id=exclude_id)
        elif isinstance(obj, Category):
            return self._validate_category(obj, exclude_id=exclude_id)
        else:
            raise ValidationError(f"Unknown object type: {type(obj)}")
    
    # ============================================================================
    # QUOTE VALIDATION
    # ============================================================================
    
    def _validate_quote(self, quote: Quote, exclude_id: Optional[int] = None) -> Optional[Quote]:
        """Validate and sanitize a Quote object"""
        # Validate text
        if not quote.text or not quote.text.strip():
            raise ValidationError("Quote text cannot be empty")
        
        if len(quote.text) > 5000:
            raise ValidationError("Quote text cannot exceed 5000 characters")
        
        quote.text = quote.text.strip()
        
        # Check for duplicate by exact text match (exclude current quote on updates)
        existing = self.session.query(Quote).filter(
            Quote.text == quote.text
        )
        
        if exclude_id:
            existing = existing.filter(Quote.id != exclude_id)
        
        if existing.first():
            return False  # Duplicate exists
        
        # Validate source if provided
        if quote.source and len(quote.source) > 300:
            raise ValidationError("Source cannot exceed 300 characters")
        
        if quote.source:
            quote.source = quote.source.strip()
        
        # Validate author exists
        if quote.author_id:
            author = self.session.query(Author).filter(Author.id == quote.author_id).first()
            if not author:
                raise ValidationError(f"Author with ID {quote.author_id} does not exist")
        
        return quote
    
    # ============================================================================
    # AUTHOR VALIDATION
    # ============================================================================
    
    def _validate_author(self, author: Author, exclude_id: Optional[int] = None) -> Optional[Author]:
        """Validate and sanitize an Author object"""
        if not author.name or not author.name.strip():
            raise ValidationError("Author name cannot be empty")
        
        # Sanitize name
        author.name = self._sanitize_author_name(author.name)
        
        # Check for duplicate (case-insensitive, exclude self on updates)
        existing = self.session.query(Author).filter(
            Author.name.ilike(author.name)
        )
        
        if exclude_id:
            existing = existing.filter(Author.id != exclude_id)
        
        if existing.first():
            return False  # Duplicate exists
        
        return author
    
    # ============================================================================
    # TAG VALIDATION
    # ============================================================================
    
    def _validate_tag(self, tag: Tag, exclude_id: Optional[int] = None) -> Optional[Tag]:
        """Validate and sanitize a Tag object"""
        if not tag.name or not tag.name.strip():
            raise ValidationError("Tag name cannot be empty")
        
        # Sanitize name
        tag.name = self._sanitize_tag_name(tag.name)
        
        # Check for duplicate (case-insensitive, exclude self on updates)
        existing = self.session.query(Tag).filter(
            Tag.name.ilike(tag.name)
        )
        
        if exclude_id:
            existing = existing.filter(Tag.id != exclude_id)
        
        if existing.first():
            return False  # Duplicate exists
        
        return tag
    
    # ============================================================================
    # USER VALIDATION
    # ============================================================================
    
    def _validate_user(self, user: User, exclude_id: Optional[int] = None) -> Optional[User]:
        """Validate and sanitize a User object"""
        # Validate name
        if not user.name or not user.name.strip():
            raise ValidationError("Name cannot be empty")
        
        if len(user.name) < 3:
            raise ValidationError("Name must be at least 3 characters")
        
        user.name = user.name.strip()
        
        # Validate email
        if not user.email or "@" not in user.email:
            raise ValidationError("Invalid email format")
        
        user.email = user.email.strip().lower()
        
        # Check for duplicates (exclude self on updates)
        existing = self.session.query(User).filter(
            (User.name == user.name) | (User.email == user.email)
        )
        
        if exclude_id:
            existing = existing.filter(User.id != exclude_id)
        
        if existing.first():
            return False  # Duplicate exists
        
        return user
    
    # ============================================================================
    # CATEGORY VALIDATION
    # ============================================================================
    
    def _validate_category(self, category: Category, exclude_id: Optional[int] = None) -> Optional[Category]:
        """Validate and sanitize a Category object"""
        if not category.name or not category.name.strip():
            raise ValidationError("Category name cannot be empty")
        
        if len(category.name) > 50:
            raise ValidationError("Category name cannot exceed 50 characters")
        
        category.name = category.name.strip()
        
        # Check for duplicate (case-insensitive, exclude self on updates)
        existing = self.session.query(Category).filter(
            Category.name.ilike(category.name)
        )
        
        if exclude_id:
            existing = existing.filter(Category.id != exclude_id)
        
        if existing.first():
            return False  # Duplicate exists
        
        return category
    
    # ============================================================================
    # SANITIZATION METHODS
    # ============================================================================
    
    def _sanitize_tag_name(self, name: str) -> str:
        """
        Sanitize tag name:
        - Lowercase
        - Remove non-English characters
        - Single word (no spaces)
        - No punctuation or symbols (only alphanumeric)
        """
        name = name.lower().strip()
        
        # Remove non-English characters
        name = ''.join(
            c for c in unicodedata.normalize('NFKD', name)
            if ord(c) < 128
        )
        
        # Keep only alphanumeric
        name = re.sub(r'[^a-z0-9]', '', name)
        
        if not name:
            raise ValidationError("Tag must contain at least one alphanumeric character")
        
        if len(name) > 100:
            raise ValidationError("Tag name cannot exceed 100 characters")
        
        return name
    
    def _sanitize_author_name(self, name: str) -> str:
        """
        Sanitize author name:
        - Remove non-English characters
        - Single space between words
        - Capitalize first letter of each word
        - Handle hyphenated names (capitalize after hyphen)
        - Add period after single letters and abbreviations
        - Only allow: letters, spaces, hyphens, periods
        """
        # Remove non-English characters
        name = ''.join(
            c for c in unicodedata.normalize('NFKD', name)
            if ord(c) < 128 or c.isspace()
        )
        
        # Remove non-allowed characters
        name = re.sub(r"[^a-zA-Z\s\-\.]", "", name)
        
        # Split into parts (by spaces and hyphens) but preserve delimiters
        parts = re.split(r'(\s+|-)', name)
        
        sanitized_parts = []
        for part in parts:
            if not part:
                continue
            
            if part.isspace():
                if not sanitized_parts or sanitized_parts[-1] != ' ':
                    sanitized_parts.append(' ')
            elif part == '-':
                sanitized_parts.append('-')
            else:
                # It's a word/abbreviation
                part = part.strip()
                
                if len(part) == 1:
                    # Single letter - capitalize and add period
                    sanitized_parts.append(part.upper() + '.')
                elif len(part) == 2 and part.isupper():
                    # Already two uppercase letters (abbreviation)
                    if not part.endswith('.'):
                        sanitized_parts.append(part + '.')
                    else:
                        sanitized_parts.append(part)
                else:
                    # Regular word - capitalize first letter, lowercase rest
                    sanitized_parts.append(part.capitalize())
        
        # Join and clean up spacing
        result = ''.join(sanitized_parts)
        result = re.sub(r'\s+', ' ', result)
        result = re.sub(r'\s+\.', '.', result)
        result = re.sub(r'-\s+', '-', result)
        
        # Capitalize letter after hyphen
        def capitalize_after_hyphen(match):
            return match.group(1) + match.group(2).upper()
        
        result = re.sub(r'(-\s*)([a-z])', capitalize_after_hyphen, result)
        
        return result.strip()
