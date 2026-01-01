from models import Session, Quote, Author, Category, User, Tag, user_quotes, user_authors
from typing import Optional, List, Dict, Any, Union, Tuple
from errors import NotFoundError, ValidationError, DatabaseError, DuplicateError
from sqlalchemy import func, and_, or_, select, desc, asc
from datetime import datetime
import re




class QuoteManager:
    """Manages quote-related database operations"""

    def __init__(self, session: Session):
        self.session = session

    @property
    def all(self) -> List[Quote]:
        """Get all quotes"""
        return self.session.query(Quote).all()

    @property
    def count(self) -> int:
        """Get total number of quotes"""
        return self.session.query(Quote).count()

    def get(self, quote_id: int) -> Quote:
        """Get quote by ID"""
        quote = self.session.query(Quote).filter_by(id=quote_id).first()
        if not quote:
            raise NotFoundError(f"Quote {quote_id} not found")
        return quote

    def create(self, text: str, author_id: int) -> Quote:
        """Create a new quote"""
        if not text or len(text.strip()) == 0:
            raise ValidationError("Quote text cannot be empty")
        
        quote = Quote(text=text, author_id=author_id)
        self.session.add(quote)
        return quote

    def update(self, quote_id: int, text: Optional[str] = None, author_id: Optional[int] = None) -> Quote:
        """Update a quote"""
        quote = self.get(quote_id)
        
        if text is not None:
            if len(text.strip()) == 0:
                raise ValidationError("Quote text cannot be empty")
            quote.text = text
        
        if author_id is not None:
            quote.author_id = author_id
        
        return quote

    def delete(self, quote_id: int) -> bool:
        """Delete a quote by ID"""
        quote = self.get(quote_id)
        self.session.delete(quote)
        return True

    def by_author(self, author_id: int) -> List[Quote]:
        """Get all quotes by a specific author"""
        return self.session.query(Quote).filter_by(author_id=author_id).all()

    def by_tag(self, tag_id: int) -> List[Quote]:
        """Get all quotes with a specific tag"""
        tag = self.session.query(Tag).get(tag_id)
        if not tag:
            raise NotFoundError(f"Tag {tag_id} not found")
        return tag.quotes

    def by_category(self, category_id: int) -> List[Quote]:
        """Get all quotes in a specific category"""
        category = self.session.query(Category).get(category_id)
        if not category:
            raise NotFoundError(f"Category {category_id} not found")
        return category.quotes

    def search(self, query: str) -> List[Quote]:
        """Search quotes by text"""
        return self.session.query(Quote).filter(Quote.text.ilike(f"%{query}%")).all()


class UserManager:
    """Manages user-related database operations"""

    def __init__(self, session: Session):
        self.session = session

    @property
    def all(self) -> List[User]:
        """Get all users"""
        return self.session.query(User).all()

    @property
    def count(self) -> int:
        """Get total number of users"""
        return self.session.query(User).count()

    def get(self, user_id: int) -> User:
        """Get user by ID"""
        user = self.session.query(User).filter_by(id=user_id).first()
        if not user:
            raise NotFoundError(f"User {user_id} not found")
        return user

    def create(self, username: str, email: str, password: str) -> User:
        """Create a new user"""
        if not username or len(username.strip()) < 3:
            raise ValidationError("Username must be at least 3 characters")
        if not email or '@' not in email:
            raise ValidationError("Invalid email address")
        
        existing = self.session.query(User).filter(
            or_(User.username == username, User.email == email)
        ).first()
        if existing:
            raise DuplicateError(f"Username or email already exists")
        
        user = User(username=username, email=email)
        user.set_password(password)
        self.session.add(user)
        return user

    def update(self, user_id: int, username: Optional[str] = None, email: Optional[str] = None) -> User:
        """Update a user"""
        user = self.get(user_id)
        
        if username is not None:
            if len(username.strip()) < 3:
                raise ValidationError("Username must be at least 3 characters")
            user.username = username
        
        if email is not None:
            if '@' not in email:
                raise ValidationError("Invalid email address")
            user.email = email
        
        return user

    def delete(self, user_id: int) -> bool:
        """Delete a user by ID"""
        user = self.get(user_id)
        self.session.delete(user)
        return True

    def by_username(self, username: str) -> Optional[User]:
        """Get user by username"""
        return self.session.query(User).filter_by(username=username).first()

    def by_email(self, email: str) -> Optional[User]:
        """Get user by email"""
        return self.session.query(User).filter_by(email=email).first()


class AuthorManager:
    """Manages author-related database operations"""

    def __init__(self, session: Session):
        self.session = session

    @property
    def all(self) -> List[Author]:
        """Get all authors"""
        return self.session.query(Author).all()

    @property
    def count(self) -> int:
        """Get total number of authors"""
        return self.session.query(Author).count()

    def get(self, author_id: int) -> Author:
        """Get author by ID"""
        author = self.session.query(Author).filter_by(id=author_id).first()
        if not author:
            raise NotFoundError(f"Author {author_id} not found")
        return author

    def create(self, name: str) -> Author:
        """Create a new author"""
        if not name or len(name.strip()) == 0:
            raise ValidationError("Author name cannot be empty")
        
        existing = self.session.query(Author).filter_by(name=name).first()
        if existing:
            raise DuplicateError(f"Author '{name}' already exists")
        
        author = Author(name=name)
        self.session.add(author)
        return author

    def update(self, author_id: int, name: str) -> Author:
        """Update an author"""
        author = self.get(author_id)
        
        if not name or len(name.strip()) == 0:
            raise ValidationError("Author name cannot be empty")
        
        author.name = name
        return author

    def delete(self, author_id: int) -> bool:
        """Delete an author by ID"""
        author = self.get(author_id)
        self.session.delete(author)
        return True

    def by_name(self, name: str) -> Optional[Author]:
        """Get author by name"""
        return self.session.query(Author).filter_by(name=name).first()

    def search(self, query: str) -> List[Author]:
        """Search authors by name"""
        return self.session.query(Author).filter(Author.name.ilike(f"%{query}%")).all()


class CategoryManager:
    """Manages category-related database operations"""

    def __init__(self, session: Session):
        self.session = session

    @property
    def all(self) -> List[Category]:
        """Get all categories"""
        return self.session.query(Category).all()

    @property
    def count(self) -> int:
        """Get total number of categories"""
        return self.session.query(Category).count()

    def get(self, category_id: int) -> Category:
        """Get category by ID"""
        category = self.session.query(Category).filter_by(id=category_id).first()
        if not category:
            raise NotFoundError(f"Category {category_id} not found")
        return category

    def create(self, name: str, keywords: Optional[List[str]] = None) -> Category:
        """Create a new category"""
        if not name or len(name.strip()) == 0:
            raise ValidationError("Category name cannot be empty")
        
        existing = self.session.query(Category).filter_by(name=name).first()
        if existing:
            raise DuplicateError(f"Category '{name}' already exists")
        
        category = Category(name=name)
        if keywords:
            category.set_keywords(keywords)
        self.session.add(category)
        return category

    def update(self, category_id: int, name: Optional[str] = None, keywords: Optional[List[str]] = None) -> Category:
        """Update a category"""
        category = self.get(category_id)
        
        if name is not None:
            if len(name.strip()) == 0:
                raise ValidationError("Category name cannot be empty")
            category.name = name
        
        if keywords is not None:
            category.set_keywords(keywords)
        
        return category

    def delete(self, category_id: int) -> bool:
        """Delete a category by ID"""
        category = self.get(category_id)
        self.session.delete(category)
        return True

    def by_name(self, name: str) -> Optional[Category]:
        """Get category by name"""
        return self.session.query(Category).filter_by(name=name).first()


class TagManager:
    """Manages tag-related database operations"""

    def __init__(self, session: Session):
        self.session = session

    @property
    def all(self) -> List[Tag]:
        """Get all tags"""
        return self.session.query(Tag).all()

    @property
    def count(self) -> int:
        """Get total number of tags"""
        return self.session.query(Tag).count()

    def get(self, tag_id: int) -> Tag:
        """Get tag by ID"""
        tag = self.session.query(Tag).filter_by(id=tag_id).first()
        if not tag:
            raise NotFoundError(f"Tag {tag_id} not found")
        return tag

    def create(self, name: str) -> Tag:
        """Create a new tag"""
        if not name or len(name.strip()) == 0:
            raise ValidationError("Tag name cannot be empty")
        
        existing = self.session.query(Tag).filter_by(name=name).first()
        if existing:
            raise DuplicateError(f"Tag '{name}' already exists")
        
        tag = Tag(name=name)
        self.session.add(tag)
        return tag

    def update(self, tag_id: int, name: str) -> Tag:
        """Update a tag"""
        tag = self.get(tag_id)
        
        if not name or len(name.strip()) == 0:
            raise ValidationError("Tag name cannot be empty")
        
        tag.name = name
        return tag

    def delete(self, tag_id: int) -> bool:
        """Delete a tag by ID"""
        tag = self.get(tag_id)
        self.session.delete(tag)
        return True

    def by_name(self, name: str) -> Optional[Tag]:
        """Get tag by name"""
        return self.session.query(Tag).filter_by(name=name).first()

    def search(self, query: str) -> List[Tag]:
        """Search tags by name"""
        return self.session.query(Tag).filter(Tag.name.ilike(f"%{query}%")).all()


class DB:
    """Database access layer"""

    def __init__(self):
        self.session = Session()
        self.quotes = QuoteManager(self.session)
        self.users = UserManager(self.session)
        self.authors = AuthorManager(self.session)
        self.categories = CategoryManager(self.session)
        self.tags = TagManager(self.session)

    def enter(self):
        """Enter the context manager"""
        return self

    def exit(self, exc_type, exc_value, traceback):
        """Exit the context manager"""
        if exc_type:
            self.session.rollback()
        else:
            self.session.commit()
        self.session.close()

    def commit(self):
        """Commit the current transaction"""
        try:
            self.session.commit()
        except Exception as e:
            self.session.rollback()
            raise DatabaseError(f"Database commit failed: {str(e)}")

    def rollback(self):
        """Rollback the current transaction"""
        self.session.rollback()

    def close(self):
        """Close the session"""
        self.session.close()