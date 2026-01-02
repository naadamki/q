from models import user_quotes, user_authors
from typing import Dict, Any, Union, Tuple
from sqlalchemy import func, and_, or_, select, desc, asc
from datetime import datetime
import re

from models import Session, Quote, Author, Category, User, Tag
from typing import Optional, List, TypeVar, Generic, Type
from abc import ABC, abstractmethod
from errors import NotFoundError, ValidationError, DatabaseError, DuplicateError, assert_that

T = TypeVar('T')

class Repository(ABC, Generic[T]):
    """Generic repository for database operations"""
    
    def __init__(self, session: Session, model: Type[T]):
        self.session = session
        self.model = model
    
    def all(self) -> List[T]:
        """Get all records"""
        return self.session.query(self.model).all()
    
    def count(self) -> int:
        """Get total count"""
        return self.session.query(self.model).count()
    
    def get(self, id: int) -> T:
        """Get record by ID"""
        record = self.session.query(self.model).filter_by(id=id).first()
        assert_that(not record).raiseNotFoundError(f"{self.model.__name__} {id} not found")
        return record
    
    def delete(self, id: int) -> bool:
        """Delete a record"""
        record = self.get(id)
        self.session.delete(record)
        return True
    
    def filter_by(self, **kwargs) -> List[T]:
        """Filter records by attributes"""
        return self.session.query(self.model).filter_by(**kwargs).all()

    def search(self, query: str) -> List[T]:
        """Search records"""
        search_field = self.model.text if self.model.__name__ == 'Quote' else self.model.name
        return self.session.query(self.model).filter(search_field.ilike(f"%{query}%")).all()

    def by_name(self, name: str) -> Optional[T]:
        """Get record by name"""
        if self.model.__name__ == 'Quote':
            raise NotImplementedError("Quote doesn't have a name attribute")
        return self.session.query(self.model).filter_by(name=name).first()

    def get_needs_review(self) -> List[T]:
        """Get records that need review"""
        assert_that(self.model.__name__ == 'Category').raiseNotImplementedError("Category doesn't have needs_review attribute")
        return self.session.query(self.model).filter_by(needs_review=True).all()

class QuoteRepository(Repository[Quote]):
    """Quote-specific repository"""
    
    def create(self, text: str, author_id: int) -> Quote:
        """Create a new quote"""
        assert_that(not text or len(text.strip()) == 0).raiseValidationError("Quote text cannot be empty")
        quote = Quote(text=text, author_id=author_id)
        self.session.add(quote)
        return quote
    
    def update(self, quote_id: int, text: Optional[str] = None, author_id: Optional[int] = None) -> Quote:
        """Update a quote"""
        quote = self.get(quote_id)
        if text is not None:
            assert_that(len(text.strip()) == 0).raiseValidationError("Quote text cannot be empty")
            quote.text = text
        if author_id is not None:
            quote.author_id = author_id
        return quote
        
    def by_author(self, author_id: int) -> List[Quote]:
        """Get quotes by author"""
        return self.filter_by(author_id=author_id)
    
    def by_tag(self, tag_id: int) -> List[Quote]:
        """Get quotes with a tag"""
        tag = self.session.query(Tag).get(tag_id)
        assert_that(not tag).raiseNotFoundError(f"Tag {tag_id} not found")
        return tag.quotes
    
    def by_category(self, category_id: int) -> List[Quote]:
        """Get quotes in a category"""
        category = self.session.query(Category).get(category_id)
        assert_that(not category).raiseNotFoundError(f"Category {category_id} not found")
        return category.quotes


class UserRepository(Repository[User]):
    """User-specific repository"""
    
    def create(self, name: str, email: str, password: str) -> User:
        """Create a new user"""
        assert_that(not name or len(name.strip()) < 3).raiseValidationError("Name must be at least 3 characters")
        assert_that(not email or '@' not in email).raiseValidationError("Invalid email address")
        
        existing = self.session.query(User).filter(
            (User.name == name) | (User.email == email)
        ).first()
        assert_that(existing is not None).raiseDuplicateError("Name or email already exists")

        user = User(name=name, email=email)
        user.set_password(password)
        self.session.add(user)
        return user

    def update(self, user_id: int, name: Optional[str] = None, email: Optional[str] = None) -> User:
        """Update a user"""
        user = self.get(user_id)
        if name is not None:
            assert_that(len(name.strip()) < 3).raiseValidationError("Name must be at least 3 characters")
            user.name = name
        if email is not None:
            assert_that('@' not in email).raiseValidationError("Invalid email address")
            user.email = email
        return user

    def by_email(self, email: str) -> Optional[User]:
        """Get user by email"""
        return self.session.query(User).filter_by(email=email).first()


class AuthorRepository(Repository[Author]):
    """Author-specific repository"""
    
    def create(self, name: str) -> Author:
        """Create a new author"""
        assert_that(not name or len(name.strip()) == 0).raiseValidationError("Author name cannot be empty")
        existing = self.session.query(Author).filter_by(name=name).first()
        assert_that(existing is not None).raiseDuplicateError(f"Author '{name}' already exists")
        author = Author(name=name)
        self.session.add(author)
        return author
    
    def update(self, author_id: int, name: str) -> Author:
        """Update an author"""
        author = self.get(author_id)
        assert_that(not name or len(name.strip()) == 0).raiseValidationError("Author name cannot be empty")
        author.name = name
        return author


class CategoryRepository(Repository[Category]):
    """Category-specific repository"""
    
    def create(self, name: str, keywords: Optional[List[str]] = None) -> Category:
        """Create a new category"""
        assert_that(not name or len(name.strip()) == 0).raiseValidationError("Category name cannot be empty")
        existing = self.session.query(Category).filter_by(name=name).first()
        assert_that(existing is not None).raiseDuplicateError(f"Category '{name}' already exists")
        category = Category(name=name)
        if keywords:
            category.set_keywords(keywords)
        self.session.add(category)
        return category
    
    def update(self, category_id: int, name: Optional[str] = None, keywords: Optional[List[str]] = None) -> Category:
        """Update a category"""
        category = self.get(category_id)
        if name is not None:
            assert_that(len(name.strip()) == 0).raiseValidationError("Category name cannot be empty")
            category.name = name
        if keywords is not None:
            category.set_keywords(keywords)
        return category
    

class TagRepository(Repository[Tag]):
    """Tag-specific repository"""
    
    def create(self, name: str) -> Tag:
        """Create a new tag"""
        assert_that(not name or len(name.strip()) == 0).raiseValidationError("Tag name cannot be empty")
        existing = self.session.query(Tag).filter_by(name=name).first()
        assert_that(existing is not None).raiseDuplicateError(f"Tag '{name}' already exists")
        tag = Tag(name=name)
        self.session.add(tag)
        return tag
    
    def update(self, tag_id: int, name: str) -> Tag:
        """Update a tag"""
        tag = self.get(tag_id)
        assert_that(not name or len(name.strip()) == 0).raiseValidationError("Tag name cannot be empty")
        tag.name = name
        return tag
    


# ============================================================================
# ============================================================================


class SearchFacade:
    """Advanced search interface across all models"""
    
    def __init__(self, quotes: QuoteRepository, authors: AuthorRepository, categories: CategoryRepository, 
                 tags: TagRepository, session: Session):
        self._quotes = quotes
        self._authors = authors
        self._categories = categories
        self._tags = tags
        self.session = session
    
    def all(self, query: str) -> dict:
        """Search across all models"""
        return {
            'quotes': self._quotes.search(query),
            'authors': self._authors.search(query),
            'categories': self._categories.search(query),
            'tags': self._tags.search(query)
        }
    
    def quotes(self, query: str) -> List[Quote]:
        """Search quotes"""
        return self._quotes.search(query)
    
    def authors(self, query: str) -> List[Author]:
        """Search authors"""
        return self._authors.search(query)

    def categories(self, query: str) -> List[Category]:
        """Search categories"""
        return self._categories.search(query)

    def by_categories(self, category_names: List[str], match_all: bool = False) -> List[Quote]:
        """Search quotes by multiple categories
        
        Args:
            category_names: List of category names
            match_all: If True, quotes must be in ALL categories. If False, ANY category.
        """
        _categories = [self.session.query(Category).filter_by(name=name).first() for name in category_names]
        _categories = [c for c in _categories if c is not None]
        
        if not _categories:
            return []
        
        if match_all:
            _quotes = set(_categories[0].quotes)
            for category in _categories[1:]:
                _quotes = _quotes.intersection(set(category.quotes))
            return list(_quotes)
        else:
            _quotes = set()
            for category in _categories:
                _quotes.update(category.quotes)
            return list(_quotes)

    def tags(self, query: str) -> List[Tag]:
        """Search tags"""
        return self._tags.search(query)
    
    def by_tags(self, tag_names: List[str], match_all: bool = False) -> List[Quote]:
        """Search quotes by multiple tags
        
        Args:
            tag_names: List of tag names to search for
            match_all: If True, quotes must have ALL tags. If False, quotes with ANY tag.
        """
        _tags = [self._tags.by_name(name) for name in tag_names]
        _tags = [t for t in _tags if t is not None]
        
        if not _tags:
            return []
        
        if match_all:
            _quotes = set(_tags[0].quotes)
            for tag in _tags[1:]:
                _quotes = _quotes.intersection(set(tag.quotes))
            return list(_quotes)
        else:
            _quotes = set()
            for tag in _tags:
                _quotes.update(tag.quotes)
            return list(_quotes)
        
    def advanced(self, text: str = None, author: str = None, 
                 tags: List[str] = None, categories: List[str] = None,
                 match_all_tags: bool = False, match_all_categories: bool = False) -> List[Quote]:
        """Advanced search combining multiple criteria
        
        Args:
            text: Search in quote text
            author: Search by author name
            tags: Search by tag names
            categories: Search by category names
            match_all_tags: If True, quote must have all tags
            match_all_categories: If True, quote must be in all categories
        """
        results = None
        
        # Start with text search if provided
        if text:
            results = set(self._quotes.search(text))
        
        # Filter by author if provided
        if author:
            author_obj = self._authors.by_name(author)
            if author_obj:
                author_quotes = set(author_obj.quotes)
                results = results.intersection(author_quotes) if results else author_quotes
            else:
                return []
        
        # Filter by tags if provided
        if tags:
            tag_quotes = set(self.by_tags(tags, match_all=match_all_tags))
            results = results.intersection(tag_quotes) if results else tag_quotes
        
        # Filter by categories if provided
        if categories:
            cat_quotes = set(self.by_categories(categories, match_all=match_all_categories))
            results = results.intersection(cat_quotes) if results else cat_quotes
        
        return list(results) if results else []


class UserFacade:
    """Simplified interface for user operations"""
    
    def __init__(self, users: UserRepository, quotes: QuoteRepository, session: Session):
        self.users = users
        self.quotes = quotes
        self.session = session
    
    def profile(self, user_id: int) -> dict:
        """Get complete user profile"""
        user = self.users.get(user_id)
        return {
            'id': user.id,            
            'name': user.name,
            'email': user.email,
            'created_at': user.created_at,
            'last_login': user.last_login,
            'is_active': user.is_active,
            'quotes': user.quotes,
            'authors': user.authors,
            'tags': user.tags,
            'quote_count': user.quote_count,
            'author_count': user.author_count,
            'tag_count': user.tag_count,
            'user': user            
        }
    
    def add_quote(self, user_id: int, quote_id: int) -> bool:
        """Add a quote to user's favorites"""
        user = self.users.get(user_id)
        quote = self.quotes.get(quote_id)
        if quote not in user.quotes:
            user.quotes.append(quote)
            return True
        return False
    
    def remove_quote(self, user_id: int, quote_id: int) -> bool:
        """Remove a quote from user's favorites"""
        user = self.users.get(user_id)
        quote = self.quotes.get(quote_id)
        if quote in user.quotes:
            user.quotes.remove(quote)
            return True
        return False
    
    def add_author(self, user_id: int, author_id: int) -> bool:
        """Add an author to user's favorites"""
        user = self.users.get(user_id)
        author = self.session.query(Author).get(author_id)
        assert_that(not author).raiseNotFoundError(f"Author {author_id} not found")
        if author not in user.authors:
            user.authors.append(author)
            return True
        return False
    
    def remove_author(self, user_id: int, author_id: int) -> bool:
        """Remove an author from user's favorites"""
        user = self.users.get(user_id)
        author = self.session.query(Author).get(author_id)
        assert_that(not author).raiseNotFoundError(f"Author {author_id} not found")
        if author in user.authors:
            user.authors.remove(author)
            return True
        return False
    
    def add_tag(self, user_id: int, tag_id: int) -> bool:
        """Add a tag to user"""
        user = self.users.get(user_id)
        tag = self.session.query(Tag).get(tag_id)
        assert_that(not tag).raiseNotFoundError(f"Tag {tag_id} not found")
        if tag not in user.tags:
            user.tags.append(tag)
            return True
        return False
    
    def remove_tag(self, user_id: int, tag_id: int) -> bool:
        """Remove a tag from user"""
        user = self.users.get(user_id)
        tag = self.session.query(Tag).get(tag_id)
        assert_that(not tag).raiseNotFoundError(f"Tag {tag_id} not found")
        if tag in user.tags:
            user.tags.remove(tag)
            return True
        return False


# ============================================================================
# ============================================================================


class QuoteBuilder:
    """Build quotes with optional fields"""
    
    def __init__(self, session: Session):
        self.session = session
        self._text: Optional[str] = None
        self._author: Optional[Author] = None
        self._tags: List[Tag] = []
        self._categories: List[Category] = []
    
    def text(self, text: str) -> 'QuoteBuilder':
        """Set quote text"""
        assert_that(not text or len(text.strip()) == 0).raiseValidationError("Quote text cannot be empty")
        self._text = text
        return self
    
    def author(self, author: Author) -> 'QuoteBuilder':
        """Set author"""
        self._author = author
        return self

    def author_name(self, author_name: str) -> 'QuoteBuilder':
        """Set author by name"""
        author = self.session.query(Author).filter_by(name=author_name).first()
        assert_that(not author).raiseNotFoundError(f"Author '{author_name}' not found")
        self._author = author
        return self
    
    def author_id(self, author_id: int) -> 'QuoteBuilder':
        """Set author by ID"""
        author = self.session.query(Author).get(author_id)
        assert_that(not author).raiseNotFoundError(f"Author {author_id} not found")
        self._author = author
        return self
    
    def tag(self, tag: Tag) -> 'QuoteBuilder':
        """Add a single tag"""
        if tag not in self._tags:
            self._tags.append(tag)
        return self
    
    def tag_names(self, tag_names: List[str]) -> 'QuoteBuilder':
        """Add tags by name"""
        for tag_name in tag_names:
            tag = self.session.query(Tag).filter_by(name=tag_name).first()
            if tag and tag not in self._tags:
                self._tags.append(tag)
        return self

    def tags(self, tags: List[Tag]) -> 'QuoteBuilder':
        """Add multiple tags"""
        for tag in tags:
            self.tag(tag)
        return self
    
    def category(self, category: Category) -> 'QuoteBuilder':
        """Add a single category"""
        if category not in self._categories:
            self._categories.append(category)
        return self
    
    def categories(self, categories: List[Category]) -> 'QuoteBuilder':
        """Add multiple categories"""
        for category in categories:
            self.category(category)
        return self
    
    def build(self) -> Quote:
        """Build and return the quote"""
        assert_that(not self._text).raiseValidationError("Quote text is required")
        assert_that(not self._author).raiseValidationError("Author is required")
        
        quote = Quote(text=self._text, author=self._author)
        
        for tag in self._tags:
            quote.tags.append(tag)
        
        for category in self._categories:
            quote.categories.append(category)
        
        self.session.add(quote)
        return quote


class UserBuilder:
    """Build users with optional fields"""
    
    def __init__(self, session: Session):
        self.session = session
        self._name: Optional[str] = None
        self._email: Optional[str] = None
        self._password: Optional[str] = None
        self._tags: List[Tag] = []
    
    def name(self, name: str) -> 'UserBuilder':
        """Set name"""
        assert_that(not name or len(name.strip()) < 3).raiseValidationError("Name must be at least 3 characters")
        self._name = name
        return self
    
    def email(self, email: str) -> 'UserBuilder':
        """Set email"""
        assert_that('@' not in email).raiseValidationError("Invalid email address")
        self._email = email
        return self
    
    def password(self, password: str) -> 'UserBuilder':
        """Set password"""
        assert_that(not password or len(password) < 6).raiseValidationError("Password must be at least 6 characters")
        self._password = password
        return self
    
    def tag(self, tag: Tag) -> 'UserBuilder':
        """Add a tag to the user"""
        if tag not in self._tags:
            self._tags.append(tag)
        return self
    
    def build(self) -> User:
        """Build and return the user"""
        assert_that(not self._name or not self._email or not self._password).raiseValidationError("Name, email, and password are required")
        
        existing = self.session.query(User).filter(
            (User.name == self._name) | (User.email == self._email)
        ).first()
        assert_that(existing is not None).raiseDuplicateError("Name or email already exists")
        
        user = User(name=self._name, email=self._email)
        user.set_password(self._password)
        
        for tag in self._tags:
            user.tags.append(tag)
        
        self.session.add(user)
        return user


# ============================================================================
# ============================================================================

class DB:
    """Database access layer with repositories and facades"""

    def __init__(self):
        self.session = Session()
        
        # Initialize repositories
        self.quotes = QuoteRepository(self.session, Quote)
        self.users = UserRepository(self.session, User)
        self.authors = AuthorRepository(self.session, Author)
        self.categories = CategoryRepository(self.session, Category)
        self.tags = TagRepository(self.session, Tag)
        
        # Initialize facades
        self.search = SearchFacade(self.quotes, self.authors, self.categories, self.tags, self.session)
        self.user = UserFacade(self.users, self.quotes, self.session)
        
        # Initialize builders
        self.quote_builder = lambda: QuoteBuilder(self.session)
        self.user_builder = lambda: UserBuilder(self.session)

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