from models import user_quotes, user_authors
from typing import  Dict, Any, Union, Tuple
from sqlalchemy import func, and_, or_, select, desc, asc
from datetime import datetime
import re

from models import Session, Quote, Author, Category, User, Tag
from typing import Optional, List, TypeVar, Generic, Type
from abc import ABC, abstractmethod
from errors import NotFoundError, ValidationError, DatabaseError, DuplicateError

T = TypeVar('T')



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


# class DB:
#     """Database access layer"""

#     def __init__(self):
#         self.session = Session()
#         self.quotes = QuoteManager(self.session)
#         self.users = UserManager(self.session)
#         self.authors = AuthorManager(self.session)
#         self.categories = CategoryManager(self.session)
#         self.tags = TagManager(self.session)

#     def enter(self):
#         """Enter the context manager"""
#         return self

#     def exit(self, exc_type, exc_value, traceback):
#         """Exit the context manager"""
#         if exc_type:
#             self.session.rollback()
#         else:
#             self.session.commit()
#         self.session.close()

#     def commit(self):
#         """Commit the current transaction"""
#         try:
#             self.session.commit()
#         except Exception as e:
#             self.session.rollback()
#             raise DatabaseError(f"Database commit failed: {str(e)}")

#     def rollback(self):
#         """Rollback the current transaction"""
#         self.session.rollback()

#     def close(self):
#         """Close the session"""
#         self.session.close()



# ============================================================================
# REPOSITORY PATTERN
# ============================================================================

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
        if not record:
            raise NotFoundError(f"{self.model.__name__} {id} not found")
        return record
    
    def delete(self, id: int) -> bool:
        """Delete a record"""
        record = self.get(id)
        self.session.delete(record)
        return True
    
    def filter_by(self, **kwargs) -> List[T]:
        """Filter records by attributes"""
        return self.session.query(self.model).filter_by(**kwargs).all()


class QuoteRepository(Repository[Quote]):
    """Quote-specific repository"""
    
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
    
    def by_author(self, author_id: int) -> List[Quote]:
        """Get quotes by author"""
        return self.filter_by(author_id=author_id)
    
    def search(self, query: str) -> List[Quote]:
        """Search quotes by text"""
        return self.session.query(Quote).filter(Quote.text.ilike(f"%{query}%")).all()
    
    def by_tag(self, tag_id: int) -> List[Quote]:
        """Get quotes with a tag"""
        tag = self.session.query(Tag).get(tag_id)
        if not tag:
            raise NotFoundError(f"Tag {tag_id} not found")
        return tag.quotes
    
    def by_category(self, category_id: int) -> List[Quote]:
        """Get quotes in a category"""
        category = self.session.query(Category).get(category_id)
        if not category:
            raise NotFoundError(f"Category {category_id} not found")
        return category.quotes


class UserRepository(Repository[User]):
    """User-specific repository"""
    
    def create(self, username: str, email: str, password: str) -> User:
        """Create a new user"""
        if not username or len(username.strip()) < 3:
            raise ValidationError("Username must be at least 3 characters")
        if not email or '@' not in email:
            raise ValidationError("Invalid email address")
        
        existing = self.session.query(User).filter(
            (User.username == username) | (User.email == email)
        ).first()
        if existing:
            raise DuplicateError("Username or email already exists")
        
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
    
    def by_username(self, username: str) -> Optional[User]:
        """Get user by username"""
        return self.session.query(User).filter_by(username=username).first()
    
    def by_email(self, email: str) -> Optional[User]:
        """Get user by email"""
        return self.session.query(User).filter_by(email=email).first()


class AuthorRepository(Repository[Author]):
    """Author-specific repository"""
    
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
    
    def by_name(self, name: str) -> Optional[Author]:
        """Get author by name"""
        return self.session.query(Author).filter_by(name=name).first()
    
    def search(self, query: str) -> List[Author]:
        """Search authors by name"""
        return self.session.query(Author).filter(Author.name.ilike(f"%{query}%")).all()


class CategoryRepository(Repository[Category]):
    """Category-specific repository"""
    
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
    
    def by_name(self, name: str) -> Optional[Category]:
        """Get category by name"""
        return self.session.query(Category).filter_by(name=name).first()


class TagRepository(Repository[Tag]):
    """Tag-specific repository"""
    
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
    
    def by_name(self, name: str) -> Optional[Tag]:
        """Get tag by name"""
        return self.session.query(Tag).filter_by(name=name).first()
    
    def search(self, query: str) -> List[Tag]:
        """Search tags by name"""
        return self.session.query(Tag).filter(Tag.name.ilike(f"%{query}%")).all()


# ============================================================================
# FACADE PATTERN
# ============================================================================

class SearchFacade:
    """Advanced search interface across all models"""
    
    def __init__(self, quotes: QuoteRepository, authors: AuthorRepository, 
                 tags: TagRepository, session: Session):
        self.quotes = quotes
        self.authors = authors
        self.tags = tags
        self.session = session
    
    def search_all(self, query: str) -> dict:
        """Search across all models"""
        return {
            'quotes': self.quotes.search(query),
            'authors': self.authors.search(query),
            'tags': self.tags.search(query),
        }
    
    def search_quotes(self, query: str) -> List[Quote]:
        """Search only quotes"""
        return self.quotes.search(query)
    
    def search_authors(self, query: str) -> List[Author]:
        """Search only authors"""
        return self.authors.search(query)
    
    def search_tags(self, query: str) -> List[Tag]:
        """Search only tags"""
        return self.tags.search(query)
    
    def search_quotes_by_author(self, author_name: str, quote_text: str = None) -> List[Quote]:
        """Search quotes by author name, optionally with quote text"""
        author = self.authors.by_name(author_name)
        if not author:
            return []
        
        _quotes = author.quotes
        if quote_text:
            _quotes = [q for q in _quotes if quote_text.lower() in q.text.lower()]
        return _quotes
    
    def search_quotes_by_tags(self, tag_names: List[str], match_all: bool = False) -> List[Quote]:
        """Search quotes by multiple tags
        
        Args:
            tag_names: List of tag names to search for
            match_all: If True, quotes must have ALL tags. If False, quotes with ANY tag.
        """
        _tags = [self.tags.by_name(name) for name in tag_names]
        _tags = [t for t in _tags if t is not None]
        
        if not _tags:
            return []
        
        if match_all:
            # Quotes that have ALL specified tags
            _quotes = set(_tags[0].quotes)
            for tag in _tags[1:]:
                _quotes = _quotes.intersection(set(tag.quotes))
            return list(_quotes)
        else:
            # Quotes that have ANY of the specified tags
            _quotes = set()
            for tag in _tags:
                _quotes.update(tag.quotes)
            return list(_quotes)
    
    def search_quotes_by_categories(self, category_names: List[str], match_all: bool = False) -> List[Quote]:
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
    
    def advanced_search(self, text: str = None, author: str = None, 
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
            results = set(self.quotes.search(text))
        
        # Filter by author if provided
        if author:
            author_quotes = set(self.search_quotes_by_author(author))
            results = results.intersection(author_quotes) if results else author_quotes
        
        # Filter by tags if provided
        if tags:
            tag_quotes = set(self.search_quotes_by_tags(tags, match_all=match_all_tags))
            results = results.intersection(tag_quotes) if results else tag_quotes
        
        # Filter by categories if provided
        if categories:
            cat_quotes = set(self.search_quotes_by_categories(categories, match_all=match_all_categories))
            results = results.intersection(cat_quotes) if results else cat_quotes
        
        return list(results) if results else []
    
    def search_by_multiple_terms(self, terms: List[str]) -> dict:
        """Search across multiple terms and aggregate results"""
        results = {
            'quotes': set(),
            'authors': set(),
            'tags': set(),
        }
        
        for term in terms:
            results['quotes'].update(self.quotes.search(term))
            results['authors'].update(self.authors.search(term))
            results['tags'].update(self.tags.search(term))
        
        return {
            'quotes': list(results['quotes']),
            'authors': list(results['authors']),
            'tags': list(results['tags']),
        }
    
    def search_authors_with_quotes(self, query: str) -> List[dict]:
        """Search authors and return their quotes
        
        Returns list of dicts with author and their matching quotes
        """
        _authors = self.authors.search(query)
        return [
            {
                'author': author,
                'quotes': author.quotes,
                'quote_count': len(author.quotes),
            }
            for author in _authors
        ]


class UserProfileFacade:
    """Simplified interface for user profile operations"""
    
    def __init__(self, users: UserRepository, quotes: QuoteRepository):
        self.users = users
        self.quotes = quotes
    
    def get_profile(self, user_id: int) -> dict:
        """Get complete user profile"""
        user = self.users.get(user_id)
        return {
            'user': user,
            'quotes': user.quotes,
            'authors': user.authors,
            'tags': user.tags,
            'quote_count': user.quote_count,
            'author_count': user.author_count,
            'tag_count': user.tag_count,
        }
    
    def add_favorite_quote(self, user_id: int, quote_id: int) -> bool:
        """Add a quote to user's favorites"""
        user = self.users.get(user_id)
        quote = self.quotes.get(quote_id)
        if quote not in user.quotes:
            user.quotes.append(quote)
            return True
        return False
    
    def remove_favorite_quote(self, user_id: int, quote_id: int) -> bool:
        """Remove a quote from user's favorites"""
        user = self.users.get(user_id)
        quote = self.quotes.get(quote_id)
        if quote in user.quotes:
            user.quotes.remove(quote)
            return True
        return False


# ============================================================================
# BUILDER PATTERN
# ============================================================================

class QuoteBuilder:
    """Build quotes with optional fields"""
    
    def __init__(self, session: Session):
        self.session = session
        self._text: Optional[str] = None
        self._author: Optional[Author] = None
        self._tags: List[Tag] = []
        self._categories: List[Category] = []
    
    def with_text(self, text: str) -> 'QuoteBuilder':
        """Set quote text"""
        if not text or len(text.strip()) == 0:
            raise ValidationError("Quote text cannot be empty")
        self._text = text
        return self
    
    def with_author(self, author: Author) -> 'QuoteBuilder':
        """Set author"""
        self._author = author
        return self
    
    def with_author_id(self, author_id: int) -> 'QuoteBuilder':
        """Set author by ID"""
        author = self.session.query(Author).get(author_id)
        if not author:
            raise NotFoundError(f"Author {author_id} not found")
        self._author = author
        return self
    
    def add_tag(self, tag: Tag) -> 'QuoteBuilder':
        """Add a single tag"""
        if tag not in self._tags:
            self._tags.append(tag)
        return self
    
    def add_tags(self, tags: List[Tag]) -> 'QuoteBuilder':
        """Add multiple tags"""
        for tag in tags:
            self.add_tag(tag)
        return self
    
    def add_category(self, category: Category) -> 'QuoteBuilder':
        """Add a single category"""
        if category not in self._categories:
            self._categories.append(category)
        return self
    
    def add_categories(self, categories: List[Category]) -> 'QuoteBuilder':
        """Add multiple categories"""
        for category in categories:
            self.add_category(category)
        return self
    
    def build(self) -> Quote:
        """Build and return the quote"""
        if not self._text:
            raise ValidationError("Quote text is required")
        if not self._author:
            raise ValidationError("Author is required")
        
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
        self._username: Optional[str] = None
        self._email: Optional[str] = None
        self._password: Optional[str] = None
        self._tags: List[Tag] = []
    
    def with_username(self, username: str) -> 'UserBuilder':
        """Set username"""
        if not username or len(username.strip()) < 3:
            raise ValidationError("Username must be at least 3 characters")
        self._username = username
        return self
    
    def with_email(self, email: str) -> 'UserBuilder':
        """Set email"""
        if '@' not in email:
            raise ValidationError("Invalid email address")
        self._email = email
        return self
    
    def with_password(self, password: str) -> 'UserBuilder':
        """Set password"""
        if not password or len(password) < 6:
            raise ValidationError("Password must be at least 6 characters")
        self._password = password
        return self
    
    def add_tag(self, tag: Tag) -> 'UserBuilder':
        """Add a tag to the user"""
        if tag not in self._tags:
            self._tags.append(tag)
        return self
    
    def build(self) -> User:
        """Build and return the user"""
        if not self._username or not self._email or not self._password:
            raise ValidationError("Username, email, and password are required")
        
        existing = self.session.query(User).filter(
            (User.username == self._username) | (User.email == self._email)
        ).first()
        if existing:
            raise DuplicateError("Username or email already exists")
        
        user = User(username=self._username, email=self._email)
        user.set_password(self._password)
        
        for tag in self._tags:
            user.tags.append(tag)
        
        self.session.add(user)
        return user


# ============================================================================
# DATABASE LAYER
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
        self.search = SearchFacade(self.quotes, self.authors, self.tags, self.session)
        self.user_profile = UserProfileFacade(self.users, self.quotes)
        
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