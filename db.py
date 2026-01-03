from models import user_quotes, user_authors
from utilities import Validator
from typing import Optional, Dict, Any, Union, Tuple
from sqlalchemy import func, and_, or_, select, desc, asc
from sqlalchemy.orm import joinedload, selectinload
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
        self.validator = Validator(session)

    def _eager_load(self, query):
        """Apply eager loading based on model type"""
        from sqlalchemy.orm import joinedload, selectinload
        
        if self.model.__name__ == 'Quote':
            return query.options(
                joinedload(Quote.author),
                selectinload(Quote.tags),
                selectinload(Quote.categories)
            )
        elif self.model.__name__ == 'Author':
            return query.options(
                selectinload(Author.quotes),
                selectinload(Author.tags),
                selectinload(Author.users)
            )
        elif self.model.__name__ == 'Tag':
            return query.options(
                selectinload(Tag.quotes),
                selectinload(Tag.authors),
                selectinload(Tag.users)
            )
        elif self.model.__name__ == 'Category':
            return query.options(
                selectinload(Category.quotes)
            )
        elif self.model.__name__ == 'User':
            return query.options(
                selectinload(User.quotes),
                selectinload(User.authors),
                selectinload(User.tags)
            )
        else:
            return query


    def add(self, obj: T) -> T:
        """Add to the session and commit"""
        self.session.add(obj)
        self.session.commit()
        return obj
    
    def create(self, **kwargs) -> T:
        """Create and validate an object - works for all models"""
        # Create the object
        obj = self.model(**kwargs)
        
        # Validate and sanitize
        validated_obj = self.validator.validate(obj)
        
        if validated_obj is False:
            raise DuplicateError(f"{self.model.__name__} already exists")
        
        # Persist
        self.session.add(validated_obj)
        self.session.commit()
        return validated_obj

    def update(self, id: int, **kwargs) -> T:
        """Update an object with validation"""
        # Get the object
        obj = self.get(id)
        if not obj:
            raise NotFoundError(f"{self.model.__name__} with ID {id} not found")
        
        # Update attributes
        for key, value in kwargs.items():
            if hasattr(obj, key):
                setattr(obj, key, value)
        
        # Validate the updated object
        validated_obj = self.validator.validate(obj, exclude_id=id)
        
        if validated_obj is False:
            raise DuplicateError(f"{self.model.__name__} with these values already exists")
        
        # Commit the changes
        self.session.commit()
        return validated_obj        

    def get(self, id: int) -> T:
        """Get record by ID with eager loading"""
        query = self.session.query(self.model)
        query = self._eager_load(query)
        record = query.filter_by(id=id).first()
        
        assert_that(not record).raiseNotFoundError(f"{self.model.__name__} {id} not found")
        return record

    def all(self) -> List[T]:
        """Get all records with eager loading"""
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.all()


    def delete(self, id: int) -> bool:
        """Delete a record - removes associations first"""
        try:
            record = self.get(id)
            if record:
                # Delete many-to-many associations first (fast)
                if hasattr(record, 'tags'):
                    record.tags.clear()
                if hasattr(record, 'categories'):
                    record.categories.clear()
                if hasattr(record, 'users'):
                    record.users.clear()
                
                # Now delete the record
                self.session.delete(record)
                self.session.commit()
                return True
            return False
        except Exception as e:
            self.session.rollback()
            raise e

    def count(self) -> int:
        """Get total count"""
        return self.session.query(self.model).count()

    def filter_by(self, **kwargs) -> List[T]:
        """Filter records by attributes with eager loading"""
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.filter_by(**kwargs).all()

    def search(self, query: str) -> List[T]:
        """Search records with eager loading"""
        search_field = self.model.text if self.model.__name__ == 'Quote' else self.model.name
        query_obj = self.session.query(self.model)
        query_obj = self._eager_load(query_obj)
        return query_obj.filter(search_field.ilike(f"%{query}%")).all()

    def by_name(self, name: str) -> Optional[T]:
        """Get record by name with eager loading"""
        assert_that(self.model.__name__ == 'Quote').raiseNotImplementedError("Quote doesn't have a name attribute")
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.filter_by(name=name).first()

    def by_tag(self, tag_id: int) -> List[T]:
        """Get quotes, authors, users by tag with eager loading"""
        assert_that(self.model.__name__ == 'Category' or self.model.__name__ == 'Tag').raiseNotImplementedError(f"{self.model.__name__} doesn't have tags")
        tag = self.session.query(Tag).get(tag_id)
        assert_that(not tag).raiseNotFoundError(f"Tag {tag_id} not found")
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.where(self.model.tags.any(Tag.id == tag_id)).all()

    def by_user(self, user_id: int) -> List[T]:
        """Get quotes, authors, tags by user with eager loading"""
        assert_that(self.model.__name__ == 'Category' or self.model.__name__ == 'User').raiseNotImplementedError(f"{self.model.__name__} doesn't have users")
        user = self.session.query(User).get(user_id)
        assert_that(not user).raiseNotFoundError(f"User {user_id} not found")
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.where(self.model.users.any(User.id == user_id)).all()

    def get_needs_review(self) -> List[T]:
        """Get records that need review with eager loading"""
        assert_that(self.model.__name__ == 'Category').raiseNotImplementedError("Category doesn't have needs_review attribute")
        query = self.session.query(self.model)
        query = self._eager_load(query)
        return query.filter_by(needs_review=True).all()






    # def filter_by(self, **kwargs) -> List[T]:
    #     """Filter records by attributes"""
    #     return self.session.query(self.model).filter_by(**kwargs).all()

    # def search(self, query: str) -> List[T]:
    #     """Search records"""
    #     search_field = self.model.text if self.model.__name__ == 'Quote' else self.model.name
    #     return self.session.query(self.model).filter(search_field.ilike(f"%{query}%")).all()

    # def by_name(self, name: str) -> Optional[T]:
    #     """Get record by name"""
    #     assert_that(self.model.__name__ == 'Quote').raiseNotImplementedError("Quote doesn't have a name attribute")
    #     return self.session.query(self.model).filter_by(name=name).first()
    
    # def by_tag(self, tag_id: int) -> List[T]:
    #     """Get quotes, authors, users by tag"""
    #     assert_that(self.model.__name__ == 'Category').raiseNotImplementedError(f"{self.model.__name__} doesn't have tags")
    #     assert_that(self.model.__name__ == 'Tag').raiseNotImplementedError(f"{self.model.__name__} doesn't have tags")
    #     tag = self.session.query(Tag).get(tag_id)
    #     assert_that(not tag).raiseNotFoundError(f"Tag {tag_id} not found")
    #     return self.session.query(self.model).where(self.model.tags.any(Tag.id == tag_id)).all()

    # def by_user(self, user_id: int) -> List[T]:
    #     """Get quotes, authors, tags by user"""
    #     assert_that(self.model.__name__ == 'Category').raiseNotImplementedError(f"{self.model.__name__} doesn't have users")
    #     assert_that(self.model.__name__ == 'User').raiseNotImplementedError(f"{self.model.__name__} doesn't have users")
    #     user = self.session.query(User).get(user_id)
    #     assert_that(not user).raiseNotFoundError(f"User {user_id} not found")
    #     return self.session.query(self.model).where(self.model.users.any(User.id == user_id)).all()

    # def get_needs_review(self) -> List[T]:
    #     """Get records that need review"""
    #     assert_that(self.model.__name__ == 'Category').raiseNotImplementedError("Category doesn't have needs_review attribute")
    #     return self.session.query(self.model).filter_by(needs_review=True).all()

class QuoteRepository(Repository[Quote]):
    """Quote-specific repository"""

    def by_author(self, author_id: int) -> List[Quote]:
        """Get quotes by author"""
        return self.filter_by(author_id=author_id)
        
    def by_category(self, category_id: int) -> List[Quote]:
        """Get quotes in a category"""
        category = self.session.query(Category).get(category_id)
        assert_that(not category).raiseNotFoundError(f"Category {category_id} not found")
        return category.quotes

    def get_quotes_without_author(self) -> List[Quote]:
        """Get all quotes without an author"""
        return self.session.query(self.model).filter(self.model.author_id.is_(None)).all()

class UserRepository(Repository[User]):
    """User-specific repository"""
    
    def create(self, name: str, email: str, password: str) -> User:
        """Create a user with password handling"""
        user = User(name=name, email=email)
        user.set_password(password)  # Special handling
        
        # Then validate
        validated_user = self.validator.validate(user)
        
        if validated_user is False:
            raise DuplicateError("User with this name or email already exists")
        
        self.session.add(validated_user)
        self.session.commit()
        return validated_user
    
    def update(self, id: int, **kwargs) -> User:
        """Update a user with special password handling"""
        # Get the user
        user = self.get(id)
        if not user:
            raise NotFoundError(f"User with ID {id} not found")
        
        # Special handling for password
        if 'password' in kwargs:
            password = kwargs.pop('password')
            user.set_password(password)
        
        # Update other attributes
        for key, value in kwargs.items():
            if hasattr(user, key):
                setattr(user, key, value)
        
        # Validate the updated object
        validated_user = self.validator.validate(user)
        
        if validated_user is False:
            raise DuplicateError("User with this name or email already exists")
        
        # Commit changes
        self.session.commit()
        return validated_user

    def by_email(self, email: str) -> Optional[User]:
        """Get user by email"""
        return self.session.query(User).filter_by(email=email).first()

    def by_quote(self, quote_id: int) -> List[User]:
        """Get users by quote"""
        quote = self.session.query(Quote).get(quote_id)
        assert_that(not quote).raiseNotFoundError(f"Quote {quote_id} not found")
        return quote.users
    
    def by_author(self, author_id: int) -> List[User]:
        """Get users by author"""
        author = self.session.query(Author).get(author_id)
        assert_that(not author).raiseNotFoundError(f"Author {author_id} not found")
        return author.users

class AuthorRepository(Repository[Author]):
    """Author-specific repository"""
        
class CategoryRepository(Repository[Category]):
    """Category-specific repository"""
      
class TagRepository(Repository[Tag]):
    """Tag-specific repository"""





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
    
    def tags(self, query: str) -> List[Category]:
        """Search tags"""
        return self._tags.search(query)

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




