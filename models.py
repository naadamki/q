from sqlalchemy import create_engine, Column, Integer, String, Text, Table, ForeignKey, Boolean, DateTime, JSON
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import relationship, sessionmaker, Mapped, mapped_column, DeclarativeBase
from datetime import datetime
from werkzeug.security import generate_password_hash, check_password_hash
from typing import Optional, List, Union
import json

Base = declarative_base()

# Many-to-many relationship table for quotes and categories
quote_categories = Table('quote_categories', Base.metadata,
    Column('quote_id', Integer, ForeignKey('quotes.id')),
    Column('category_id', Integer, ForeignKey('categories.id'))
)

# Many-to-many relationship table for users and quotes
user_quotes = Table('user_quotes', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('quote_id', Integer, ForeignKey('quotes.id')),
)

# Many-to-many relationship table for users and authors
user_authors = Table('user_authors', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('author_id', Integer, ForeignKey('authors.id')),
)

# Many-to-many relationship tables for tags
quote_tags = Table('quote_tags', Base.metadata,
    Column('quote_id', Integer, ForeignKey('quotes.id')),
    Column('tag_id', Integer, ForeignKey('tags.id')),
)

author_tags = Table('author_tags', Base.metadata,
    Column('author_id', Integer, ForeignKey('authors.id')),
    Column('tag_id', Integer, ForeignKey('tags.id')),
)

user_tags = Table('user_tags', Base.metadata,
    Column('user_id', Integer, ForeignKey('users.id')),
    Column('tag_id', Integer, ForeignKey('tags.id')),
)


class User(Base):
    __tablename__ = 'users'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    username: Mapped[str] = mapped_column(String(80), unique=True, nullable=False)
    email: Mapped[str] = mapped_column(String(120), unique=True, nullable=False)
    password_hash: Mapped[str] = mapped_column(String(255), nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    last_login: Mapped[Optional[datetime]] = mapped_column(DateTime)
    is_active: Mapped[bool] = mapped_column(Boolean, default=True)

    quotes: Mapped[List['Quote']] = relationship('Quote', secondary=user_quotes, back_populates='users')
    authors: Mapped[List['Author']] = relationship('Author', secondary=user_authors, back_populates='users')
    tags: Mapped[List['Tag']] = relationship('Tag', secondary=user_tags, back_populates='users')

    @property
    def quote_count(self) -> int:
        """Get total number of favorite quotes"""
        return len(self.quotes)

    @property
    def author_count(self) -> int:
        """Get total number of favorite authors"""
        return len(self.authors)

    @property
    def tag_count(self) -> int:
        """Get total number of tags"""
        return len(self.tags)

    @property
    def all(self) -> List[Union['Quote', 'Author', 'Tag']]:
        """Get all related quotes, authors, and tags"""
        return list(self.quotes) + list(self.authors) + list(self.tags)

    def __repr__(self) -> str:
        return f"<User(id={self.id}, username='{self.username}')>"

    def set_password(self, password: str) -> None:
        """Hash and set the user's password"""
        if not password or len(password) < 6:
            raise ValueError("Password must be at least 6 characters")
        self.password_hash = generate_password_hash(password)
    
    def check_password(self, password: str) -> bool:
        """Check if the provided password matches the hash"""
        return check_password_hash(self.password_hash, password)



class Author(Base):
    __tablename__ = 'authors'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(200), unique=True, nullable=False)

    quotes: Mapped[List['Quote']] = relationship('Quote', back_populates='author')
    users: Mapped[List['User']] = relationship('User', secondary=user_authors, back_populates='authors')
    tags: Mapped[List['Tag']] = relationship('Tag', secondary=author_tags, back_populates='authors')
    
    @property
    def quote_count(self) -> int:
        """Get how many quotes this author has"""
        return len(self.quotes)

    @property
    def user_count(self) -> int:
        """Get how many users have this author"""
        return len(self.users)

    @property
    def tag_count(self) -> int:
        """Get total number of tags"""
        return len(self.tags)

    @property
    def all(self) -> List[Union['Quote', 'Tag']]:
        """Get all related quotes and tags"""
        return list(self.quotes) + list(self.tags)

    def __repr__(self) -> str:
        return f"<Author(id={self.id}, name='{self.name}')>"


class Category(Base):
    __tablename__ = 'categories'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(50), unique=True, nullable=False)
    keywords: Mapped[Optional[str]] = mapped_column(Text)  # Stores JSON array

    quotes: Mapped[List['Quote']] = relationship('Quote', secondary=quote_categories, back_populates='categories')

    @property
    def quote_count(self) -> int:
        """Get how many quotes are in this category"""
        return len(self.quotes)

    @property
    def all(self) -> List['Quote']:
        """Get all quotes in this category"""
        return self.quotes

    def __repr__(self) -> str:
        return f"<Category(id={self.id}, name='{self.name}')>"

    def list_keywords(self) -> List[str]:
        """Return keywords as a list"""
        if self.keywords:
            return json.loads(self.keywords)
        return []

    def add_keywords(self, keywords: List[str]) -> None:
        """Add keywords to the existing list"""
        current = self.list_keywords()
        current.extend(keywords)
        self.keywords = json.dumps(current)

    def set_keywords(self, keywords: List[str]) -> None:
        """Replace all keywords with a new list"""
        self.keywords = json.dumps(keywords)


class Quote(Base):
    __tablename__ = 'quotes'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    text: Mapped[str] = mapped_column(Text, nullable=False)
    author_id: Mapped[int] = mapped_column(Integer, ForeignKey('authors.id'), nullable=False)
    tag_list: Mapped[Optional[str]] = mapped_column(Text) 

    # Relationships
    author: Mapped['Author'] = relationship('Author', back_populates='quotes')
    categories: Mapped[List['Category']] = relationship('Category', secondary=quote_categories, back_populates='quotes')
    users: Mapped[List['User']] = relationship('User', secondary=user_quotes, back_populates='quotes')
    tags: Mapped[List['Tag']] = relationship('Tag', secondary=quote_tags, back_populates='quotes')

    @property
    def user_count(self) -> int:
        """Get how many users have favorited this quote"""
        return len(self.users)

    @property
    def category_count(self) -> int:
        """Get total number of categories"""
        return len(self.categories)

    @property
    def all(self) -> List[Union['Category', 'Tag']]:
        """Get all related categories and tags"""
        return list(self.categories) + list(self.tags)

    def __repr__(self) -> str:
        author_name = self.author.name if self.author else "Unknown"
        text_preview = self.text[:30] + "..." if len(self.text) > 30 else self.text
        text_preview = text_preview.replace('\n', ' ').replace('\r', ' ')
        return f"<Quote(id={self.id}, author='{author_name}', text='{text_preview}')>"



class Tag(Base):
    __tablename__ = 'tags'

    id: Mapped[int] = mapped_column(Integer, primary_key=True)
    name: Mapped[str] = mapped_column(String(100), unique=True, nullable=False)
    created_at: Mapped[datetime] = mapped_column(DateTime, default=datetime.utcnow)
    
    # Polymorphic relationships - tag can be applied to many of each type
    quotes: Mapped[List['Quote']] = relationship('Quote', secondary=quote_tags, back_populates='tags')
    authors: Mapped[List['Author']] = relationship('Author', secondary=author_tags, back_populates='tags')
    users: Mapped[List['User']] = relationship('User', secondary=user_tags, back_populates='tags')

    @property 
    def quote_count(self) -> int:
        """Get total number of quotes"""
        return len(self.quotes)

    @property
    def author_count(self) -> int:
        """Get total number of authors"""
        return len(self.authors)

    @property
    def user_count(self) -> int:
        """Get total number of users"""
        return len(self.users)

    @property
    def total_count(self) -> int:
        """Get total number of items this tag is applied to"""
        return self.quote_count + self.author_count + self.user_count

    @property
    def all(self) -> List[Union['Quote', 'Author', 'User']]:
        """Get all items (quotes, authors, users) this tag is applied to"""
        return list(self.quotes) + list(self.authors) + list(self.users)

    def __repr__(self) -> str:
        return f"<Tag(id={self.id}, name='{self.name}')>"



# Database setup
engine = create_engine('sqlite:///quotes.db', echo=False)
Base.metadata.create_all(engine)
Session = sessionmaker(bind=engine)