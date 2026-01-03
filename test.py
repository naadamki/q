"""
DAO, CQRS, Query Object, and Specification Patterns
Different architectural approaches to data access and query design
"""

from sqlalchemy.orm import Session
from typing import List, Optional, Generic, TypeVar, Protocol
from dataclasses import dataclass
from datetime import datetime
from models import Quote, Author, Tag, Category

T = TypeVar('T')


# ============================================================================
# 1. DAO PATTERN (Data Access Object)
# ============================================================================
"""
DAO Pattern Key Differences from Repository:
- More FOCUSED on database-specific details
- Less about CRUD generics, more about specific data access
- Often has multiple DAOs working together
- Doesn't try to be a generic interface (no BaseDAO)
- Emphasizes encapsulating HOW data is accessed, not WHAT operations are available
- Often returns primitive types or DTOs instead of domain objects

Repository: "Give me all quotes with tags"
DAO: "Execute this specific SQL optimized for this use case"
"""

@dataclass
class QuoteDTO:
    """Data Transfer Object - what the DAO returns"""
    id: int
    text: str
    author_name: str
    tag_names: List[str]
    needs_review: bool


class QuoteDAO:
    """
    DAO focused on how to access quote data
    Returns DTOs optimized for specific use cases
    """
    
    def __init__(self, session: Session):
        self.session = session
    
    def fetch_quote_detail(self, quote_id: int) -> Optional[QuoteDTO]:
        """
        Fetch quote with all details in ONE optimized query
        Returns a DTO, not a domain object
        """
        from sqlalchemy import select, joinedload
        
        quote = self.session.query(Quote).options(
            joinedload(Quote.author),
            joinedload(Quote.tags)
        ).filter(Quote.id == quote_id).first()
        
        if not quote:
            return None
        
        return QuoteDTO(
            id=quote.id,
            text=quote.text,
            author_name=quote.author.name,
            tag_names=[tag.name for tag in quote.tags],
            needs_review=quote.needs_review
        )
    
    def fetch_quotes_for_display(self, page: int, per_page: int) -> tuple[List[QuoteDTO], int]:
        """
        Fetch quotes optimized for display/list pages
        Returns only what's needed for UI
        """
        query = self.session.query(Quote).options(
            joinedload(Quote.author)
        )
        
        total = query.count()
        quotes = query.offset((page - 1) * per_page).limit(per_page).all()
        
        dtos = [
            QuoteDTO(
                id=q.id,
                text=q.text[:100] + "..." if len(q.text) > 100 else q.text,
                author_name=q.author.name,
                tag_names=[],  # Don't load tags for list view (expensive)
                needs_review=q.needs_review
            )
            for q in quotes
        ]
        
        return dtos, total
    
    def fetch_quotes_by_author_efficient(self, author_id: int) -> List[QuoteDTO]:
        """
        Get quotes by author - optimized specific query
        Uses raw SQL if needed for performance
        """
        quotes = self.session.query(Quote).filter(
            Quote.author_id == author_id
        ).all()
        
        return [
            QuoteDTO(
                id=q.id,
                text=q.text,
                author_name=q.author.name,
                tag_names=[tag.name for tag in q.tags],
                needs_review=q.needs_review
            )
            for q in quotes
        ]
    
    def fetch_review_queue(self) -> List[QuoteDTO]:
        """
        Get all quotes needing review
        Optimized for admin/review interface
        """
        quotes = self.session.query(Quote).filter(
            Quote.needs_review == True
        ).order_by(Quote.id).all()
        
        return [
            QuoteDTO(
                id=q.id,
                text=q.text,
                author_name=q.author.name,
                tag_names=[tag.name for tag in q.tags],
                needs_review=True
            )
            for q in quotes
        ]


# Usage:
# dao = QuoteDAO(session)
# quote_dto = dao.fetch_quote_detail(1)
# quotes, total = dao.fetch_quotes_for_display(page=1, per_page=50)


# ============================================================================
# 2. QUERY OBJECT PATTERN
# ============================================================================
"""
Query Object Pattern:
- Encapsulate queries as objects instead of methods
- Build complex queries by composing query objects
- Makes queries reusable and testable
- Separates query logic from repositories/services
"""

class QueryObject(Generic[T]):
    """Base query object"""
    
    def __init__(self, session: Session, model: type[T]):
        self.session = session
        self.model = model
        self._query = session.query(model)
    
    def build(self):
        """Get the underlying SQLAlchemy query"""
        return self._query
    
    def execute(self) -> List[T]:
        """Execute the query"""
        return self._query.all()
    
    def execute_single(self) -> Optional[T]:
        """Execute and get first result"""
        return self._query.first()
    
    def count(self) -> int:
        """Count results"""
        return self._query.count()


class QuoteQuery(QueryObject[Quote]):
    """Reusable quote queries"""
    
    def by_author(self, author_id: int) -> 'QuoteQuery':
        """Filter by author"""
        self._query = self._query.filter(Quote.author_id == author_id)
        return self
    
    def with_tag(self, tag_name: str) -> 'QuoteQuery':
        """Filter by tag"""
        self._query = self._query.join(Quote.tags).filter(Tag.name == tag_name)
        return self
    
    def search_text(self, text: str) -> 'QuoteQuery':
        """Search text"""
        self._query = self._query.filter(Quote.text.ilike(f"%{text}%"))
        return self
    
    def needs_review(self) -> 'QuoteQuery':
        """Filter to quotes needing review"""
        self._query = self._query.filter(Quote.needs_review == True)
        return self
    
    def with_author_eager(self) -> 'QuoteQuery':
        """Eager load author"""
        from sqlalchemy.orm import joinedload
        self._query = self._query.options(joinedload(Quote.author))
        return self
    
    def with_tags_eager(self) -> 'QuoteQuery':
        """Eager load tags"""
        from sqlalchemy.orm import selectinload
        self._query = self._query.options(selectinload(Quote.tags))
        return self
    
    def paginate(self, page: int, per_page: int) -> dict:
        """Paginate results"""
        total = self._query.count()
        results = self._query.offset((page - 1) * per_page).limit(per_page).all()
        
        return {
            'data': results,
            'total': total,
            'page': page,
            'per_page': per_page,
            'total_pages': (total + per_page - 1) // per_page
        }
    
    def limit_results(self, limit: int) -> 'QuoteQuery':
        """Limit results"""
        self._query = self._query.limit(limit)
        return self


# Usage:
# query = QuoteQuery(session, Quote)
# results = (query
#     .by_author(1)
#     .with_tag("inspirational")
#     .search_text("success")
#     .with_author_eager()
#     .with_tags_eager()
#     .execute())
#
# paginated = (QuoteQuery(session, Quote)
#     .needs_review()
#     .with_author_eager()
#     .paginate(page=1, per_page=50))


# ============================================================================
# 3. SPECIFICATION PATTERN
# ============================================================================
"""
Specification Pattern:
- Define reusable business rules/filters as objects
- Compose specifications together
- Makes complex business logic explicit and testable
- Each specification is a self-contained filter
"""

class Specification(Generic[T]):
    """Base specification"""
    
    def to_predicate(self, model: type[T]):
        """Convert specification to SQLAlchemy filter"""
        raise NotImplementedError
    
    def and_spec(self, other: 'Specification[T]') -> 'AndSpecification[T]':
        """Combine with AND"""
        return AndSpecification(self, other)
    
    def or_spec(self, other: 'Specification[T]') -> 'OrSpecification[T]':
        """Combine with OR"""
        return OrSpecification(self, other)


class QuotesByAuthor(Specification[Quote]):
    """Specification: quotes by specific author"""
    
    def __init__(self, author_id: int):
        self.author_id = author_id
    
    def to_predicate(self, model=Quote):
        return Quote.author_id == self.author_id


class QuotesNeedingReview(Specification[Quote]):
    """Specification: quotes that need review"""
    
    def to_predicate(self, model=Quote):
        return Quote.needs_review == True


class QuotesWithTag(Specification[Quote]):
    """Specification: quotes with specific tag"""
    
    def __init__(self, tag_name: str):
        self.tag_name = tag_name
    
    def to_predicate(self, model=Quote):
        return Quote.tags.any(Tag.name == self.tag_name)


class QuotesMatchingText(Specification[Quote]):
    """Specification: quotes matching text"""
    
    def __init__(self, text: str):
        self.text = text
    
    def to_predicate(self, model=Quote):
        return Quote.text.ilike(f"%{self.text}%")


class AndSpecification(Specification[T]):
    """Combine two specifications with AND"""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def to_predicate(self, model: type[T]):
        from sqlalchemy import and_
        return and_(self.left.to_predicate(model), self.right.to_predicate(model))


class OrSpecification(Specification[T]):
    """Combine two specifications with OR"""
    
    def __init__(self, left: Specification[T], right: Specification[T]):
        self.left = left
        self.right = right
    
    def to_predicate(self, model: type[T]):
        from sqlalchemy import or_
        return or_(self.left.to_predicate(model), self.right.to_predicate(model))


class SpecificationQuery:
    """Execute specifications against the database"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def find_all(self, spec: Specification[T], model: type[T]) -> List[T]:
        """Find all matching the specification"""
        return self.session.query(model).filter(spec.to_predicate(model)).all()
    
    def find_one(self, spec: Specification[T], model: type[T]) -> Optional[T]:
        """Find first matching the specification"""
        return self.session.query(model).filter(spec.to_predicate(model)).first()
    
    def count(self, spec: Specification[T], model: type[T]) -> int:
        """Count matching the specification"""
        return self.session.query(model).filter(spec.to_predicate(model)).count()


# Usage:
# spec_query = SpecificationQuery(session)
#
# # Simple specification
# spec1 = QuotesByAuthor(1)
# quotes = spec_query.find_all(spec1, Quote)
#
# # Composed specifications (quotes by author 1 that need review)
# spec2 = QuotesNeedingReview()
# combined = spec1.and_spec(spec2)
# quotes = spec_query.find_all(combined, Quote)
#
# # Complex: quotes by author 1 that need review AND have "inspiration" tag
# complex_spec = (QuotesByAuthor(1)
#     .and_spec(QuotesNeedingReview())
#     .and_spec(QuotesWithTag("inspiration")))
# quotes = spec_query.find_all(complex_spec, Quote)


# ============================================================================
# 4. CQRS PATTERN (Command Query Responsibility Segregation)
# ============================================================================
"""
CQRS Pattern:
- Separate READ operations (queries) from WRITE operations (commands)
- Different models for reading vs writing
- Optimize reads and writes independently
- Useful for complex domains with different access patterns
"""

# ---- WRITE SIDE (Commands) ----

@dataclass
class CreateQuoteCommand:
    """Command to create a quote"""
    text: str
    author_id: int
    source: Optional[str] = None


@dataclass
class UpdateQuoteCommand:
    """Command to update a quote"""
    quote_id: int
    text: Optional[str] = None
    needs_review: Optional[bool] = None


@dataclass
class AssignTagsCommand:
    """Command to assign tags to a quote"""
    quote_id: int
    tag_names: List[str]


class CommandHandler:
    """Handles write operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def handle_create_quote(self, cmd: CreateQuoteCommand) -> Quote:
        """Execute create quote command"""
        quote = Quote(
            text=cmd.text,
            author_id=cmd.author_id,
            source=cmd.source
        )
        self.session.add(quote)
        self.session.commit()
        return quote
    
    def handle_update_quote(self, cmd: UpdateQuoteCommand) -> Quote:
        """Execute update quote command"""
        quote = self.session.query(Quote).filter(Quote.id == cmd.quote_id).first()
        
        if not quote:
            raise ValueError("Quote not found")
        
        if cmd.text is not None:
            quote.text = cmd.text
        if cmd.needs_review is not None:
            quote.needs_review = cmd.needs_review
        
        self.session.commit()
        return quote
    
    def handle_assign_tags(self, cmd: AssignTagsCommand) -> Quote:
        """Execute assign tags command"""
        quote = self.session.query(Quote).filter(Quote.id == cmd.quote_id).first()
        
        if not quote:
            raise ValueError("Quote not found")
        
        for tag_name in cmd.tag_names:
            tag = self.session.query(Tag).filter(Tag.name == tag_name).first()
            if not tag:
                tag = Tag(name=tag_name)
                self.session.add(tag)
            
            if tag not in quote.tags:
                quote.tags.append(tag)
        
        self.session.commit()
        return quote


# ---- READ SIDE (Queries) ----

@dataclass
class QuoteReadModel:
    """Read model - optimized for display"""
    id: int
    text: str
    author_name: str
    tags: List[str]
    created_at: datetime
    needs_review: bool


class QueryHandler:
    """Handles read operations"""
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_quote_detail(self, quote_id: int) -> Optional[QuoteReadModel]:
        """Get a quote for detail view"""
        from sqlalchemy.orm import joinedload
        
        quote = self.session.query(Quote).options(
            joinedload(Quote.author),
            joinedload(Quote.tags)
        ).filter(Quote.id == quote_id).first()
        
        if not quote:
            return None
        
        return QuoteReadModel(
            id=quote.id,
            text=quote.text,
            author_name=quote.author.name,
            tags=[tag.name for tag in quote.tags],
            created_at=quote.created_at,
            needs_review=quote.needs_review
        )
    
    def get_quotes_paginated(self, page: int, per_page: int) -> dict:
        """Get quotes for list view"""
        from sqlalchemy.orm import joinedload
        
        query = self.session.query(Quote).options(
            joinedload(Quote.author)
        )
        
        total = query.count()
        quotes = query.offset((page - 1) * per_page).limit(per_page).all()
        
        return {
            'data': [
                QuoteReadModel(
                    id=q.id,
                    text=q.text[:100],
                    author_name=q.author.name,
                    tags=[],
                    created_at=q.created_at,
                    needs_review=q.needs_review
                )
                for q in quotes
            ],
            'total': total,
            'page': page,
            'per_page': per_page
        }
    
    def search_quotes(self, text: str) -> List[QuoteReadModel]:
        """Search quotes"""
        from sqlalchemy.orm import joinedload
        
        quotes = self.session.query(Quote).options(
            joinedload(Quote.author)
        ).filter(Quote.text.ilike(f"%{text}%")).all()
        
        return [
            QuoteReadModel(
                id=q.id,
                text=q.text,
                author_name=q.author.name,
                tags=[tag.name for tag in q.tags],
                created_at=q.created_at,
                needs_review=q.needs_review
            )
            for q in quotes
        ]


# Usage:
# # WRITE SIDE
# cmd_handler = CommandHandler(session)
#
# create_cmd = CreateQuoteCommand(
#     text="Success is...",
#     author_id=1,
#     source="Book Title"
# )
# quote = cmd_handler.handle_create_quote(create_cmd)
#
# assign_cmd = AssignTagsCommand(quote_id=quote.id, tag_names=["inspiration"])
# cmd_handler.handle_assign_tags(assign_cmd)
#
# # READ SIDE
# query_handler = QueryHandler(session)
# quote_view = query_handler.get_quote_detail(quote.id)
# quotes_list = query_handler.get_quotes_paginated(page=1, per_page=50)
# results = query_handler.search_quotes("success")