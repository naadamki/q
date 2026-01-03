"""
Advanced Patterns: Data Processing, Concurrency, and API/Web
For handling large datasets efficiently and building scalable APIs
"""

from sqlalchemy.orm import Session, sessionmaker
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor
from threading import Lock, Thread
from queue import Queue
import time
from datetime import datetime
from typing import List, Dict, Callable
import json


# ============================================================================
# DATA PROCESSING PATTERNS
# ============================================================================

# 1. PIPELINE PATTERN
# Chain data transformations together

class ProcessingPipeline:
    """
    Chain multiple processing steps together
    Each step transforms data and passes to next step
    """
    
    def __init__(self):
        self.steps = []
    
    def add_step(self, name: str, func: Callable):
        """Add a processing step"""
        self.steps.append((name, func))
        return self
    
    def process(self, data):
        """Execute all steps in order"""
        result = data
        for step_name, func in self.steps:
            print(f"Executing: {step_name}")
            result = func(result)
        return result


# Example usage:
# pipeline = ProcessingPipeline()
# pipeline.add_step("Load quotes", load_quotes)
#         .add_step("Clean text", clean_quote_text)
#         .add_step("Extract tags", extract_tags)
#         .add_step("Validate", validate_quotes)
#         .add_step("Save to DB", save_quotes)
#
# result = pipeline.process(raw_data)


# 2. BATCH PROCESSING PATTERN (with callbacks)
class BatchProcessor:
    """
    Process large datasets in batches with callbacks
    Useful for monitoring progress, handling errors, etc.
    """
    
    def __init__(self, 
                 session: Session,
                 batch_size: int = 1000,
                 on_batch_complete: Callable = None,
                 on_error: Callable = None):
        self.session = session
        self.batch_size = batch_size
        self.on_batch_complete = on_batch_complete
        self.on_error = on_error
        self.processed = 0
    
    def process_query_results(self, query, processor_func: Callable):
        """
        Process query results in batches
        
        Args:
            query: SQLAlchemy query object
            processor_func: Function to call on each batch
        """
        total = query.count()
        offset = 0
        
        while offset < total:
            batch = query.offset(offset).limit(self.batch_size).all()
            
            if not batch:
                break
            
            try:
                processor_func(batch)
                self.processed += len(batch)
                
                if self.on_batch_complete:
                    self.on_batch_complete({
                        'processed': self.processed,
                        'total': total,
                        'progress': (self.processed / total) * 100,
                        'batch_size': len(batch)
                    })
                
                self.session.commit()
            
            except Exception as e:
                self.session.rollback()
                if self.on_error:
                    self.on_error(e, batch)
            
            offset += self.batch_size
    
    def process_list(self, items: List, processor_func: Callable):
        """Process a list in batches"""
        total = len(items)
        
        for i in range(0, total, self.batch_size):
            batch = items[i:i + self.batch_size]
            
            try:
                processor_func(batch)
                self.processed += len(batch)
                
                if self.on_batch_complete:
                    self.on_batch_complete({
                        'processed': self.processed,
                        'total': total,
                        'progress': (self.processed / total) * 100,
                        'batch_size': len(batch)
                    })
            
            except Exception as e:
                if self.on_error:
                    self.on_error(e, batch)


# Usage example:
# def on_batch_done(stats):
#     print(f"Progress: {stats['progress']:.1f}% ({stats['processed']}/{stats['total']})")
#
# def process_batch(batch):
#     for quote in batch:
#         quote.needs_review = False
#
# processor = BatchProcessor(session, batch_size=500, on_batch_complete=on_batch_done)
# processor.process_query_results(Quote.query, process_batch)


# ============================================================================
# CONCURRENCY PATTERNS
# ============================================================================

# 1. THREAD POOL PATTERN (for I/O-bound work)
class ThreadPoolDataProcessor:
    """
    Use thread pools for I/O-bound operations
    (database queries, API calls, file I/O)
    
    NOT for CPU-bound work (use ProcessPoolDataProcessor instead)
    """
    
    def __init__(self, session_factory: sessionmaker, max_workers: int = 5):
        self.session_factory = session_factory
        self.max_workers = max_workers
    
    def process_quotes_parallel(self, quote_ids: List[int], 
                               process_func: Callable) -> List:
        """
        Process quotes in parallel using thread pool
        Each thread gets its own database session
        """
        results = []
        
        def process_with_session(quote_id):
            session = self.session_factory()
            try:
                from models import Quote
                quote = session.query(Quote).filter(Quote.id == quote_id).first()
                if quote:
                    return process_func(quote, session)
            finally:
                session.close()
        
        with ThreadPoolExecutor(max_workers=self.max_workers) as executor:
            futures = [executor.submit(process_with_session, qid) 
                      for qid in quote_ids]
            
            for future in futures:
                result = future.result()
                if result:
                    results.append(result)
        
        return results


# Usage:
# processor = ThreadPoolDataProcessor(Session, max_workers=10)
# quote_ids = [1, 2, 3, 4, 5]
# 
# def fetch_and_enrich(quote, session):
#     # Do something with quote (could call external API, etc.)
#     return {'id': quote.id, 'text': quote.text}
#
# results = processor.process_quotes_parallel(quote_ids, fetch_and_enrich)


# 2. PRODUCER-CONSUMER PATTERN
class ProducerConsumer:
    """
    Decouple data production from processing
    Producer adds data to queue, consumers process it
    """
    
    def __init__(self, num_consumers: int = 3):
        self.queue = Queue()
        self.num_consumers = num_consumers
        self.running = False
    
    def producer(self, data_source: Callable):
        """Generate data and add to queue"""
        for item in data_source():
            self.queue.put(item)
        
        # Signal consumers to stop
        for _ in range(self.num_consumers):
            self.queue.put(None)
    
    def consumer(self, process_func: Callable, consumer_id: int):
        """Process items from queue"""
        while True:
            item = self.queue.get()
            
            if item is None:  # Stop signal
                break
            
            try:
                process_func(item, consumer_id)
            except Exception as e:
                print(f"Consumer {consumer_id} error: {e}")
            finally:
                self.queue.task_done()
    
    def run(self, data_source: Callable, process_func: Callable):
        """Start producer and consumers"""
        self.running = True
        
        # Start consumer threads
        consumer_threads = [
            Thread(target=self.consumer, args=(process_func, i))
            for i in range(self.num_consumers)
        ]
        
        for thread in consumer_threads:
            thread.start()
        
        # Start producer (blocking)
        self.producer(data_source)
        
        # Wait for all consumers to finish
        self.queue.join()
        
        for thread in consumer_threads:
            thread.join()
        
        self.running = False


# Usage:
# def data_generator():
#     """Yield quotes from database"""
#     session = Session()
#     for quote in session.query(Quote).yield_per(100):
#         yield quote
#
# def process_quote(quote, consumer_id):
#     print(f"Consumer {consumer_id} processing quote {quote.id}")
#     # Do expensive operation
#     time.sleep(1)
#
# pc = ProducerConsumer(num_consumers=5)
# pc.run(data_generator, process_quote)


# 3. RATE LIMITER PATTERN
class RateLimiter:
    """
    Control the rate of operations
    Useful for API calls, database writes, etc.
    """
    
    def __init__(self, max_per_second: int = 10):
        self.max_per_second = max_per_second
        self.min_interval = 1.0 / max_per_second
        self.last_call = 0
        self.lock = Lock()
    
    def wait(self):
        """Wait if necessary to maintain rate limit"""
        with self.lock:
            now = time.time()
            time_since_last = now - self.last_call
            
            if time_since_last < self.min_interval:
                wait_time = self.min_interval - time_since_last
                time.sleep(wait_time)
            
            self.last_call = time.time()


# Usage:
# limiter = RateLimiter(max_per_second=5)
# 
# for item in items:
#     limiter.wait()
#     process_item(item)  # Won't exceed 5 per second


# ============================================================================
# API/WEB PATTERNS
# ============================================================================

# 1. REQUEST/RESPONSE WRAPPER PATTERN
class APIResponse:
    """Standardized API response format"""
    
    def __init__(self, data=None, error=None, status_code=200):
        self.data = data
        self.error = error
        self.status_code = status_code
    
    def to_dict(self) -> Dict:
        """Convert to dictionary for JSON response"""
        return {
            'success': self.error is None,
            'data': self.data,
            'error': self.error,
            'timestamp': datetime.utcnow().isoformat()
        }
    
    def to_json(self) -> str:
        """Convert to JSON string"""
        return json.dumps(self.to_dict())


# Usage:
# response = APIResponse(data={'quotes': [...]}, status_code=200)
# return response.to_dict(), response.status_code


# 2. SERVICE LAYER PATTERN
# Encapsulate business logic separate from API endpoints

class QuoteService:
    """
    Business logic layer
    Handles all quote operations, separate from API
    """
    
    def __init__(self, session: Session):
        self.session = session
    
    def get_quote_by_id(self, quote_id: int):
        """Get a single quote"""
        from models import Quote
        return self.session.query(Quote).filter(
            Quote.id == quote_id
        ).first()
    
    def search_quotes(self, query: str, limit: int = 50):
        """Search quotes by text"""
        from models import Quote
        return self.session.query(Quote).filter(
            Quote.text.ilike(f"%{query}%")
        ).limit(limit).all()
    
    def create_quote(self, text: str, author_id: int) -> 'Quote':
        """Create a new quote"""
        from models import Quote
        quote = Quote(text=text, author_id=author_id)
        self.session.add(quote)
        self.session.commit()
        return quote
    
    def update_quote(self, quote_id: int, **kwargs) -> 'Quote':
        """Update a quote"""
        from models import Quote
        quote = self.get_quote_by_id(quote_id)
        if not quote:
            raise ValueError("Quote not found")
        
        for key, value in kwargs.items():
            if hasattr(quote, key):
                setattr(quote, key, value)
        
        self.session.commit()
        return quote
    
    def delete_quote(self, quote_id: int) -> bool:
        """Delete a quote"""
        from models import Quote
        quote = self.get_quote_by_id(quote_id)
        if not quote:
            return False
        
        self.session.delete(quote)
        self.session.commit()
        return True


# Usage in Flask:
# @app.route('/api/quotes/<int:quote_id>')
# def get_quote(quote_id):
#     session = SessionFactory()
#     service = QuoteService(session)
#     
#     try:
#         quote = service.get_quote_by_id(quote_id)
#         if not quote:
#             response = APIResponse(error='Quote not found', status_code=404)
#         else:
#             response = APIResponse(data={'id': quote.id, 'text': quote.text})
#     except Exception as e:
#         response = APIResponse(error=str(e), status_code=500)
#     
#     return response.to_dict(), response.status_code


# 3. DEPENDENCY INJECTION PATTERN
class DIContainer:
    """
    Manage dependencies - makes testing easier
    """
    
    def __init__(self):
        self.services = {}
    
    def register(self, name: str, service):
        """Register a service"""
        self.services[name] = service
    
    def get(self, name: str):
        """Get a service"""
        return self.services.get(name)


# Usage:
# container = DIContainer()
# container.register('session', Session())
# container.register('quote_service', QuoteService(container.get('session')))
#
# # In endpoints:
# quote_service = container.get('quote_service')
# quote = quote_service.get_quote_by_id(1)


# 4. CACHING PATTERN
class CacheLayer:
    """
    Simple in-memory cache for frequently accessed data
    """
    
    def __init__(self, ttl_seconds: int = 300):
        self.cache = {}
        self.ttl = ttl_seconds
    
    def get(self, key: str):
        """Get cached value if not expired"""
        if key not in self.cache:
            return None
        
        value, timestamp = self.cache[key]
        
        if time.time() - timestamp > self.ttl:
            del self.cache[key]
            return None
        
        return value
    
    def set(self, key: str, value):
        """Cache a value"""
        self.cache[key] = (value, time.time())
    
    def clear(self):
        """Clear all cache"""
        self.cache.clear()


# Usage:
# cache = CacheLayer(ttl_seconds=600)
#
# @app.route('/api/quotes/<int:quote_id>')
# def get_quote(quote_id):
#     # Check cache first
#     cached = cache.get(f'quote_{quote_id}')
#     if cached:
#         return cached
#     
#     # Get from DB
#     quote = service.get_quote_by_id(quote_id)
#     
#     # Cache it
#     cache.set(f'quote_{quote_id}', quote.to_dict())
#     
#     return quote.to_dict()


# 5. MIDDLEWARE PATTERN
class RequestLogger:
    """Middleware to log requests"""
    
    @staticmethod
    def log_request(func):
        def wrapper(*args, **kwargs):
            start = time.time()
            result = func(*args, **kwargs)
            duration = time.time() - start
            print(f"Request took {duration:.2f}s")
            return result
        return wrapper


class ErrorHandler:
    """Middleware to handle errors"""
    
    @staticmethod
    def handle_errors(func):
        def wrapper(*args, **kwargs):
            try:
                return func(*args, **kwargs)
            except ValueError as e:
                return APIResponse(error=str(e), status_code=400).to_dict(), 400
            except Exception as e:
                return APIResponse(error='Internal server error', status_code=500).to_dict(), 500
        return wrapper


# Usage:
# @app.route('/api/quotes')
# @ErrorHandler.handle_errors
# @RequestLogger.log_request
# def list_quotes():
#     # endpoint code
#     pass
