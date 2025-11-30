"""
Cache service for Malla using Redis.
"""

import functools
import hashlib
import logging
import pickle
from collections.abc import Callable
from typing import Any, TypeVar, cast

import redis

from ..config import get_config

logger = logging.getLogger(__name__)

T = TypeVar("T")


class CacheService:
    """Service for handling application caching."""

    _redis_client: redis.Redis | None = None
    _enabled: bool = False

    @classmethod
    def initialize(cls) -> None:
        """Initialize the Redis connection."""
        config = get_config()
        if config.redis_url:
            try:
                cls._redis_client = redis.from_url(config.redis_url)
                # Test connection
                cls._redis_client.ping()
                cls._enabled = True
                logger.info(f"Redis cache initialized at {config.redis_url}")
            except Exception as e:
                logger.error(f"Failed to initialize Redis cache: {e}")
                cls._enabled = False
        else:
            logger.info("Redis URL not configured, caching disabled")
            cls._enabled = False

    @classmethod
    def get(cls, key: str) -> Any | None:
        """Get value from cache."""
        if not cls._enabled or not cls._redis_client:
            return None
        try:
            data = cls._redis_client.get(key)
            if data:
                return pickle.loads(data)
        except Exception as e:
            logger.warning(f"Cache get error for {key}: {e}")
        return None

    @classmethod
    def set(cls, key: str, value: Any, ttl: int = 60) -> None:
        """Set value in cache."""
        if not cls._enabled or not cls._redis_client:
            return
        try:
            # Use pickle to handle complex Python objects
            data = pickle.dumps(value)
            cls._redis_client.setex(key, ttl, data)
        except Exception as e:
            logger.warning(f"Cache set error for {key}: {e}")

    @classmethod
    def delete_pattern(cls, pattern: str) -> None:
        """Delete keys matching pattern."""
        if not cls._enabled or not cls._redis_client:
            return
        try:
            keys = cls._redis_client.keys(pattern)
            if keys:
                cls._redis_client.delete(*keys)
        except Exception as e:
            logger.warning(f"Cache delete pattern error for {pattern}: {e}")


def cache_response(ttl: int = 60, prefix: str = "view") -> Callable[[Callable[..., T]], Callable[..., T]]:
    """Decorator to cache function results."""

    def decorator(func: Callable[..., T]) -> Callable[..., T]:
        @functools.wraps(func)
        def wrapper(*args: Any, **kwargs: Any) -> T:
            # Lazy initialization if needed
            if CacheService._redis_client is None:
                CacheService.initialize()

            # Generate cache key based on function name and arguments
            key_parts = [prefix, func.__module__, func.__name__]

            # Collect arguments for key generation
            try:
                # Include Flask request parameters if available
                request_data = ""
                try:
                    from flask import request
                    # Check if we are in a request context (request.args will raise/fail if not)
                    if request and hasattr(request, "args"):
                        # Use sorted items to ensure consistency
                        request_data = str(sorted(request.args.items()))
                except Exception:
                    # Not in Flask context or import failed, ignore
                    pass

                key_data = str(args) + str(sorted(kwargs.items())) + request_data
                key_hash = hashlib.md5(key_data.encode()).hexdigest()
                cache_key = f"{':'.join(key_parts)}:{key_hash}"

                # Try to get from cache
                cached_result = CacheService.get(cache_key)
                if cached_result is not None:
                    return cast(T, cached_result)
            except Exception as e:
                logger.warning(f"Cache key generation error: {e}")
                return func(*args, **kwargs)

            # Execute function
            result = func(*args, **kwargs)

            # Store in cache
            try:
                CacheService.set(cache_key, result, ttl)
            except Exception:
                pass

            return result

        return wrapper

    return decorator
