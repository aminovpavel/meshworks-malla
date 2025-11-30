"""
Cache service for Malla using Redis.
"""

import functools
import hashlib
import logging
import pickle
from collections.abc import Callable
from dataclasses import dataclass
from typing import Any, TypeVar, cast

from flask import Response

import redis

from ..config import get_config

logger = logging.getLogger(__name__)

T = TypeVar("T")


@dataclass
class CachedFlaskResponse:
    data: bytes
    status: int
    headers: list[tuple[str, str]]
    mimetype: str | None


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
                # Always create a new client to ensure fresh connection parameters
                client = redis.from_url(config.redis_url)
                # Test connection
                client.ping()

                # Assign only if successful
                cls._redis_client = client
                cls._enabled = True
                logger.info(f"Redis cache initialized successfully at {config.redis_url}")
            except Exception as e:
                # Ensure we reset to None so we can retry later
                cls._redis_client = None
                cls._enabled = False
                # Log full exception for debugging
                logger.error(f"Failed to initialize Redis cache: {e!r}", exc_info=True)
        else:
            logger.info("Redis URL not configured, caching disabled")
            cls._enabled = False
            cls._redis_client = None

    @classmethod
    def get(cls, key: str) -> Any | None:
        """Get value from cache."""
        if not cls._enabled or not cls._redis_client:
            # Attempt lazy re-initialization if configured but not enabled
            # This handles cases where Redis was down at startup but came up later
            if cls._redis_client is None and get_config().redis_url:
                 cls.initialize()

            # If still not enabled, give up
            if not cls._enabled or not cls._redis_client:
                return None

        try:
            data = cls._redis_client.get(key)
            if data:
                return pickle.loads(cast(bytes, data))
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
            # Explicit cast to satisfy type checkers if needed, though redis-py types are usually handled
            cls._redis_client.setex(key, ttl, data)
        except Exception as e:
            logger.warning(f"Cache set error for {key}: {e}", exc_info=True)

    @classmethod
    def delete_pattern(cls, pattern: str) -> None:
        """Delete keys matching pattern."""
        if not cls._enabled or not cls._redis_client:
            return
        try:
            keys = cls._redis_client.keys(pattern)
            if keys:
                cls._redis_client.delete(*cast(list[str], keys))
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

            try:
                # Include Flask request parameters if available
                request_data = ""
                try:
                    from flask import request
                    # Check if we are in a request context (request.args will raise/fail if not)
                    if request:
                        # Accessing request.args might trigger logic, be careful
                        # We use sorted items to ensure consistency
                        if hasattr(request, "args"):
                            request_data += str(sorted(request.args.items()))

                        # Also consider form data for POST requests if relevant?
                        # For now, sticking to args as per original logic
                except Exception:
                    # Not in Flask context or import failed, ignore
                    pass

                key_data = str(args) + str(sorted(kwargs.items())) + request_data
                key_hash = hashlib.md5(key_data.encode()).hexdigest()
                cache_key = f"{':'.join(key_parts)}:{key_hash}"

                # Try to get from cache
                cached_result = CacheService.get(cache_key)
                if isinstance(cached_result, CachedFlaskResponse):
                    return cast(
                        T,
                        Response(
                            cached_result.data,
                            status=cached_result.status,
                            headers=cached_result.headers,
                            mimetype=cached_result.mimetype,
                        ),
                    )
                if cached_result is not None:
                    # Log cache hit at debug level
                    logger.debug(f"Cache hit for {cache_key}")
                    return cast(T, cached_result)
            except Exception as e:
                logger.warning(f"Cache key generation or retrieval error: {e}")
                # Fallthrough to execute function

            # Execute function
            result = func(*args, **kwargs)

            # Store in cache
            try:
                if isinstance(result, Response):
                    # Cache Flask Response objects specifically
                    cached_value: CachedFlaskResponse | Any = CachedFlaskResponse(
                        data=result.get_data(),
                        status=result.status_code,
                        headers=[(k, v) for k, v in result.headers.items()],
                        mimetype=result.mimetype,
                    )
                else:
                    cached_value = result

                # Only cache successful responses if needed (optional logic, keeping simple for now)
                CacheService.set(cache_key, cached_value, ttl)
            except Exception as e:
                logger.error(f"Failed to cache response for {cache_key}: {e}", exc_info=True)

            return result

        return wrapper

    return decorator
