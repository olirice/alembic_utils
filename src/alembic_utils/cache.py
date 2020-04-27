import functools
from typing import Any, Callable, MutableMapping


def cachedmethod(cache: Callable[[Any], MutableMapping], key=lambda x: x):
    """Decorator to wrap a class or instance method with a memoizing
    callable that saves results in a cache.

    cache receives a single input, self
    key receives all inputs
    """

    def decorator(method):
        def wrapper(self, *args, **kwargs):

            _cache = cache(self)
            _key = key(self, *args, **kwargs)
            try:
                return _cache[_key]
            except KeyError:
                pass  # key not found
            _value = method(self, *args, **kwargs)
            _cache[_key] = _value
            return _value

        return functools.update_wrapper(wrapper, method)

    return decorator
