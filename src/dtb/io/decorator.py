import os
from typing import Callable
from ..model.input import Input
from ..model.output import Output


def io(*args, **kwargs):
   def decorator(func: Callable):
       def wrapper():
            if "DATABRICKS_RUNTIME_VERSION" in os.environ:
                return func(*args, **kwargs)
            return lambda *args, **kwargs: None
       setattr(wrapper, 'args', [arg for arg in args])
       setattr(wrapper, 'kwargs', kwargs)
       setattr(wrapper, 'datasets', {
           k: v for k, v in kwargs.items() if isinstance(v, Input) or isinstance(v, Output)
       })
       return wrapper
   return decorator
