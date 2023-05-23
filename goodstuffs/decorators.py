"""
- 1. Decorator without extra parameters
- 2. Decorator with extra parameters
- @wraps to carry forward the base function information. 
    Without it, when you call help(mysleep), it will show the information for wrapper function instead of the base function 
"""

import time
from functools import wraps
from datetime import datetime

## 0: decorator template
def decorator(f):
    """ 
    f: The function to be decorated
    wraps(f): carry thru the function signiture into `wrapper`
    `*args, **kwargs`: pass along any arguments to the decorated function

    If without `<do anything>`, the decorated version will be the same as the 
        original version. 
    """
    @wraps(f)
    def wrapper(*args, **kwargs):
        # <do anything>
        ret = f(*args, **kwargs) # execute the decorated function as it is and stored the result
        # <do anything>
        return ret
    return wrapper


## 1: decorator without extra parameters
def timeit(f):
    """decorator with no extra arguments"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        t1 = datetime.now()
        ret = f(*args, **kwargs)
        print(datetime.now() - t1)
        return ret
    return wrapper

@timeit
def mysleep(seconds=1):
    time.sleep(seconds)
    return "im awake now"
# [equivalent] sleep = timeit(sleep, 1, 2)

print(mysleep(1))


## 2: decorator with extra parameters
class timeit_with_msg:
    """ decorator with extra argument `msg`"""
    def __init__(self, msg='Hello'):
        self._msg = msg

    def __call__(self, f):
        print('__call__')
        @wraps(f)
        def wrapper(*args, **kwargs):
            print(self._msg)
            t1 = datetime.now()
            ret = f(*args, **kwargs)
            print(datetime.now() - t1)
            return ret
        return wrapper

@timeit_with_msg(msg='I am going to sleep now') # trigger __init__()
def mysleep(seconds=1): # trigger __call__()
    """sleep for n seconds"""
    time.sleep(seconds)
    return "im awake now"
# [equivalent] sleep = timeit_with_msg(msg='Hi')(sleep)

result = mysleep(1) # trigger wrapper()
print(result)
