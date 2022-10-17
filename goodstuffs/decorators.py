"""
- Decorator without extra arguments
- Decorator with extra arguments
- @wraps to carry forward the base function information. 
    Without it, when you call help(mysleep), it will show the information for wrapper function instead of the base function 
"""

from functools import wraps
from datetime import datetime
import time

## 1
def timeit(f):
    """decorator with no extra arguments"""
    @wraps(f)
    def wrapper(*args, **kwargs):
        t1 = datetime.now()
        f(*args, **kwargs)
        print(datetime.now() - t1)
    return wrapper
@timeit
def mysleep(seconds=1):
    time.sleep(seconds)
# sleep = timeit(sleep, 1, 2)
mysleep(1)

## 2
class timeit_with_msg:
    """ decorator with extra argument `msg`"""
    def __init__(self, msg='Hello'):
        self._msg = msg

    def __call__(self, f):
        @wraps(f)
        def wrapper(*args, **kwargs):
            print(self._msg)
            t1 = datetime.now()
            f(*args, **kwargs)
            print(datetime.now() - t1)
        return wrapper

@timeit_with_msg(msg='Hi')
def mysleep(seconds=1):
    """sleep for n seconds"""
    time.sleep(seconds)
# sleep = timeit_with_msg(msg='Hi')(sleep)
mysleep(1)
help(mysleep)