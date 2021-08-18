import functools


def upto(n):
    def sumemup(fn):
        @functools.wraps(fn)
        def wrapper(*args):
            result = 0
            for i in range(1, n + 1):
                result += fn(i)
            return result

        return wrapper

    return sumemup


@upto(5)
def sum_constant(x):
    """
    """
    return x


@upto(5)
def sum_cube(x):
    """
    >>> sum_cube(5)
    225
    """
    return x ** 3


print(sum_cube)
print(sum_cube(5))
print(sum_constant(5))

import sys

print(sys.version)


def fib(x):
    if x <= 1:
        return x
    else:
        return fib(x - 2) + fib(x - 1)


fib(4)
