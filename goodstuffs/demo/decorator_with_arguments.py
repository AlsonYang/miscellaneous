from functools import cache, lru_cache
from datetime import datetime
def timeit(text): # This is the decorator with arguments
    def decorator(fn):
        def wrapper():
            print(text)
            t1 = datetime.now()
            fn()
            # print(f'the total time it took was {(datetime.now() - t1).seconds} seconds')
        return wrapper
    return decorator

@lru_cache(maxsize = 3) # This is an useful python decorator for caching results
def fib(n):
    if n <= 2:
        return 1
    else:
        return fib(n-1) + fib(n-2)

@timeit(text = 'hi')
def main():
    result = [fib(x) for x in range(0, 100)]
    print(result)

# main = timeit(text='hi')(main) --> this is how decorator and its argument is called under the hood

if __name__ == '__main__':
    main()