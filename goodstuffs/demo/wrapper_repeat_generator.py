#------can only iter through once-----
#------cannot use cycle(generator) because list will return infinite----
print('\n example 1: issue - cannot iter the generator twice')
def a_gen(n):
    for i in range(n):
        yield i
a = a_gen(10)
print(a is iter(a))
print(iter(a) is iter(a) )
print(list(a))
print(list(a))

#------solution: use a class to produce instance of genrator-----
print('\n example 2: using class')
class a_gen2:
    def __init__(self,n):
        self.n = n
    def __iter__(self):
        for i in range(self.n):
            yield i
b = a_gen2(10)
print(type(b))
print(b)
print(b is iter(b))
print(iter(b) is iter(b) )
print(list(b))
print(list(b))

#------advanced: create a wrapper ------
print('\n example 3: using a wrapper that contains class in example 2')

def multi_gen(fn_gen):
    class wrapper:
        def __init__(self,*args,**kwargs):
            self.args = args
            self.kwargs = kwargs
        def __iter__(self):
            return fn_gen(*self.args, **self.kwargs)
    return wrapper
@multi_gen
def a_gen3(n):
    for i in range(n):
        yield i

c = a_gen3(10)
print(type(c))
print(c)
print(c is iter(c))
print(iter(c) is iter(c) )
print(list(c))
print(list(c))







