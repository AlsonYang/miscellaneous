# list objects in python from most memory intensive to least
'''
from Ben Ong
--> can add funtionality to print out used and avaialble memory as well
'''
import sys
def sizeof_fmt(num, suffix='B'):
    ''' by Fred Cirera,  https://stackoverflow.com/a/1094933/1870254, modified'''
    for unit in ['','Ki','Mi','Gi','Ti','Pi','Ei','Zi']:
        if abs(num) < 1024.0:
            return "%3.1f %s%s" % (num, unit, suffix)
        num /= 1024.0
    return "%.1f %s%s" % (num, 'Yi', suffix)
s='''for name, size in sorted(((name, sys.getsizeof(value)) for name, value in locals().items()), key= lambda x: -x[1])[:15]:
        print("{:>30}: {:>8}".format(name, sizeof_fmt(size)))'''
exec(s)

# check the rss usages in currnet process
import os, psutil
process = psutil.Process(os.getpid())
print(f'rss: {process.memory_info().rss/(1025**3):.2f}GB')  # in bytes 

# check the memory usages by all process
import psutil
print(f'used: {psutil.virtual_memory().used/(1025**3):.2f}GB')