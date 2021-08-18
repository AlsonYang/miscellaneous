'''
This particular example tested 3 libs of parallelisation: multiprocessing, concurrent.futures and joblib
It simply loops through [0,99999] (PRIMES) and check (is_prime) if each number is a prime number
with 16 cores
joblib: 1.24 secs
mp: 5.15 secs
futures: 21.13 secs
'''

from multiprocessing import Pool
import concurrent.futures
from joblib import Parallel, delayed

import math
import time
import tqdm
import psutil

PRIMES = range(100000)

def is_prime(n):
    # time.sleep(0.1)
    if n < 2:
        return False
    if n == 2:
        return True
    if n % 2 == 0:
        return False

    sqrt_n = int(math.floor(math.sqrt(n)))
    for i in range(3, sqrt_n + 1, 2):
        if n % i == 0:
            return False
    return True

def is_prime_mp(max_workers):
    '''
    1. fork the main process into all child processes
    2. will hang when a child process crashes (eg.main_process_memory * workers > total_memory) --> bug and problematic, dont use! ref: https://stackoverflow.com/questions/24894682/python-multiprocessing-crash-in-subprocess/24894997#24894997
    '''
    with Pool(max_workers) as p:
        return tqdm.tqdm(p.map(is_prime, PRIMES))

def is_prime_future(max_workers):
    '''
    wrapper of mp
    1. fork the main process into all child processes
    2. return exception when child process crashes, no info is given why it crashes
    3. interrupt an interactive session may not kill the child process --> bug, not too problematic, just eat up resources when not needed
    '''
    with concurrent.futures.ProcessPoolExecutor(max_workers) as executor:
        return tqdm.tqdm(executor.map(is_prime, PRIMES))

def is_prime_joblib(max_workers):
    '''
    1. child process uses less memory than the main process, but there is an 
    extra process (resource tracker) needs to be created --> can be memory efficient if
    number of workers are high and the main process is memory intensive
    2. will crush and return exception when run into memory issues, suggested info on why it crashes
    3. tqdm doesnt work with joblib
    '''
    return tqdm.tqdm(Parallel(n_jobs=max_workers)(delayed(is_prime)(n) for n in PRIMES))

if __name__ == '__main__':
    # user input
    max_workers = psutil.cpu_count()
    prime_parallel_fn = is_prime_future
    # execute
    start = time.time()
    result = list(prime_parallel_fn(max_workers))
    print(f'time took for {prime_parallel_fn} with {max_workers} cores to run parallelisation was {(time.time() - start)/1:.2f} secs')
    print(result[:10])
 