from functools import partial
from multiprocessing import Pool, cpu_count
import time
from typing import Iterable


class SlowIterable(object):
    def __init__(self, iterable):
        self.iterable = iterable
        self.index = 0

    def __iter__(self):
        return self

    def __next__(self):
        if self.index >= len(self.iterable):
            raise StopIteration
        result = self.iterable[self.index]
        self.index += 1
        return result
    
    def __len__(self):
        return len(self.iterable)
    
    def __getitem__(self, index):
        time.sleep(1e-3)
        return self.iterable[index]
    
    def __repr__(self):
        return repr(self.iterable)
    
    def __str__(self):
        return str(self.iterable)
    
    def __bool__(self):
        return bool(self.iterable)
    
    
def get_square(x):
    return x**2


def get_square_slow_iterable(iterable, idx):
    return iterable[idx]**2


def profile(func):
    def wrapper(*args, **kwargs):
        t1 = time.time()
        result = func(*args, **kwargs)
        t2 = time.time()
        print(func.__name__, t2 - t1)
        return result
    return wrapper


@profile
def get_square_sequential(iterable: Iterable):
    return [x**2 for x in iterable]


@profile
def get_square_parallel(iterable: Iterable, num_workers: int):
    pool = Pool(num_workers)
    r = pool.map_async(get_square, iterable)
    r.wait()
    pool.close()
    return r.get()


@profile
def get_square_parallel2(iterable: Iterable, num_workers: int):
    with Pool(num_workers) as pool:
        r = pool.map_async(get_square, iterable)
        r.wait()
    return r.get()

@profile
def get_square_parallel3(iterable: Iterable, num_workers: int):
    with Pool(num_workers) as pool:
        r = pool.map_async(get_square, iterable, chunksize=len(iterable)//num_workers)
        r.wait()
    return r.get()


@profile
def get_square_parallel4(iterable: Iterable, num_workers: int):
    with Pool(num_workers) as pool:
        r = list(pool.imap(get_square, iterable, chunksize=len(iterable)//num_workers))
    return r


@profile
def get_square_slow_iterable_parallel(iterable: Iterable, num_workers: int):
    with Pool(num_workers) as pool:
        arg_iter = [(SlowIterable(iterable), i) for i in range(len(iterable))]
        result = pool.starmap_async(get_square_slow_iterable, arg_iter, chunksize=len(iterable)//num_workers)
        result.wait()
    return result.get()


@profile
def get_square_slow_iterable_parallel_partial(iterable: Iterable, num_workers: int):
    with Pool(num_workers) as pool:
        iterable = SlowIterable(iterable)
        func = partial(get_square_slow_iterable, iterable)
        result = pool.map(func, range(len(iterable)))
    return result


@profile
def get_square_slow_iterable_parallel_partial_imap(iterable: Iterable, num_workers: int):
    # TODO: imap takes too long time, 
    with Pool(num_workers) as pool:
        iterable = SlowIterable(iterable)
        func = partial(get_square_slow_iterable, iterable)
        result = pool.imap(func, range(len(iterable)))
    return list(result)

if __name__ == '__main__':
    # TASK 1: parallelize fast iterable
    # get_square_sequential(10**8)
    # for n in range(cpu_count(), cpu_count()+1):
    #     print(n)
    #     get_square_parallel(iterable=range(10**8), num_workers=n)
    #     get_square_parallel2(iterable=range(10**8), num_workers=n)
    #     get_square_parallel3(iterable=range(10**8), num_workers=n)
    #     get_square_parallel4(iterable=range(10**8), num_workers=n)
    
    # TASK 2: parallelize slow iterable
    get_square_slow_iterable_parallel(iterable=range(10**5), num_workers=cpu_count())
    get_square_slow_iterable_parallel_partial(iterable=range(10**5), num_workers=cpu_count())
    # res = get_square_slow_iterable_parallel_partial_imap(iterable=range(10**5), num_workers=cpu_count())
    # print(res)
