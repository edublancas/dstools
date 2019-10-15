from multiprocessing import Pool, TimeoutError
import time
import os

def f(x):
    return x*x


done = []

def callback(x):
    done.append(x)


t = [[0, 1], [2, 3], [4, 5]]
i = 0

def poll():
    global i
    print('polling', i)

    if i < len(t):
        r = t[i]
        i+=1
        print('return ', r)
        return r
    else:
        print('NONEEE')
        return None


with Pool(processes=4) as pool:
    next_ = poll()

    while next_ is not None:
        for val in next_:
            print('starting', val)
            pool.apply_async(f, (val,), callback=callback).get()

        time.sleep(1)

        next_ = poll()

        

done