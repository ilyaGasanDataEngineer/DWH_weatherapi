import threading
import queue
import time
from itertools import count
from os import lockf, times


def printing_numbers():
    for i in range(10):
        print(i)


def producer(q):
    for i in range(10):
        q.put(i)
        print(f'proceduced {i}')


def consumer(q):
    count=0
    while count<3:
        while not q.empty():
            item = q.get()
            print(f'Consumed {item}')
            q.task_done()
            count = 0
        else:
            count += 1





q = queue.Queue()

procedure1_thread = threading.Thread(target=producer, args=(q,))
procedure2_thread = threading.Thread(target=consumer, args=(q,))

procedure1_thread.start()
procedure2_thread.start()

procedure1_thread.join()
procedure2_thread.join()

