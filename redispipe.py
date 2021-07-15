# Redis Pipeline
import redis
import datetime
from multiprocessing import Process

def pipeline_exec(client):
    begin = datetime.datetime.now()
    pipeline = client.pipeline(transaction = False)
    pipeline.sadd('mylist','firstelement')
    pipeline.sadd('mylist','secondelement')
    pipeline.sadd('mylist','thirdelement')
    pipeline.sadd('mylist','fourthelement')
    pipeline.sadd('mylist','fifthelement')
    pipeline.smembers('mylist')
    print(pipeline.execute())
    end = datetime.datetime.now()
    print('pipeline_exec:',end-begin)

def clear_set(client):
    client.delete('mylist')
    print(client.smembers('mylist'))

def base_exec(client):
    begin = datetime.datetime.now()
    client.sadd('mylist','firstelement')
    client.sadd('mylist','secondelement')
    client.sadd('mylist','thirdelement')
    client.sadd('mylist','fourthelement')
    client.sadd('mylist','fifthelement')
    print(client.smembers('mylist'))
    end = datetime.datetime.now()
    print('base_exec:',end-begin)

def perform_basic_check(client):
    clear_set(client)
    pipeline_exec(client)
    clear_set(client)
    base_exec(client)

def pipeline_exec_loop(client, upper_bound = 100, id = 0):
    begin = datetime.datetime.now()
    pipeline = client.pipeline(transaction = False)
    for i in range(upper_bound):
        pipeline.sadd('mylist','process:'+str(id)+'element:'+str(i))
    # pipeline.smembers('mylist')
    pipeline.execute()
    end = datetime.datetime.now()
    print('pipeline_exec:',end-begin)

def base_exec_loop(client, upper_bound = 100, id = 1):
    begin = datetime.datetime.now()
    for i in range(upper_bound):
        client.sadd('mylist','process:'+str(id)+'element'+str(i))
    # print(client.smembers('mylist'))
    end = datetime.datetime.now()
    print('base_exec:',end-begin)

def perform_loop_check(client, upper_bound):
    clear_set(client)
    pipeline_exec_loop(client, upper_bound)
    clear_set(client)
    base_exec_loop(client, upper_bound)

# perform_basic_check(client)
# perform_loop_check(client, 100000)

if __name__ == '__main__':
    client = redis.Redis(host = "127.0.0.1", port = 6379)
    proc = []
    id = 1
    no_of_paralled_func = 100
    no_of_comnd_per_func = 100000
    for i in range(no_of_paralled_func):
        p = Process(target=base_exec_loop, args=(client, no_of_comnd_per_func, id,))
        p.start()
        proc.append(p)
        id += 1
    for p in proc:
        p.join()
    

