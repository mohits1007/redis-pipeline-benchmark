# Redis Pipeline
import redis
import datetime

def pipeline_exec(client):
    begin = datetime.datetime.now()
    pipeline = client.pipeline(transaction = False)
    pipeline.sadd('mylist','firstelement')
    pipeline.sadd('mylist','secondelement')
    pipeline.sadd('mylist','thirdelement')
    pipeline.sadd('mylist','fourthelement')
    pipeline.sadd('mylist','fifthelement')
    pipeline.smembers('mylist')
    print(pipeline.execute()[-1])
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

def pipeline_exec_loop(client, upper_bound):
    begin = datetime.datetime.now()
    pipeline = client.pipeline(transaction = False)
    for i in range(upper_bound):
        pipeline.sadd('mylist','element'+str(i))
    pipeline.smembers('mylist')
    print(pipeline.execute()[-1])
    end = datetime.datetime.now()
    print('pipeline_exec:',end-begin)

def base_exec_loop(client, upper_bound):
    begin = datetime.datetime.now()
    for i in range(upper_bound):
        client.sadd('mylist','element'+str(i))
    print(client.smembers('mylist'))
    end = datetime.datetime.now()
    print('base_exec:',end-begin)

def perform_loop_check(client, upper_bound):
    clear_set(client)
    pipeline_exec_loop(client, upper_bound)
    clear_set(client)
    base_exec_loop(client, upper_bound)

def main():
    client = redis.Redis(host = "127.0.0.1", port = 6379)
    # perform_basic_check(client)
    perform_loop_check(client, 100000)
    

if __name__ == "__main__":
    main()