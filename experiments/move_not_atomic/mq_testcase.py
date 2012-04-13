from zkmq import *
from multiprocessing import Process
import time
import logging

ZOOKEPER_ARG = "localhost:2181"
class ConsumerProcess(Process):
    def __init__(self):
        super(ConsumerProcess, self).__init__()
        
    def run(self):
        self.zk = ZooKeeper(ZOOKEPER_ARG)
        time.sleep(5)
        c = Consumer(self.zk)
        while True:
            data = c.reserve()
            if not data: break
            name= "/parsed/" + data
            try:
                self.zk.create(name, '',[ZOO_OPEN_ACL_UNSAFE])
            except :
                logging.debug("Node already exists in parsed /parsed/%s, task was reserved second time by consumer %s" %(data,c))
            logging.debug("Consumer %s has parsed /parsed/%s" % (c, data))
            c.done()
        c.close()

class ProducerProcess(Process):
    def __init__(self):
        super(ProducerProcess, self).__init__()
        
    def run(self):
        zk = ZooKeeper(ZOOKEPER_ARG)
        p = Producer(zk)
        map(p.put, map(str, range(1,500)))

if __name__ == '__main__':
    zk = ZooKeeper(ZOOKEPER_ARG)
    format = "%(asctime)s:%(levelname)s:/%(process)s:%(message)s"
    logging.basicConfig(format=format, filename='output.log', level=logging.DEBUG)
    if not zk.exists('/parsed'):
        zk.create('/parsed','',[ZOO_OPEN_ACL_UNSAFE])
    
    processes = [ProducerProcess(), ]
    for i in range(20):
        processes.append(ConsumerProcess())
    for p in processes:
        p.start()
    for e in processes: 
        p.join()   
    # do garbage collection
    gc = GarbageCollector(zk)
    gc.collect()
    
    zk.close()