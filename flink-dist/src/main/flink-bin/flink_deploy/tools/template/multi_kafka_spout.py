#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from datetime import datetime, timedelta
from pyutil.program import metrics2 as metrics
from pyutil.kafka_proxy.kafka_proxy import KafkaProxy
from pyutil.program.conf import Conf

import os, sys, logging, signal, time, multiprocessing, ctypes, json, traceback, random, \
    math, uuid, threading, Queue
import ss_lib.topology.util.storm as storm
import ss_lib.topology.util.dsignal as dsignal


class SpoutProducer(threading.Thread):
    def __init__(self, conf, index, queue):
        threading.Thread.__init__(self)
        self.conf = conf
        self.queue = queue
        self.topic = self.conf["topic_name"]
        self.consumer_group = self.conf["consumer_group"]
        self.whence = self.conf.get("whence", 1)
        self.proxy = KafkaProxy(topic=self.topic, consumer_group=self.consumer_group, partitions=index, conf=Conf("/opt/tiger/ss_conf/ss/kafka.conf"))
        self.index = index
        self.offset = self.conf.get("offset",0)
        self.proxy.set_consumer_offset(self.offset, self.whence)
        self.read_num = self.conf.get("read_num", 100)
        self.sleep_time = self.conf.get("sleep_time", 0.05)
        
        self.decay_factor = self.conf.get("decay_factor", 80.0) 
        self.decay_read_num = self.read_num / (self.decay_factor*1.0) 
            
        self.start_time = time.time()
        self.running = True

    def run(self):
        while self.running:
            try:
                if os.getppid() == 1:
                    logging.info("the worker of storm exit, now we must exit too!")
                    os._exit(-1)
                ts = time.time()
                if (ts - self.start_time) <= self.decay_factor :
                    self.read_num = self.decay_read_num*(ts - self.start_time)
                msgs = self.proxy.fetch_msgs(count=int(self.read_num))
                metrics.emit_timer("get_{{spout_name}}.latency", (time.time() - ts)*1000000)
                if not msgs:
                    time.sleep(1)
                    continue
                self.queue.put((self.index, msgs))
            except Exception as e:
                logging.exception(e)
            time.sleep(self.sleep_time)
        logging.info("multi_kafka_spout partitions:%d run exit"%(self.index))

    def stop(self):
        logging.info("multi_kafka_spout partition:%d stop"%(self.index))
        self.running = False

class KafkaSpout(storm.Spout):
    def __init__(self):
        self.conf = None

    def init_metrics(self):
        metrics.init(self.conf)
        metrics.define_counter("get_{{spout_name}}.throughput", "num")
        metrics.define_timer("get_{{spout_name}}.latency", "ms")

    def initialize(self, conf=None, context=None):
        try:
            self.conf = conf

            self.is_debug = self.conf.get("is_debug", 0)
            self.open_ack = self.conf.get("open_ack", {{ack}})

            self.init_metrics()
            
            self.dsignal = dsignal.DSignal(self.conf, handler=self, component_name="kafka_spout")

            partition_num = self.conf.get("partition")
            self.queue = Queue.Queue()
            self.producer_list = []
            for i in xrange(partition_num):
                producer = SpoutProducer(self.conf, i, self.queue)
                self.producer_list.append(producer)
                producer.start()
            
        except Exception as e:
            logging.exception(e)
            raise

    def on_signal(self, data):
        try:
            if data.get("cmd") == "stop":
                for producer in self.producer_list:
                    producer.stop()
            elif data.get("cmd") == "reload":
                pass
            else:
                pass
        except Exception as e:
            logging.exception(e)

    def ack(self, id):
        pass

    def fail(self, id):
        pass

    def nextTuple(self):
        try:
            index, msgs = self.queue.get()
            if self.is_debug:
                print "%d:%s"%(index, msgs)
            else:
                if self.open_ack:
                    storm.emit([msgs],id=random.randint(0,10000000000))
                else:
                    storm.emit([msgs])
                metrics.emit_counter("get_{{spout_name}}.throughput", len(msgs))
        except Exception as e:
            logging.exception(e)

def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)
    if len(sys.argv) == 1:
        KafkaSpout().run()
    else:
        conf = {"zk_hosts" : "10.4.16.44:2181, 10.4.16.45:2181, 10.4.16.46:2181, 10.4.16.47:2181, 10.4.16.152:2181", "zk_root_dsignal" : "/storm2/dsignal", "sleep_time":0.5,"partition":6, "whence":2, "topic_name":"user_action_log", "read_num":10, "open_ack":0,"consumer_group":"storm_test", "is_debug":1, "decay_factor":2}
        t = KafkaSpout()
        t.initialize(conf)
        while 1:
            t.nextTuple()

if __name__ == "__main__":
    main()

