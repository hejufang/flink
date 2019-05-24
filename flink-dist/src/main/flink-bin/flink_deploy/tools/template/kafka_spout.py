#!/usr/bin/env python
# -*- coding: utf-8 -*- 

from datetime import datetime, timedelta
from pyutil.program import metrics2 as metrics
from pyutil.kafka_proxy.kafka_proxy import KafkaProxy
from pyutil.program.conf import Conf
from ss_lib.topology.util.spout_data_producer import SpoutDataProducer

import os, sys, ujson, logging, signal, time, multiprocessing, ctypes, json, traceback, random, \
    math, uuid, threading, Queue, base64, uuid
import ss_lib.topology.util.subprocess_caller as process_caller
import ss_lib.topology.util.storm as storm
import ss_lib.topology.util.dsignal as dsignal


class KafkaSpout(storm.Spout):
    def __init__(self):
        self.conf = None

    def init_metrics(self):
        metrics.init(self.conf)
        metrics.define_counter("_{{spout_name}}.throughput", "num")
        metrics.define_counter("_{{spout_name}}.pending_full", "num")
        metrics.define_counter("_{{spout_name}}.queue.size", "num")

    def initialize(self, conf=None, context=None):
        try:
            self.conf = conf

            self.is_debug = self.conf.get("is_debug", 0)
            self.open_ack = self.conf.get("open_ack", {{ack}})
            if self.open_ack:
                self.pending_tuple = {}
                self.pending_threshold = self.conf.get("pending_threshold", 800)

            self.init_metrics()
            
            self.dsignal = dsignal.DSignal(self.conf, handler=self, component_name="kafka_spout")
            if 'index' in self.conf:
                self.topic = self.conf["topic_name"]
                self.consumer_group = self.conf["consumer_group"]
                self.whence = self.conf.get("whence", 1)
                self.proxy = KafkaProxy(topic=self.topic, consumer_group=self.consumer_group, partitions=self.conf["index"], conf=Conf("/opt/tiger/ss_conf/ss/kafka.conf"))
                self.index = self.conf["index"]
                self.offset = self.conf.get("offset",0)
                self.proxy.set_consumer_offset(self.offset, self.whence)
                self.read_num = self.conf.get("read_num", 100)
                self.sleep_time = self.conf.get("sleep_time", 0.05)
                
                self.decay_factor = self.conf.get("decay_factor", 80.0) 
                self.decay_read_num = self.read_num / (self.decay_factor*1.0) 

                self.start_time = time.time()
            else:
                partition_num = self.conf.get("partition")
                self.queue = Queue.Queue(maxsize=500)
                self.producer_list = []
                for i in xrange(partition_num):
                    producer = SpoutDataProducer(self.conf, i, self.queue)
                    self.producer_list.append(producer)
                    producer.start()

            self.base64 = self.conf.get("base64", 0)

            try:
                self.process_caller = process_caller.ProcessCaller(self.conf, \
                        desc=self.conf.get("base_spout_name"))
            except Exception as e:
                logging.exception(e)
            
        except Exception as e:
            logging.exception(e)
            raise

    def on_signal(self, data):
        try:
            if data.get("cmd") == "stop":
                if 'index' in self.conf:
                    self.sleep_time = 1000
                else:
                    for producer in self.producer_list:
                        producer.stop()
            elif data.get("cmd") == "reload":
                pass
            else:
                pass
        except Exception as e:
            logging.exception(e)

    def ack(self, msgid):
        if msgid in self.pending_tuple:
            del self.pending_tuple[msgid]
        else:
            logging.error("there is not msgid in pending_tuple!!!")

    def fail(self, msgid):
        if msgid in self.pending_tuple:
            del self.pending_tuple[msgid]
        else:
            logging.error("there is not msgid in pending_tuple!!!")

    def nextTuple(self):
        try:
            if len(self.pending_tuple) >= self.pending_threshold:
                metrics.emit_counter("_{{spout_name}}.pending_full", 1)
                logging.info("too many tuple pending in spout, pid:%d, pending_size:%d"%(os.getpid(), len(self.pending_tuple)))
                time.sleep(0.05)
                return
            if 'index' in self.conf:
                ts = time.time()
                if (ts - self.start_time) <= self.decay_factor :
                    self.read_num = self.decay_read_num*(ts - self.start_time)
                msgs = self.proxy.fetch_msgs(count=int(self.read_num))
                index = self.index
                if not msgs:
                    storm.log("%s return not normal"%str(msgs))
                    time.sleep(0.010)
                    return
                time.sleep(self.sleep_time)
            else:
                index, msgs = self.queue.get()
                metrics.emit_counter("_{{spout_name}}.queue.size", self.queue.qsize())

            if self.is_debug:
                print "%d:%s"%(index, msgs)
            else:
                req_tup = []
                if self.base64:
                    for line in msgs:
                        req_tup.append([base64.b64encode(line)])
                else:
                    for line in msgs:
                        req_tup.append([line])
                req = {}
                req["batch"] = req_tup
                ret_dict = self.process_caller.exec_task(req)
                ret_tup_list = ret_dict.get("res", [])
                for tup in ret_tup_list:
                    if self.open_ack:
                        msgid = str(uuid.uuid4())
                        self.pending_tuple[msgid] = tup
                        storm.emit(tup,id=msgid)
                    else:
                        storm.emit(tup)
                metrics.emit_counter("_{{spout_name}}.throughput", len(ret_tup_list))
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

