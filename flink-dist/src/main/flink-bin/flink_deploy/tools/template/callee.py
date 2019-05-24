#!/bin/env python
# -*- coding:utf-8 -*-

from pyutil.program import metrics2 as metrics
from ss_lib.topology.util.pipe_util import PipeHandler
from ss_lib.pytopology.utils.callee import Callee

import sys, json, ujson, os, sys, signal, logging, time, traceback, select, random


class ProcessCallee(Callee):
    def init(self, conf):
        self.conf = conf
        metrics.init(self.conf)
        self.stream = PipeHandler(stdin=sys.stdin, stdout=sys.stdout, mode=self.conf.get("pipe_mode", 0))
        """
        add your init logic here
        """


    """
    if you need set deal fail tup fail in storm, set res=None
    if you need not emit tuple to next bolt, set res=[]
    if you have tuple emit to next bolt, set res = [(field1,field2),(filed1,field2),(field1,field2)]
    """
    def calc(self, tup_batch):
        res = []
        for tup in tup_batch:
            #your code here
            pass

        return res

    """
    you need not modify run function
    """
    def run(self):
        for req in self.stream:
            try:
                ret = {"res":[]}
                if "batch" in req:
                    batch = req["batch"]
                else:
                    batch = [req["tuple"]]

                ret["res"] = self.calc(batch) 
                self.stream.write_msg(ret)
            except Exception as e:
                logging.exception(e)
                self.stream.write_msg(ret)

def main():
    signal.signal(signal.SIGINT, signal.SIG_DFL)
    signal.signal(signal.SIGTERM, signal.SIG_DFL)
    signal.signal(signal.SIGHUP, signal.SIG_DFL)

    if len(sys.argv) == 1:
        conf = {}
    else:
        conf_str = sys.argv[1]
        conf = ujson.loads(conf_str)
    callee = ProcessCallee(conf)
    try:
        callee.run()
    except:
        logging.exception("callee exception, proces[%d] exit!"%(os.getpid()))

if __name__ == "__main__":
    main()

