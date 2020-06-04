#!/bin/env python
# -*- coding:utf-8 -*-

from pyutil.program import metrics3 as metrics
from pytopology.utils.callee import Callee
import json, logging, os, random, select, signal, sys, traceback, time, ujson


class ProcessCallee(Callee):
    def init(self, conf):
        self.conf = conf
        metrics.init(self.conf)
        # add your init logic here

    def calc(self, tup_batch):
        """ calculate tup_batch by your compute logic.

        Args:
            tup_batch: the result messages passed upstream. This is a list of tuple.

        Returns:
            If you need set deal fail tup fail in pyFlink, set res=None
            If you need not emit tuple to next bolt, set res=[]
            If you have tuple emit to next bolt, set res = [(field1,field2),(filed1,field2),(field1,field2)]
        """

        res = []
        for tup in tup_batch:
            # your code here
            pass

        return res
