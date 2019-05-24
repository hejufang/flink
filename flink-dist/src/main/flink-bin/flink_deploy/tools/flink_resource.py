#!/usr/bin/env python
# -*- coding: utf-8 -*-


class FlinkResource(object):
    def __init__(self):
        self.tm_slot = None
        self.tm_memory = None
        self.tm_num = None
        self.tm_cores = None
        self.jm_memory = None
        self.jm_cores = None
        self.cpu_cores = None
        self.memoryMB = None
        self.container_cutoff = 0.5
        self.bolts = []
        self.spouts = []

    @staticmethod
    def create_by_dict(obj_map):
        res = FlinkResource()
        res.from_dict(obj_map)
        return res

    def from_dict(self, obj_map):
        for a, v in obj_map.items():
            setattr(self, a, v)
