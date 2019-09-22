#!/usr/bin/env python
# -*- coding:utf-8 -*-

import json


class CommonUtil(object):
    @staticmethod
    def get_max_value_from_dict(dic):
        max_value = 0
        if not dic:
            return max_value
        for value in dic.values():
            max_value = max(max_value, value)
        return max_value

    @staticmethod
    def get_sum_value_from_dict(dic):
        sum_value = 0
        if not dic:
            return sum_value
        for value in dic.values():
            sum_value += value
        return sum_value

    @staticmethod
    def load_conf(conf_file):
        with open(conf_file, 'r') as f:
            conf = json.load(f)
        return conf
