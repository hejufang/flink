#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import math
import os
import requests
import threading
from common_util import CommonUtil


class MetricUtil(object):
    search_metric_parallelism = threading.Semaphore(5)
    current_dir = os.path.split(os.path.realpath(__file__))[0]
    conf_file = os.path.join(current_dir, "metrics_conf.json")
    CONF = CommonUtil.load_conf(conf_file)

    @staticmethod
    def get_metric_dps(metric_url, retry=3):
        for i in range(0, retry):
            try:
                resp = requests.get(metric_url).json()
                if not resp:
                    return None
                return resp[0]['dps']
            except Exception as e:
                continue
        return None

    @staticmethod
    def get_multi_metric_dps(metric_url, retry=3):
        MetricUtil.search_metric_parallelism.acquire()
        dps_list = []
        try:
            for i in range(0, retry):
                try:
                    r = requests.get(metric_url, 10)
                    if not r:
                        return None

                    if r.status_code != 200:
                        logging.info("Failed to get metric from %s", metric_url)
                        continue

                    for item in r.json():
                        dps = item['dps']
                        if len(dps) > 3:
                            dps_list.append(dps)
                    break
                except Exception as e:
                    logging.exception(e)
                    continue
            if len(dps_list) == 0:
                print "metric_url = ", metric_url
        finally:
            MetricUtil.search_metric_parallelism.release()
        return dps_list

    @staticmethod
    def get_metric_data(region, metric_name, start='24h-ago', end='',
                        agg='max:30m-avg', retry=3):
        url = MetricUtil.CONF['metrics_template'] % (
            region, start, end, agg, metric_name)
        return MetricUtil.get_metric_dps(url, retry=retry)

    @staticmethod
    def get_multi_metric_data(region, metric_name, start='24h-ago', end='',
                              agg='max:30m-avg', retry=3):
        url = MetricUtil.CONF['metrics_template'] % (
            region, start, end, agg, metric_name)
        return MetricUtil.get_multi_metric_dps(url, retry=retry)

    @staticmethod
    def get_job_resource_metrics(type, region, app_name):
        conf = MetricUtil.CONF['resource_metrics'][type]
        alloc_cpu_metric = conf['alloc_cpu'] % app_name
        user_cpu_metric = conf['used_cpu'] % app_name

        alloc_mem_metric = conf['alloc_memory'] % app_name
        total_used_mem_metric = conf['used_memory'] % app_name
        alloc_jvm_mem_metric = conf['alloc_jvm_memory'] % app_name
        used_jvm_mem_metric = conf['used_jvm_memory'] % app_name

        alloc_cpu = MetricUtil.get_resource_multi_metric_max(region, alloc_cpu_metric, start='1h-ago',
                                                             agg='max:10m-avg')
        user_cpu = MetricUtil.get_resource_multi_metric_max(region, user_cpu_metric, agg='max:1h-avg')
        alloc_mem = MetricUtil.get_resource_multi_metric_max(region, alloc_mem_metric, start='1h-ago',
                                                             agg='max:10m-avg')
        total_used_mem_cpu = MetricUtil.get_resource_multi_metric_max(region, total_used_mem_metric)
        alloc_jvm_mem = MetricUtil.get_resource_multi_metric_max(region, alloc_jvm_mem_metric, start='1h-ago',
                                                                 agg='max:10m-avg')
        used_jvm_mem = MetricUtil.get_resource_multi_metric_max(region, used_jvm_mem_metric)

        result = dict()
        result['vcores_alloc'] = math.ceil(alloc_cpu)
        result['vcores_used'] = round(user_cpu)
        result['vcores_idle'] = result['vcores_alloc'] - result['vcores_used']
        result['memory_total_alloc'] = math.ceil(alloc_mem)
        result['memory_total_used'] = math.ceil(total_used_mem_cpu)
        result['memory_total_idle'] = result['memory_total_alloc'] - result['memory_total_used']
        result['memory_jvm_alloc'] = math.ceil(alloc_jvm_mem)
        result['memory_jvm_used'] = math.ceil(used_jvm_mem)
        result['memory_jvm_idle'] = result['memory_jvm_alloc'] - result['memory_jvm_used']

        return result

    @staticmethod
    def get_resource_metric_max(region, metrics):
        metrics_list = MetricUtil.get_metric_data(region, metrics)
        max_value = 0
        if not metrics_list:
            return max_value
        for value in metrics_list.values():
            max_value = max(max_value, value)
        return max_value

    @staticmethod
    def get_resource_metric_min(region, metrics, start='24h-ago',
                                agg='min:30m-avg'):
        metrics_list = MetricUtil.get_metric_data(region, metrics, start=start,
                                                  agg=agg)

        if not metrics_list or len(metrics_list) <= 0:
            return 0
        min_value = metrics_list.values()[0]
        for value in metrics_list.values():
            min_value = min(min_value, value)
        return min_value

    @staticmethod
    def get_resource_multi_metric_max(region, metrics, start='24h-ago',
                                      agg='max:30m-avg'):
        metrics_list = MetricUtil.get_multi_metric_data(region, metrics,
                                                        start=start, agg=agg)
        max_value = 0
        # Remove the first point to avoid spike.
        is_the_first_point = True
        if not metrics_list:
            return max_value
        for i in range(0, len(metrics_list), 1):
            for value in metrics_list[i].values():
                if not is_the_first_point:
                    max_value = max(max_value, value)
                else:
                    is_the_first_point = False
        return max_value
