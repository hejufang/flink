#!/usr/bin/env python
# -*- coding:utf-8 -*-

import threading
import os
import sys

from metric_util import MetricUtil
from common_util import CommonUtil

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))


class JobStatusCheckThread(threading.Thread):
    def __init__(self, name, pending_job_queue):
        threading.Thread.__init__(self)
        self.name = name
        self.pending_job_queue = pending_job_queue

    def run(self):
        while not self.pending_job_queue.empty():
            job = self.pending_job_queue.get()
            job_info = job['job_info']
            cluster_info = job['cluster_info']
            job_name = job['job_name']
            flink_dtop_region = cluster_info['flink_dtop_region']
            metrics_region = cluster_info['metrics_region']
            job_info["resource"] = self.get_app_resource(flink_dtop_region, job_info['application_name'])
            slot_status = self.get_job_slot_status(metrics_region, job_name)
            job_info["resource"]['total_slots'] = slot_status['total_slots']
            job_info["resource"]['available_slots'] = slot_status['available_slots']
            job_info["resource"]['taskmanager_number'] = slot_status['taskmanager_number']
            job_info['job_name'] = job_name
            job_info['restart_count'] = self.get_job_restart_status(cluster_info, job_name)

    @staticmethod
    def get_app_resource(region, app_name):
        type = "application"
        resource = MetricUtil.get_job_resource_metrics(type, region, app_name)
        return resource

    @staticmethod
    def get_job_slot_status(region, jobname):
        total_slots_metrics = "flink.jobmanager.taskSlotsTotal{jobname=%s}" % jobname
        available_slots_metrics = "flink.jobmanager.taskSlotsAvailable{jobname=%s}" % jobname
        tm_num_metrics = "flink.jobmanager.numRegisteredTaskManagers{jobname=%s}" % jobname
        total_slots = MetricUtil.get_resource_metric_max(region, total_slots_metrics)
        available_slots = MetricUtil.get_resource_metric_min(region, available_slots_metrics)
        tm_num = MetricUtil.get_resource_metric_max(region, tm_num_metrics)
        result = {'total_slots': int(total_slots),
                  'available_slots': int(available_slots),
                  'taskmanager_number': int(tm_num)}
        return result

    @staticmethod
    def get_job_restart_status(cluster_info, jobname):
        metrics_region = cluster_info['metrics_region']
        job_restart_metric_name = "flink.job.restart{jobname=%s}" % jobname
        job_restart_dps = MetricUtil.get_metric_data(metrics_region, job_restart_metric_name, agg='sum:1h-sum-zero')
        job_restart = CommonUtil.get_sum_value_from_dict(job_restart_dps)
        return job_restart
