#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import argparse
import os
import sys

from yarn_util import YarnUtil
from common_util import CommonUtil
from job_status_check_thread import JobStatusCheckThread

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))
AVAILABLE_DCS = ['cn', 'sg', 'va']
AVAILABLE_CLUSTERS = ['flink', 'dw', 'lepad', 'hyrax', 'horse', 'alisg', 'maliva']
SORT_TYPES = ['cpu', 'mem', 'slot', 'restart']
MAX_THREAD_NUM_PER_QUEUE = 5
THREAD_JOIN_TIMEOUT_SECONDS = 300


def check_app_resource_in_queue(region, cluster, queue, sort_type):
    """
    Check applications resource usage in queue.
    """
    jobs = YarnUtil.get_apps_from_queue(region, cluster, queue)
    cluster_info = get_cluster_info(region, cluster)
    pending_job_queue = Queue.Queue()
    for job_name in jobs:
        job = jobs[job_name]
        job['region'] = region
        job['cluster'] = cluster
        job['queue'] = queue
        new_job = {'job_info': job, 'cluster_info': cluster_info, 'region': region,
                   'queue': queue, 'job_name': job_name}
        pending_job_queue.put(new_job)

    thread_list = []
    thread_num = min(len(jobs), MAX_THREAD_NUM_PER_QUEUE)
    for i in range(0, thread_num, 1):
        thread = JobStatusCheckThread('job-status-check' + str(i), pending_job_queue)
        thread_list.append(thread)
        thread.start()

    for th in thread_list:
        th.join(THREAD_JOIN_TIMEOUT_SECONDS)

    if sort_type == "cpu":
        print_jobs(sort_by_cpu_idle(jobs))
    elif sort_type == "mem":
        print_jobs(sort_by_memory_idle(jobs))
    elif sort_type == "slot":
        print_jobs(sort_by_slot_idle(jobs))
    elif sort_type == "restart":
        print_jobs(sort_by_restart(jobs))

    return jobs


def sort_by_memory_idle(jobs):
    return sorted(jobs.items(), key=lambda kv: kv[1]['resource']['memory_total_idle'], reverse=True)


def sort_by_cpu_idle(jobs):
    return sorted(jobs.items(), key=lambda kv: kv[1]['resource']['vcores_idle'], reverse=True)


def sort_by_slot_idle(jobs):
    return sorted(jobs.items(), key=lambda kv: kv[1]['resource']['available_slots'], reverse=True)


def sort_by_restart(jobs):
    return sorted(jobs.items(), key=lambda kv: kv[1]['restart_count'])


def print_jobs(jobs):
    print_list = []
    title = "{owner:15}{job_name:60}" \
            "{total_cpu:10}{used_cpu:10}{idle_cpu:10}" \
            "{total_mem:15}{used_mem:15}{idle_mem:15}" \
            "{total_slots:10}{idle_slots:10}" \
            "{restart:15}".format(owner='owner', job_name='job_name',
                               total_cpu='alloc_cpu', used_cpu='used_cpu', idle_cpu='idle_cpu',
                               total_mem='alloc_mem', used_mem='used_mem', idle_mem='idle_mem',
                               total_slots='total_slots', idle_slots='idle_slots',
                               restart='restart')
    print_list.append(title)

    for job in jobs:
        job_name = job[0]
        job_info = job[1]
        owner = job_info['owner']
        total_cpu = job_info['resource']['vcores_alloc']
        used_cpu = job_info['resource']['vcores_used']
        idle_cpu = job_info['resource']['vcores_idle']
        total_mem = job_info['resource']['memory_total_alloc']
        used_mem = job_info['resource']['memory_total_used']
        idle_mem = job_info['resource']['memory_total_idle']
        total_slots = job_info['resource']['total_slots']
        idle_slots = job_info['resource']['available_slots']
        restart_count = job_info['restart_count']
        line = "{owner:15}{job_name:50}" \
               "{total_cpu:15}{used_cpu:15}{idle_cpu:15}" \
               "{total_mem:15}{used_mem:15}{idle_mem:15}" \
               "{total_slots:15}{idle_slots:15}" \
               "{restart:15}".format(owner=owner, job_name=job_name,
                                     total_cpu=total_cpu, used_cpu=used_cpu, idle_cpu = idle_cpu,
                                     total_mem=total_mem, used_mem=used_mem, idle_mem=idle_mem,
                                     total_slots=total_slots, idle_slots=idle_slots,
                                     restart=restart_count)
        print_list.append(line)
    for l in print_list:
        print l


def get_cluster_info(region_id, cluster_id):
    current_dir_tmp = os.path.split(os.path.realpath(__file__))[0]
    conf_file = os.path.join(current_dir_tmp, "flink_cluster_conf.json")
    regions_info = CommonUtil.load_conf(conf_file)
    for cluster_info in regions_info[region_id]:
        if cluster_info['cluster_id'] == cluster_id:
            return cluster_info
    raise Exception("no cluster:" + region_id + "/" + cluster_id)


def print_usage():
    print "Usage: check_resource.py {region} {cluster} {queue}"


def parse_cmd():
    parse = argparse.ArgumentParser()
    parse.add_argument('-r', '--region', default='cn', required=True, choices=AVAILABLE_DCS)
    parse.add_argument('-c', '--cluster', default='flink', required=True, choices=AVAILABLE_CLUSTERS)
    parse.add_argument('-q', '--queue', required=True)
    parse.add_argument('-s', '--sort_type', choices=SORT_TYPES, required=True)
    return parse


if __name__ == "__main__":
    args = parse_cmd().parse_args()
    region = args.region
    cluster = args.cluster
    queue = args.queue
    sort_type = args.sort_type
    check_app_resource_in_queue(region, cluster, queue, sort_type)
