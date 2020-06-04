#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import os
import sys
import threading
import time

"""
Generates a file: "redundant_job_manager" in current path.

We should run this as 'yarn' if the cluster is in China, 
otherwise run as 'tiger'

Usage: check_redundant_job_manager.py {cluster}
Available cluster: cn, va_aws, sg_aliyun

Example: 
check_redundant_job_manager.py cn
"""

RUNNING_APP_CMD = "/opt/tiger/yarn_deploy/hadoop/bin/yarn application -list " \
                  "| grep -v KILLED| grep -v FAILED" \
                  "| grep -v FINISHED | awk '{print $1}'"

CONFS = {'cn': {'RM': '10.10.45.140',
                'TAG': 'inf.hadoop.compute.flink'},
         'dw': {'RM': '10.10.45.39',
                'TAG': 'inf.hadoop.compute.dw.flink'},
         'wj': {'RM': '10.31.42.31',
                'TAG': 'inf.hadoop.compute.wj.flink'},
         'va_aws': {'RM': '10.100.6.56',
                    'TAG': 'aws.va.inf.hadoop.compute.default.flink'},
         'sg_aliyun':
             {'RM': '10.115.5.188',
              'TAG': 'alicloud.sg.inf.hadoop.compute.default.flink'}
         }
CONF = {}


class CheckThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        global host_queue
        find_app_cmd = "ps -ef | grep /opt/tiger/yarn_deploy/" \
                       "hadoop/bin/container-executor |" \
                       " grep -v grep | awk '{print $12,$13}' | sort | uniq"
        while not host_queue.empty():
            host_name = host_queue.get()
            containers_on_this_host = ssh_cmd(host_name, find_app_cmd)
            for container in containers_on_this_host:
                os.popen("echo " + host_name + " " + container +
                         " >>running_containers")


# remove '\n' of each element of list
def clean_list(l):
    new_list = []
    for element in l:
        e = element.replace("\n", "")
        new_list.append(e)
    return new_list


def ssh_cmd(host, cmd):
    complete_cmd = "ssh -oStrictHostKeyChecking=no " + host + " " + cmd
    print 'cmd = ' + complete_cmd
    r = os.popen(complete_cmd)
    result = r.readlines()
    result = clean_list(result)
    return result


def print_usage():
    print "Usage: check_redundant_job_manager.py {cluster} \n" \
          "Available cluster:" + str(CONFS.keys())


def get_retry_count(c_id):
    if len(c_id.split('_')) < 2:
        return -1
    length = len(c_id.split('_'))
    return int(c_id.split('_')[length - 2])


def count_job_manager():
    app_job_manager = {}
    with open("running_containers") as f:
        line = f.readline()
        while line:
            line = line.replace('\n', '')
            if len(line.split(' ')) < 3:
                line = f.readline()
                print 'line = ' + line
                continue
            host1 = line.split(' ')[0]
            app = line.split(' ')[1]
            container = line.split(' ')[2]
            retry_count = get_retry_count(container)
            host_container = {'host': host1, 'container': container}
            if '_000001' not in container:
                line = f.readline()
                continue
            if app not in app_job_manager:
                app_job_manager[app] = {'host_container': [host_container],
                                        'max_retry_count': retry_count}
            else:
                app_job_manager[app]['max_retry_count'] = \
                    max(retry_count, app_job_manager[app]['max_retry_count'])
                app_job_manager[app]['host_container'].append(host_container)
            line = f.readline()

    for (k, v) in app_job_manager.items():
        max_retry_count = v['max_retry_count']
        host_containers = v['host_container']
        if len(host_containers) > 1:
            os.popen('echo ' + k + ' ' + str(host_containers) +
                     ' >>app_with_redundant_job_manager')
        for host_container in host_containers:
            host = host_container['host']
            container = host_container['container']

            if get_retry_count(container) < max_retry_count:
                os.popen('echo ' + host + ' ' + container +
                         ' >>redundant_job_manager')


def main():
    global host_queue
    global CONF
    if len(sys.argv) < 2:
        print_usage()
        exit()
    else:
        cluster = sys.argv[1]
        if cluster not in CONFS:
            print "Invalid cluster:" + cluster
            print_usage()
            exit()
        CONF = CONFS[cluster]
        print
        print("TAG : " + CONF['TAG'])
    os.popen('rm running_containers;rm redundant_job_manager;'
             'rm app_with_redundant_job_manager')
    host_cmd = "lh -s " + CONF['TAG']
    hosts = os.popen(host_cmd).readlines()
    hosts = clean_list(hosts)

    host_queue = Queue.Queue()
    for host in hosts:
        host_queue.put(host)

    thread_arr = []
    i = 0
    while i < thread_num:
        thread = CheckThread("my-thread-" + str(i))
        thread_arr.append(thread)
        i += 1
        thread.start()
    for th in thread_arr:
        th.join(60)

    count_job_manager()


if __name__ == '__main__':
    thread_num = 100
    main()
