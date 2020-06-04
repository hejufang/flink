#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import os
import sys
import threading

"""
Generates a file: "abnormal_hosts_apps" in current path, which contains
abnormal host-application pairs.

We should run this as 'yarn' if the cluster is in China, otherwise run as 'tiger'

Usage: check_application_on_yarn.py {cluster}
Available cluster: cn, va_aws, sg_aliyun

Example: 
check_application_on_yarn.py cn
"""

# TODO: Extract a common module
RUNNING_APP_CMD = "/opt/tiger/yarn_deploy/hadoop/bin/yarn application -list " \
                  "| grep -v KILLED| grep -v FAILED" \
                  "| grep -v FINISHED | awk '{print $1}'"

CONFS = {'cn': {'RM': '10.10.45.140',
                'TAG': 'inf.hadoop.compute.flink'},
         'dw': {'RM': '10.10.45.39',
                'TAG': 'inf.hadoop.compute.dw.flink'},
         'wj': {'RM': '10.31.42.31',
                'TAG': 'inf.hadoop.compute.wj.flink'},
         'hl': {'RM': '10.20.52.38',
                'TAG': 'inf.hadoop.compute.hl.flink'},
         'va_aws': {'RM': '10.100.6.56',
                    'TAG': 'aws.va.inf.hadoop.compute.default.flink'},
         'sg_aliyun':
             {'RM': '10.115.5.188',
              'TAG': 'alicloud.sg.inf.hadoop.compute.default.flink'}
         }
CONF = {}


class CheckThread(threading.Thread):
    def __init__(self, name, apps):
        threading.Thread.__init__(self)
        self.name = name
        self.applications = apps

    def run(self):
        global host_queue
        find_app_cmd = "ps -ef | grep /opt/tiger/yarn_deploy/" \
                       "hadoop/bin/container-executor |" \
                       " grep -v grep | awk '{print $12}' | sort | uniq"
        while not host_queue.empty():
            host_name = host_queue.get()
            apps_on_this_host = ssh_cmd(host_name, find_app_cmd)
            for app in apps_on_this_host:
                if app not in self.applications:
                    print "abnormal app: " + app
                    os.popen("echo " + host_name + ": " + app
                             + " >>abnormal_hosts_apps")
                    os.popen("echo " + app + " >>abnormal_apps")


# remove '\n' of each element of list
def clean_list(l):
    new_list = []
    for element in l:
        e = element.replace("\n", "")
        new_list.append(e)
    return new_list


def ssh_cmd(host, cmd):
    r = os.popen("ssh -oStrictHostKeyChecking=no " + host + " " + cmd)
    result = r.readlines()
    result = clean_list(result)
    return result


def print_usage():
    print "Usage: check_application_on_yarn.py {cluster} \n" \
          "Available cluster:" + str(CONFS.keys())


def save_running_job(apps):
    if len(apps) < 1:
        exit("No running applications, please make sure your RM is right")
    for app in apps:
        os.popen("echo " + app + " >> running_apps")


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
        print("Resource Manager ip : " + CONF['RM'])
        print("TAG : " + CONF['TAG'])
    os.popen('rm abnormal_hosts_apps; rm abnormal_apps; rm running_apps')
    applications = ssh_cmd(CONF['RM'], RUNNING_APP_CMD)
    save_running_job(applications)
    host_cmd = "lh -s " + CONF['TAG']
    hosts = os.popen(host_cmd).readlines()
    hosts = clean_list(hosts)

    host_queue = Queue.Queue()
    for host in hosts:
        host_queue.put(host)

    i = 0
    while i < thread_num:
        thread = CheckThread("my-thread-" + str(i), applications)
        i += 1
        thread.start()


if __name__ == '__main__':
    thread_num = 50
    main()
