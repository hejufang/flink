#!/usr/bin/env python
# -*- coding:utf-8 -*-

import Queue
import os
import sys
import threading

"""
Run this shell with one parameter, which represents the 
location of abnormal_hosts_apps file
"""
user = 'tiger'


class KillThread(threading.Thread):
    def __init__(self, name):
        threading.Thread.__init__(self)
        self.name = name

    def run(self):
        global lines_queue
        while not lines_queue.empty():
            line = lines_queue.get()
            cols = line.split(":")
            host = cols[0].replace(" ", "")
            app = cols[1].replace(" ", "").replace("\n", "")
            do_kill(host, app)


def clean_list(l):
    new_list = []
    for element in l:
        e = element.replace("\n", "")
        new_list.append(e)
    return new_list


def ssh_cmd(host, cmd):
    complete_cmd = "ssh -oStrictHostKeyChecking=no " + \
                   user + "@" + host + " " + cmd
    print "cmd = " + complete_cmd
    r = os.popen(complete_cmd)
    result = r.readlines()
    result = clean_list(result)
    return result


def read_file(path):
    global lines_queue
    lines_queue = Queue.Queue()
    with open(path) as f:
        line = f.readline()
        while line:
            lines_queue.put(line)
            line = f.readline()
    return lines_queue


def do_kill(host, application):
    cmd = "ps -ef | grep " + application + "| awk '{print $2}'"
    pids = ssh_cmd(host, cmd)
    print "pids = "
    print pids
    if len(pids) < 1:
        return
    kill_cmd = ""
    for pid in pids:
        kill_cmd += "kill -9 " + pid + ";"
    kill_cmd = "'" + kill_cmd + "'"
    result = ssh_cmd(host, kill_cmd)
    print "result = " + str(result)


def print_usage():
    print "Usage: " \
          "kill_container_multi_thread.py {abnormal_hosts_apps_file} {user}"


def main():
    global lines_queue
    global user
    if len(sys.argv) < 3:
        print_usage()
        exit()
    else:
        print("abnormal_hosts_apps file : " + sys.argv[1])
        user = sys.argv[2]
        print("do as user : " + user)
        lines_queue = read_file(sys.argv[1])
        i = 0
        while i < thread_num:
            thread = KillThread("my-thread-" + str(i))
            i += 1
            thread.start()


if __name__ == "__main__":
    thread_num = 50
    main()
