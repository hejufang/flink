#!/usr/bin/env python
# -*- coding: utf-8 -*-

# for example:
# login devlopment machine with user can ssh other yarn cluster's nodes,
# run script like this:
#  /opt/tiger/flink_deploy/tools/check_jar_conflict.py -a check_conflict_lib
#  -r cn -c leser -p org.apache.flink.core,org.apache.flink.runtime

from __future__ import print_function
from yarn_util import YarnUtil
import argparse
import commands
import json
import requests
import socket
import traceback

YARN_USER = "yarn"


def get_flink_apps(operator, region, cluster, queue):
    yarn_util = YarnUtil()
    if queue:
        return yarn_util \
            .get_flink_running_apps_in_queue(operator, region, cluster, queue)
    else:
        return yarn_util \
            .get_flink_running_apps_in_cluster(operator, region, cluster)


def check_conflict_lib(flink_apps, packages, region):
    try:
        if ("cn" != region):
            global YARN_USER
            YARN_USER = "tiger"
        print("yarn_user: " + YARN_USER)
        check_conflict_lib_impl(flink_apps, packages)
    except Exception:
        print("check_lib_impl ex: " + traceback.print_stack())


def check_conflict_lib_impl(flink_apps, packages):
    conflict_info_list = []
    package_list = packages.split(',')
    index = 0
    app_num = len(flink_apps)
    print("start check packages: " + packages + ", num: " + str(app_num))
    # loop judge all running flink_apps in cluster
    for app in flink_apps:
        index = index + 1
        try:
            if (app is None):
                print("app is none, will ignore it")
                continue
            if (len(app["trackingUrl"]) == 0):
                print("get tracking_url error.AppId: " + app["app_id"])
                continue
            ip, container = get_container_cluster_ip(app["trackingUrl"])
            if (ip is None):
                continue
            app_id = app["app_id"]
            app_name = app["app_name"]
            user_name = app["user"]
            print("%s of %s, %s %s %s" %
                  (index, app_num, ip, container, app_id))
            # get yarn job jar dir by cmd
            ls_jar_cmd = "ls /data*/yarn/nmdata/usercache/" \
                         + user_name + "/appcache/" \
                         + app_id + "/" + container + "/*.jar"
            ls_jar_all = (execute_remote(ip, ls_jar_cmd))
            # get jar location from ls dir
            if ('.jar' in ls_jar_all):
                jar_dir_list = ls_jar_all.split('\n')
                last_jar = jar_dir_list[len(jar_dir_list) - 1]
                jar_dir = last_jar[0:last_jar.rfind('/')]
            else:
                print("no jar find: " + ls_jar_cmd)
                continue

            is_conflict = False
            conflict_jar = ""
            user_jar = ""
            # judge all class given whether jar is conflict
            for package in package_list:
                cmd = "find " + jar_dir \
                      + " -name \"*.jar\" -maxdepth 1  " \
                        "! -name \"flink.jar\" -exec grep -Hsli " \
                      + package + " {} \\;"
                cmd_result = (execute_remote(ip, cmd))
                # we get conflict
                if ('.jar' in cmd_result):
                    jar_tmp = cmd_result[cmd_result.rfind('/') + 1:]
                    if (user_jar != jar_tmp):
                        user_jar = user_jar + jar_tmp
                    conflict_jar = conflict_jar + " " + package
                    is_conflict = True
                    print("find conflict, ip: " + ip + ", dir: " + cmd_result)
            # mark conflict_info
            if (is_conflict):
                conflict_info = "appName: " + app_name + ", appId: " + app_id \
                                + ', userName: ' + user_name \
                                + ", lib: " + user_jar \
                                + ", conflict: " + conflict_jar
                conflict_info_list.append(conflict_info)
        except Exception:
            print("check conflict app " + app_id
                  + " ex: " + traceback.print_stack())
            continue

    # now print result
    print("\n\n\n\n\nconflict result: ")
    for conflict_info in conflict_info_list:
        print(conflict_info + "\n")


def execute_remote(ip, cmd):
    ssh_cmd_prefix = "ssh " + YARN_USER + "@" + ip
    status, output = commands \
        .getstatusoutput(ssh_cmd_prefix + " '" + cmd + "' ")
    return output


def get_container_cluster_ip(tracking_url):
    try:
        rep = requests.get(tracking_url + "taskmanagers").text
        if (rep is None or len(rep) == 0):
            print("taskmanagers is not ready")
            return None, None
        taskmanagers = json.loads(rep)["taskmanagers"]
        if (len(taskmanagers) == 0):
            print("taskmanagers is not ready")
            return None, None
        task0_info = taskmanagers[0]
        path = task0_info["path"]
        begin = path.find("@")
        end = path.find(":", begin + 1)
        container_name = task0_info["id"]
        return socket.gethostbyname(path[begin + 1:end]), container_name
    except Exception:
        print("get container cluster ip error." + tracking_url)
        return None, None


COMMANDS = {
    'check_conflict_lib': check_conflict_lib
}


def check_apps(operator, region, cluster, queue, action, packages):
    flink_apps = get_flink_apps(operator, region, cluster, queue)
    COMMANDS[action](flink_apps, packages, region)


if __name__ == "__main__":
    parser = argparse \
        .ArgumentParser(description='Check Flink Job Libs Conflict')
    parser.add_argument('-r', '--region', type=str,
                        action='store', required=True,
                        help='the region which you want search')
    parser.add_argument('-c', '--cluster', type=str,
                        action='store', required=True,
                        help='the cluster which you want search')
    parser.add_argument('-q', '--queue', type=str, action='store',
                        help='the queue what you want search')
    parser.add_argument('-p', '--packages', type=str, action='store',
                        default='org.apache.flink.core'
                                ',org.apache.flink.runtime',
                        help='lib packages to check if conflict, split by ,')
    parser.add_argument('-a', '--action', type=str, action='store',
                        required=True, choices=COMMANDS.keys(),
                        help='the action what you want search')
    args = parser.parse_args()
    print (args)
    check_apps("tools", args.region, args.cluster,
               args.queue, args.action, args.packages)
