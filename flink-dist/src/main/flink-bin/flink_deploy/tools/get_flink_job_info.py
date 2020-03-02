#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from lark_util import LarkUtils
from yarn_util import YarnUtil

import argparse
import json
import requests
import time
import traceback


ROBOT_TOKEN = 'b-5f196d62-0108-4535-99e6-83071ff7ce2e'


def get_flink_apps(operator, region, cluster, queue):
    yarn_util = YarnUtil()
    if queue:
        return yarn_util.get_flink_running_apps_in_queue(operator, region, cluster, queue)
    else:
        return yarn_util.get_flink_running_apps_in_cluster(operator, region, cluster)


def get_job_id_from_tracking_url(url):
    rep = requests.get(url + "/jobs/overview").text
    try:
        job_id = json.loads(rep)["jobs"][0]["jid"]
        return job_id
    except Exception:
        print(url)
        traceback.print_stack()
        return None


def get_latest_checkpoint_size(url, jid):
    rep = requests.get(url + "/jobs/" + jid + "/checkpoints").text
    try:
        json_rep = json.loads(rep)
    except Exception:
        print(url, jid)
        traceback.print_stack()
        return 0

    if "latest" in json_rep and json_rep["latest"]:
        latest = json_rep["latest"]
        if "completed" in latest and latest["completed"]:
            latest_complete = latest["completed"]
            if "state_size" in latest_complete and latest_complete["state_size"]:
                state_size = latest_complete["state_size"]
                print("Job %s latest state_size = %s, tracking url: %s" % (jid, state_size, url))
                return state_size
    # print("Job %s has no checkpoints, tracking url: %s" % (jid, url))
    return 0


def get_state_size(app):
    tracking_url = app["trackingUrl"]
    if len(tracking_url) > 0:
        job_id = get_job_id_from_tracking_url(tracking_url)
        if job_id:
            size = get_latest_checkpoint_size(tracking_url, job_id) / 1024
            return size
    return 0


def get_memory_vcores(app):
    resource_info = app["resourceInfo"]
    vcores = resource_info["vCores"]
    memory = resource_info["memory"]
    return [vcores, memory]


def get_state_and_resource(flink_apps):
    total_apps = len(flink_apps)
    state_apps = 0
    state_size = 0
    memory = 0
    vcores = 0

    cnt = 0
    for app in flink_apps:
        cnt += 1
        if cnt % 100 == 0:
            print("Total %s, still %s left." % (total_apps, total_apps - cnt))
        size = get_state_size(app)
        if size > 0:
            state_apps += 1
            state_size += size
            resources = get_memory_vcores(app)
            vcores += resources[0]
            memory += resources[1]

    print("Traverse %s apps" % total_apps)
    print("StateSize: %s KB, State Apps: %s, Memory: %s MB, vCores: %s" % (state_size, state_apps, memory, vcores))


def is_docker(url, jid):
    rep = requests.get(url + "/jobs/" + jid + "/config").text
    try:
        json_rep = json.loads(rep)
    except Exception:
        print(url, jid)
        traceback.print_stack()
        return True

    if 'execution-config' in json_rep and 'user-config' in json_rep['execution-config']:
        user_config = json_rep['execution-config']['user-config']
        if 'topology.yaml' in user_config and 'docker.image' not in user_config['topology.yaml']:
            return False
    return True


def check_docker(flink_apps):
    lark_util = LarkUtils(ROBOT_TOKEN)
    total_apps = len(flink_apps)
    no_docker_apps = 0
    cnt = 0
    for app in flink_apps:
        cnt += 1
        if cnt % 5 == 0:
            print("Total %s, still %s left. no docker apps %s" % (total_apps, total_apps - cnt, no_docker_apps))
        tracking_url = app["trackingUrl"]
        if len(tracking_url) > 0:
            job_id = get_job_id_from_tracking_url(tracking_url)
            if job_id:
                if not is_docker(tracking_url, job_id):
                    no_docker_apps += 1
                    print(app['app_name'], app['user'])
                    alert_msg = "由于 pushonline 下线，无法保障物理环境仓库依赖，" \
                                "所以尽快将 PyFlink 任务迁移到 docker 环境下，" \
                                "迁移 Docker 参见该文档： http://flink.bytedance.net/1645/13512/，" \
                                "如果是 bigbang 作业的话，请在 reckon 平台上重启。" \
                                "%s 作业未迁移，请尽快迁移" % str(app['app_name'])
                    time.sleep(1)
                    try:
                        lark_util.send_message(app['user'], alert_msg)
                    except Exception:
                        traceback.print_stack()


COMMANDS = {
    'get_state_and_resource': get_state_and_resource,
    'check_docker': check_docker
}


def traverse_apps(operator, region, cluster, queue, action):
    flink_apps = get_flink_apps(operator, region, cluster, queue)
    COMMANDS[action](flink_apps)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description='Check Flink Jobs')
    parser.add_argument('-r', '--region', type=str, action='store', required=True,
                        help='the region which you want search')
    parser.add_argument('-c', '--cluster', type=str, action='store', required=True,
                        help='the cluster which you want search')
    parser.add_argument('-q', '--queue', type=str, action='store',
                        help='the queue what you want search')
    parser.add_argument('-a', '--action', type=str, action='store', required=True,
                        choices=COMMANDS.keys(),
                        help='the action what you want search')

    args = parser.parse_args()
    print(args.region, args.cluster, args.queue, args.action)
    traverse_apps("tools", args.region, args.cluster, args.queue, args.action)
