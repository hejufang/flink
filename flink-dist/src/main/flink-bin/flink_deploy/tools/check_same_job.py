#!/usr/bin/env python
# -*- coding: utf-8 -*-

import sys
sys.path.insert(0, "/opt/tiger")
import time

from alert import Alert
from lark_util import LarkUtils
from yarn_util import YarnUtil

INF_STREAM_ALERT = "6765347458630287630"


def get_repeated_jobs(running_jobs):
    apps = set()
    result = []
    for app in running_jobs:
        app_name = app['app_name']
        if app_name.startswith('DP_DTS'):
            continue

        if app['app_name'] in apps:
            result.append(app['app_name'])
        else:
            apps.add(app['app_name'])
    return result


if __name__ == '__main__':
    lark = LarkUtils(Alert.ROBOT_TOKEN)
    operator = 'check_same_job'
    region_clusters = YarnUtil.get_all_clusters(operator)
    result = {}
    for region, clusters in region_clusters.items():
        for cluster in clusters:
            time.sleep(30)
            flink_running_apps = YarnUtil.get_flink_running_apps_in_cluster(operator, region, cluster)
            repeated_apps = get_repeated_jobs(flink_running_apps)
            if len(repeated_apps) > 0:
                alert_msg = "%s, %s, %s" % (region, cluster, ','.join(repeated_apps))
                print alert_msg
                lark.post_message(INF_STREAM_ALERT, alert_msg)
