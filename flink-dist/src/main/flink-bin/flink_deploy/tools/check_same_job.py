#!/usr/bin/env python
# -*- coding: utf-8 -*-

from __future__ import print_function
from lark_util import LarkUtils
from yarn_util import YarnUtil

import time


INF_STREAM_ALERT = "6765347458630287630"
ROBOT_TOKEN = 'b-5f196d62-0108-4535-99e6-83071ff7ce2e'


'''
TODO(zhangguanghui): Provided by yaop api in the future.
'''
FLINK_CLUSTERS= {
    'cn': ['flink', 'dw', 'lepad', 'hyrax', 'horse'],
    'sg': ['alisg'],
    'va': ['maliva', 'marmt'],
}


def get_all_running_flink_jobs_in_region(operator, region):
    all_flink_running_jobs = []
    clusters = FLINK_CLUSTERS[region]
    for cluster in clusters:
        print('Get jobs from', region, cluster)
        time.sleep(10)
        flink_running_jobs = YarnUtil.get_flink_running_apps_in_cluster(
            operator, region, cluster)
        all_flink_running_jobs.extend(flink_running_jobs)
    return all_flink_running_jobs


def get_repeated_jobs(running_jobs):
    apps = set()
    result = []
    for app in running_jobs:
        if app['app_name'] in apps:
            result.append(app['app_name'])
        else:
            apps.add(app['app_name'])
    return result


if __name__ == '__main__':
    lark = LarkUtils(ROBOT_TOKEN)
    operator = 'check_same_job'
    for region in FLINK_CLUSTERS:
        all_flink_running_jobs = get_all_running_flink_jobs_in_region(operator, region)
        repeated_jobs = get_repeated_jobs(all_flink_running_jobs)
        if len(repeated_jobs) > 0:
            alert_msg = "The repeated jobs on %s: %s" % (region, ','.join(repeated_jobs))
            print(alert_msg)
            lark.post_message(INF_STREAM_ALERT, alert_msg)
