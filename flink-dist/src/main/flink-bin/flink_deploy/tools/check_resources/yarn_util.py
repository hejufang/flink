#!/usr/bin/env python
# -*- coding:utf-8 -*-

import logging
import json
import os
import requests
from common_util import CommonUtil


class YarnUtil(object):
    FETCH_YARN_APP_URL = "/cluster/apps?queue=%s"
    YARN_REQUEST_URL_FORMAT = 'http://%s/ws/v1%s'

    # TODO(zhangguanghui): replace flink_cluser_conf.json by yaop
    current_dir = os.path.split(os.path.realpath(__file__))[0]
    conf_file = os.path.join(current_dir, "flink_cluster_conf.json")
    CONF = CommonUtil.load_conf(conf_file)

    def __init__(self):
        pass

    @staticmethod
    def get_all_cluster():
        result = {}
        for k, v in YarnUtil.CONF.items():
            for cluster in v:
                if k in result:
                    clu = result[k]
                else:
                    clu = []
                clu.append(cluster['cluster_id'])
                result[k] = clu
        return result

    @staticmethod
    def get_all_clusters_info():
        return YarnUtil.CONF

    @staticmethod
    def get_apps_from_queue(region, cluster_name, queue_name):
        result = {}
        res = YarnUtil.make_yarn_request(region, cluster_name,
                                         YarnUtil.FETCH_YARN_APP_URL % queue_name)

        if res['apps'] is not None:
            app_list = res['apps']['app']
            for app in app_list:
                app_name = app['name']
                owner = None
                index = app_name.rfind("_")
                job_name = app_name
                if index != -1:
                    remove_user_app_name = app_name[0: index]
                    owner = app_name[index + 1:]
                    job_name = remove_user_app_name
                if not owner:
                    owner = app['user']
                result[job_name] = {"application_id": app['id'],
                                    "owner": owner,
                                    "application_name": app_name}
        return result

    @staticmethod
    def make_yarn_request(region, cluster_name, url, is_json=True,
                          request_xml=False):
        # Resolve yarn webapp addrs and execute the specified http request
        yarn_rm_webapp_addrs = YarnUtil.get_yarn_rm_webapp_addrs(region,
                                                                 cluster_name)
        retry = 3
        for i in range(0, retry):
            for addr in yarn_rm_webapp_addrs:
                try:
                    real_url = YarnUtil.YARN_REQUEST_URL_FORMAT % (addr, url)
                    headers = {'Accept': 'application/xml'} if request_xml else {}
                    r = requests.get(real_url, allow_redirects=False,
                                     headers=headers, timeout=30)
                    r.encoding = 'utf-8'
                    rt = r.text
                    if rt and rt.find('standby') > -1:
                        continue
                    if r.status_code == 200:
                        return json.loads(rt) if is_json else rt
                except Exception, e:
                    logging.exception(e)
        raise Exception('Out of resourcemanager: %s' % yarn_rm_webapp_addrs)

    @staticmethod
    def get_yarn_rm_webapp_addrs(region, cluster_name):
        for cluster in YarnUtil.CONF[region]:
            if cluster['cluster_id'] == cluster_name:
                return cluster['rms']
        raise Exception("Cluster not found : " + region + "." + cluster_name)
