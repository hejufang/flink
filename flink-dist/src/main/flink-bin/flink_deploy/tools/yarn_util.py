#!/usr/bin/env python
# -*- coding:utf-8 -*-


from http_util import HttpUtil


class YarnUtil(object):
    """
        uses to check job exists or not and kill job
    """

    GET_ALL_YARN_REGION_URL = "/v2/region?operator=%s"
    GET_ALL_YARN_CLUSTER_URL = "/v2/cluster?operator=%s"
    GET_CLUSTER_APP_URL = "/v2/apps?operator=%s&region=%s&cluster_id=%s&limit=1000000"
    GET_QUEUE_APP_URL = "/v2/apps?operator=%s&region=%s&cluster_id=%s&queue=%s&limit=100000"
    GET_QUEUE_INFO_URL = "/v2/queue?operator=%s&region=%s&cluster_id=%s&name=%s"
    GET_QUEUE_RESOURCE_URL = "/v2/queue_check?operator=%s&action=queue_now&region=%s&cluster=%s&queue=%s"
    KILL_APP_URL = "/v2/app_kill?operator=%s&region=%s&cluster_id=%s&app_id=%s"
    GET_APP_INFO_URL = "/v2/app?operator=%s&region=%s&cluster_id=%s&app_id=%s"

    YAOP_HOST = "http://yaop-flink.bytedance.net"
    YAOP_TOKEN = "7693dd6d6beaa3a69401fb30efc2211e"
    APP_ID_KEY = "app_id"
    APP_NAME_KEY = "app_name"
    APP_URL_KEY = "trackingUrl"
    APP_STATE = "state"
    APP_RUNNING = "NOT_FINISHED"
    APP_TYPE = "applicationType"
    FLINK = "Apache Flink"
    NAME = "name"
    YAOP_RESPONSE_RESULT_KEY = "result"
    YAOP_KILL_SUCCESS = "success"
    YAOP_RESPONSE_DATA_KEY = "data"

    VCORES = "vCores"
    MEMORY = "memory"
    MEM_MAX_MB_KEY = "max_mem_mb"
    MEM_MIN_MB_KEY = "min_mem_mb"
    MEM_USED_MB_KEY = "used_mem_mb"
    VCORES_MAX_KEY = "max_cpu"
    VCORES_MIN_KEY = "min_cpu"
    VCORES_USED_KEY = "used_cpu"

    def __init__(self):
        pass

    @staticmethod
    def is_exists(operator, region, cluster, queue, job_name):
        exists = job_name in YarnUtil.get_running_apps_map_in_queue(operator,
                                                                    region,
                                                                    cluster,
                                                                    queue)
        return exists

    @staticmethod
    def get_running_application_id(operator, region, cluster, queue, job_name):
        apps = YarnUtil.get_apps_with_job_name(operator, region, cluster, queue, job_name,
                                               YarnUtil.APP_RUNNING)
        if len(apps) > 0:
            return apps[0].get(YarnUtil.APP_ID_KEY)
        return None

    @staticmethod
    def get_application_id_list(operator, region, cluster, queue, job_name):
        app_id_list = []
        apps = YarnUtil.get_apps_with_job_name(operator, region, cluster, queue, job_name)
        for app in apps:
            app_id = app.get(YarnUtil.APP_ID_KEY)
            app_id_list.append(app_id)
        return app_id_list

    @staticmethod
    def get_running_ui_url(operator, region, cluster, queue, job_name):
        apps = YarnUtil.get_apps_with_job_name(operator, region, cluster, queue, job_name,
                                               YarnUtil.APP_RUNNING)
        if len(apps) > 0:
            return apps[0].get(YarnUtil.APP_URL_KEY)
        return None

    @staticmethod
    def get_job_ui_url_list(operator, region, cluster, queue, job_name):
        result = []
        apps = YarnUtil.get_apps_with_job_name(operator, region, cluster, queue, job_name)
        for app in apps:
            url = app.get(YarnUtil.APP_URL_KEY)
            result.append(url)
        return result

    @staticmethod
    def get_running_job_info(operator, region, cluster, queue, job_name):
        app_id = YarnUtil.get_running_application_id(operator, region, cluster,
                                                     queue, job_name)
        resp = YarnUtil.get_running_app_info(operator, region, cluster, app_id)
        return resp

    @staticmethod
    def get_job_info_list(operator, region, cluster, queue, job_name):
        result = []
        app_ids = YarnUtil.get_application_id_list(operator, region, cluster,
                                                   queue, job_name)
        for app_id in app_ids:
            resp = YarnUtil.get_running_app_info(operator, region, cluster, app_id)
            result.append(resp)
        return result

    @staticmethod
    def get_running_app_info(operator, region, cluster, app_id):
        url = YarnUtil.GET_APP_INFO_URL % (operator, region, cluster, app_id)
        result = YarnUtil.make_yarn_request_get(url)
        data = result.get(YarnUtil.YAOP_RESPONSE_DATA_KEY, [])
        if data and len(data) > 0:
            return data[0]
        return None

    @staticmethod
    def kill_job(operator, region, cluster, queue, job_name):
        app_id = YarnUtil.get_running_application_id(operator, region, cluster,
                                                     queue, job_name)
        if not app_id:
            print "Job %s not exist in cluster: %s" % (job_name, cluster)
            result = True
        else:
            resp = YarnUtil.kill_app(operator, region, cluster, app_id)
            kill_result = resp.get(YarnUtil.YAOP_RESPONSE_RESULT_KEY)
            if kill_result == YarnUtil.YAOP_KILL_SUCCESS:
                print "Kill job: %s in cluster: %s successfully!" \
                      % (job_name, cluster)
                result = True
            else:
                print "Failed to kill job: %s in cluster: %s!" \
                      % (job_name, cluster)
                print resp
                result = False
        return result

    @staticmethod
    def kill_app(operator, region, cluster, app_id):
        url = YarnUtil.KILL_APP_URL % (operator, region, cluster, app_id)
        result = YarnUtil.make_yarn_request_get(url)
        return result

    @staticmethod
    def get_queue_resources(operator, region, cluster, queue):
        response = YarnUtil.get_queue_info(operator, region, cluster, queue)
        return response

    @staticmethod
    def get_queue_info(operator, region, cluster, queue):
        url = YarnUtil.GET_QUEUE_RESOURCE_URL % (operator, region, cluster, queue)
        result = YarnUtil.make_yarn_request_get(url)
        return result

    @staticmethod
    def get_queue_resource(operator, region, cluster, queue):
        queue_info = YarnUtil.get_queue_info(operator, region, cluster, queue)
        max_resources = queue_info.get("maxResources")
        min_resources = queue_info.get("minResources")
        used_resources = queue_info.get("usedResources")
        resource = {
            YarnUtil.MEM_MAX_MB_KEY: max_resources.get(YarnUtil.MEMORY),
            YarnUtil.MEM_MIN_MB_KEY: min_resources.get(YarnUtil.MEMORY),
            YarnUtil.MEM_USED_MB_KEY: used_resources.get(YarnUtil.MEMORY),
            YarnUtil.VCORES_MAX_KEY: max_resources.get(YarnUtil.VCORES),
            YarnUtil.VCORES_MIN_KEY: min_resources.get(YarnUtil.VCORES),
            YarnUtil.VCORES_USED_KEY: used_resources.get(YarnUtil.VCORES)}
        return resource

    @staticmethod
    def get_apps_with_job_name(operator, region, cluster, queue, job_name, state=None):
        result = []
        if queue:
            apps = YarnUtil.get_apps_in_queue(operator, region, cluster, queue, state)
        else:
            apps = YarnUtil.get_apps_in_cluster(operator, region, cluster, state)
        for app in apps:
            app_name = app.get(YarnUtil.APP_NAME_KEY)
            job_name_temp = YarnUtil.parse_job_name(app_name)
            if job_name_temp == job_name:
                result.append(app)
        return result

    @staticmethod
    def get_running_apps_in_queue(operator, region, cluster, queue):
        apps_map = YarnUtil.get_apps_map_in_queue(operator, region, cluster, queue,
                                                  YarnUtil.APP_RUNNING)
        return apps_map.values()

    @staticmethod
    def get_running_apps_map_in_queue(operator, region, cluster, queue):
        return YarnUtil.get_apps_map_in_queue(operator, region, cluster, queue,
                                              YarnUtil.APP_RUNNING)

    @staticmethod
    def get_apps_map_in_queue(operator, region, cluster, queue, state=None):
        result = {}
        apps = YarnUtil.get_apps_in_queue(operator, region, cluster, queue, state)
        for app in apps:
            if YarnUtil.APP_ID_KEY in app and YarnUtil.APP_NAME_KEY in app:
                app_name = app.get(YarnUtil.APP_NAME_KEY)
                job_name = YarnUtil.parse_job_name(app_name)
                result[job_name] = app
        return result

    @staticmethod
    def get_apps_in_queue(operator, region, cluster, queue, state=None):
        url = YarnUtil.GET_QUEUE_APP_URL % (operator, region, cluster, queue)
        if state:
            url += "&%s=%s" % (YarnUtil.APP_STATE, state)
        result = YarnUtil.make_yarn_request_get(url)
        apps = result.get(YarnUtil.YAOP_RESPONSE_DATA_KEY, [])
        return apps

    @staticmethod
    def get_apps_in_cluster(operator, region, cluster, state=None):
        url = YarnUtil.GET_CLUSTER_APP_URL % (operator, region, cluster)
        if state:
            url += "&%s=%s" % (YarnUtil.APP_STATE, state)
        result = YarnUtil.make_yarn_request_get(url)
        apps = result.get(YarnUtil.YAOP_RESPONSE_DATA_KEY, [])
        return apps

    @staticmethod
    def get_running_apps_in_cluster(operator, region, cluster):
        return YarnUtil.get_apps_in_cluster(operator, region, cluster,
                                            YarnUtil.APP_RUNNING)

    @staticmethod
    def get_flink_running_apps_in_cluster(operator, region, cluster):
        all_apps = YarnUtil.get_running_apps_in_cluster(operator, region, cluster)
        result = []
        for app in all_apps:
            if app.get(YarnUtil.APP_TYPE) == YarnUtil.FLINK:
                result.append(app)
        return result

    @staticmethod
    def get_flink_running_app_num_by_cluster(operator):
        clusters = YarnUtil.get_all_clusters(operator)
        result = {}
        for region, clusters in clusters.items():
            for cluster in clusters:
                count = YarnUtil.get_flink_running_app_num_in_cluster(operator,
                                                                      region,
                                                                      cluster)
                print "%s: %s count: %s" % (region, cluster, count)
                if region in result:
                    result[region] += count
                else:
                    result[region] = count
        print result
        return result

    @staticmethod
    def get_all_flink_running_app_num(operator):
        clusters = YarnUtil.get_all_clusters(operator)
        count = 0
        for region, clusters in clusters.items():
            for cluster in clusters:
                count += YarnUtil.get_flink_running_app_num_in_cluster(operator,
                                                                       region,
                                                                       cluster)
        return count

    @staticmethod
    def get_flink_running_app_num_in_cluster(operator, region, cluster):
        flink_running_apps = YarnUtil.get_flink_running_apps_in_cluster(operator,
                                                                        region,
                                                                        cluster)
        return len(flink_running_apps)

    @staticmethod
    def get_all_regions(operator):
        url = YarnUtil.GET_ALL_YARN_REGION_URL % operator
        result = YarnUtil.make_yarn_request_get(url)
        regions = result.get(YarnUtil.YAOP_RESPONSE_DATA_KEY, [])
        result = []
        for region in regions:
            result.append(region.get(YarnUtil.NAME))
        return result

    @staticmethod
    def get_all_clusters(operator):
        url = YarnUtil.GET_ALL_YARN_CLUSTER_URL % operator
        result = YarnUtil.make_yarn_request_get(url)
        data = result.get(YarnUtil.YAOP_RESPONSE_DATA_KEY, [])
        result = {}
        for region, dcs in data.items():
            for dc, clusters in dcs.items():
                for cluster in clusters:
                    cluster_name = cluster.get(YarnUtil.NAME)
                    if region not in result:
                        result[region] = [cluster_name]
                    else:
                        result[region].append(cluster_name)
        return result

    @staticmethod
    def make_yarn_request_get(path):
        url = YarnUtil.YAOP_HOST + path
        headers = \
            {"Authorization": "YaopToken " + YarnUtil.YAOP_TOKEN}
        (sec, result) = HttpUtil.do_get(url, headers=headers, allow_redirects=False)
        return result

    @staticmethod
    def parse_job_name(app_name):
        index = app_name.rfind("_")
        if index != -1:
            job_name = app_name[0: index]
        else:
            job_name = app_name
        return job_name


if __name__ == '__main__':
    app_num = YarnUtil.get_flink_running_app_num_by_cluster("test")
    print app_num
