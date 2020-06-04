#coding=utf-8

from yarn_util import YarnUtil
import requests
import json


def get_jobID_from_tracking_url(url):
    rep = requests.get(url + "jobs/overview").text
    try:
        jobID = json.loads(rep)["jobs"][0]["jid"]
        return jobID
    except Exception:
        return None

def get_latest_checkpoint_size(url, jid):
    rep = requests.get(url + "jobs/" + jid + "/checkpoints").text
    try:
        jsonRep = json.loads(rep)
    except Exception:
        return 0
    if "latest" in jsonRep and jsonRep["latest"]:
        latest = jsonRep["latest"]
        if "completed" in latest and latest["completed"]:
            latest_complete = latest["completed"]
            if "state_size" in latest_complete and latest_complete["state_size"]:
                state_size = latest_complete["state_size"]
                print("Job %s latest state_size = %s, tracking url: %s" % (jid, state_size, url))
                return state_size
    # print("Job %s has no checkpoints, tracking url: %s" % (jid, url))
    return 0

def get_state_size(app):
    trackingUrl = app["trackingUrl"]
    if len(trackingUrl) > 0:
        jobId = get_jobID_from_tracking_url(trackingUrl)
        if jobId:
            size = get_latest_checkpoint_size(trackingUrl, jobId) / 1024
            return size
    return 0

def get_memory_vcores(app):
    resourceInfo = app["resourceInfo"]
    vcores = resourceInfo["vCores"]
    memory = resourceInfo["memory"]
    return [vcores, memory]

def traverse_apps(operator, region, cluster):
    yarnUtil = YarnUtil()
    flinkApps = yarnUtil.get_flink_running_apps_in_cluster(operator, region, cluster)

    totalApps = len(flinkApps)
    stateApps = 0
    stateSize = 0
    memory = 0
    vcores = 0

    cnt = 0
    for app in flinkApps:
        cnt += 1
        if cnt % 100 == 0:
            print("Total %s, still %s left." % (totalApps, totalApps - cnt))
        size = get_state_size(app)
        if size > 0:
            stateApps += 1
            stateSize += size
            resources = get_memory_vcores(app)
            vcores += resources[0]
            memory += resources[1]

    print("Traverse %s apps in region: %s , cluster: %s" % (totalApps, region, cluster))
    print("StateSize: %s KB, State Apps: %s, Memory: %s MB, vCores: %s" % (stateSize, stateApps, memory, vcores))

if __name__ == "__main__":
    traverse_apps("user", "region", "cluster")
