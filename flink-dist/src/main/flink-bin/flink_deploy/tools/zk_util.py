import os
import zookeeper
from yarn_util import YarnUtil
from yaml_util import YamlUtil
import time

def clear_zk(zk_info, app_id):
    server = zk_info['server']
    root_path = zk_info['root_path']
    zk = zookeeper.init(server)
    zk_path = os.path.join(root_path, app_id)

    # Check if the zk_path is valid,
    # in case the incorrect operation happened
    if "application_" in zk_path:
        delete_zk_node_recursively(zk, zk_path)
    else:
        print ("Cannot delete invalid zk path: " + zk_path)


def delete_zk_node_recursively(zk, path):
    children = zookeeper.get_children(zk, path)
    for child in children:
        child_path = os.path.join(path, child)
        delete_zk_node_recursively(zk, child_path)
    zookeeper.delete(zk, path)


def get_zk_info(conf):
    zk_info = {}
    server = conf.get("high-availability.zookeeper.quorum")
    root_path = conf.get("high-availability.zookeeper.path.root")
    zk_info['server'] = server
    zk_info['root_path'] = root_path
    return zk_info

def get_expired_nodes(zk_info, region, cluster):
    server = zk_info['server']
    zk = zookeeper.init(server)
    path = "/%s/%s/flink" % (region, cluster)
    children = zookeeper.get_children(zk, path)
    nodes = set()
    for child in children:
        if "application_" in child:
            nodes.add(child)
    return nodes

def get_clusters_with_server(yaml_info, server):
    clusters = []
    for cluster in yaml_info:
        if "high-availability.zookeeper.quorum" in yaml_info[cluster]:
            if yaml_info[cluster]["high-availability.zookeeper.quorum"] == server:
                clusters.append(cluster)
    return clusters

def clean_expired_nodes(region, cluster):
    yaml_info = YamlUtil.get_yaml_info("flink-conf.yaml")
    server = yaml_info[cluster]["high-availability.zookeeper.quorum"]
    zk_info = {"server": server}
    nodes = get_expired_nodes(zk_info, region, cluster)
    apps = []
    clusters = get_clusters_with_server(yaml_info, server)
    for cl in clusters:
        cluster_apps = YarnUtil.get_flink_running_apps_in_cluster("liaojiayi", region, cl)
        print("cluster: %s, apps: %s" % (cl, len(cluster_apps)))
        apps.extend(cluster_apps)
    app_ids = map(lambda x: x['app_id'], apps)
    print("region : %s, cluster: %s, zk: %s" % (region, ",".join(clusters), server))
    print("all zk nodes : %s" % len(nodes))
    print("all running apps : %s" % len(app_ids))
    for app_id in app_ids:
        if app_id in nodes:
            nodes.remove(app_id)
        elif app_id not in nodes:
            print("App %s does not have zk nodes." % app_id)
    print("We're going to delete %s nodes" % len(nodes))

    zk = zookeeper.init(server)
    for node in nodes:
        path = "/%s/%s/flink/%s" % (region, cluster, node)
        print("Deleting zk node %s" % path)
        # delete zk node
        delete_zk_node_recursively(zk, path)
        time.sleep(30)

if __name__ == "__main__":
    region = "cn"
    cluster = "horse"

    # clean_expired_nodes(region, cluster)
