import os
import zookeeper


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
