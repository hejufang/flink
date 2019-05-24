#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os


def clear_hdfs(hdfs_info, conf_dir, hadoop_conf_dir, app_id):
    zk_storage_dir = hdfs_info['zk_storage_dir']
    job_work_dir = os.path.join(hdfs_info['job_work_dir'], ".flink/")
    zk_storage_path = os.path.join(zk_storage_dir, app_id)
    job_work_path = os.path.join(job_work_dir, app_id)
    delete_application_hdfs_path(conf_dir, hadoop_conf_dir, zk_storage_path)
    delete_application_hdfs_path(conf_dir, hadoop_conf_dir, job_work_path)


def delete_application_hdfs_path(conf_dir, hadoop_conf_dir, path):
    HADOOP_OPTS = "-Dlog4j.configuration=file:" + \
               os.path.join(conf_dir, "log4j-cli.properties")
    os.environ['HADOOP_CONF_DIR'] = hadoop_conf_dir
    os.environ['HADOOP_OPTS'] = HADOOP_OPTS
    if "application_" not in path:
        print ("Cannot delete invalid hdfs path: " + path)
        return False
    delete_cmd = "hadoop fs -rm -r " + path
    os.popen(delete_cmd)


def get_hdfs_info(conf):
    hdfs_info = {}
    zk_storage_dir = conf.get("high-availability.storageDir")
    jobmanager_archive_dir = conf.get("jobmanager.archive.fs.dir")
    historyserver_archive_dir = conf.get("historyserver.archive.fs.dir")
    checkpoint_dir = conf.get("state.checkpoints.dir")
    job_work_dir = conf.get("job.work.dir")
    hdfs_info['zk_storage_dir'] = zk_storage_dir
    hdfs_info['jobmanager_archive_dir'] = jobmanager_archive_dir
    hdfs_info['historyserver_archive_dir'] = historyserver_archive_dir
    hdfs_info['checkpoint_dir'] = checkpoint_dir
    hdfs_info['job_work_dir'] = job_work_dir
    return hdfs_info
