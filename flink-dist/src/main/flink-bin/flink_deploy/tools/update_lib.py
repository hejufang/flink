#!/usr/bin/env python
# -*- coding:utf-8 -*-
from conf_utils import ConfUtils
import argparse
import os
import subprocess
import sys
import yaml

LIB_REPLICATION_NUM = 50
ALL_CLUSTERS = ['flink_share_yarn',
                'flink_independent_yarn',
                'flink_mva_aliyun_yarn',
                'flink_test_yarn',
                'flink_sg_aliyun_yarn',
                'flink_dw_yarn',
                'flink_lepad_yarn',
                'flink_oryx_yarn',
                'flink_topi_yarn',
                'flink_hl_yarn',
                'flink_hyrax_yarn',
                'flink_wj_yarn']


class UpdateLib():
    @staticmethod
    def upload_lib_to_hdfs(cluster_white_list, version):
        current_dir = os.path.split(os.path.realpath(__file__))[0]
        remote_lib_paths = set()
        clusters = ConfUtils.get_all_clusters(version)
        for cluster_name in clusters:
            if cluster_name not in cluster_white_list:
                continue
            flink_conf = ConfUtils.get_flink_conf(cluster_name, version)
            remote_lib_path = UpdateLib.get_hdfs_lib_path(flink_conf)
            remote_lib_paths.add(remote_lib_path)
        local_lib_path = os.path.join(current_dir,
                                      '../deploy/flink-%s/lib/*' % version)
        UpdateLib.do_upload(local_lib_path, remote_lib_paths)

    @staticmethod
    def get_hdfs_lib_path(conf):
        job_work_dir = conf.get('job.work.dir', '')
        flink_lib_path = conf.get('flink.lib.path', 'lib')
        remote_lib_path = os.path.join(job_work_dir, flink_lib_path)
        return remote_lib_path

    @staticmethod
    def do_upload(local_lib_path, remote_lib_paths):
        for remote_lib_path in remote_lib_paths:
            mkdir_cmd = "hadoop fs -mkdir -p " + remote_lib_path
            put_cmd = "hadoop fs -put -f " + local_lib_path + " " \
                      + remote_lib_path
            setrep_cmd = "hadoop fs -setrep " + str(LIB_REPLICATION_NUM) + " " \
                         + remote_lib_path
            return_code = UpdateLib.execute_cmd(mkdir_cmd)
            if return_code != 0:
                print "Failed to update library on hdfs : " + remote_lib_path
                print "Please do it manually."
                continue
            return_code = UpdateLib.execute_cmd(put_cmd)
            if return_code != 0:
                print "Failed to update library on hdfs : " + remote_lib_path
                print "Please do it manually."
                continue
            UpdateLib.execute_cmd(setrep_cmd)
            if return_code != 0:
                print "Failed to set replication for " + remote_lib_path
                print "Please do it manually."
                continue

    @staticmethod
    def execute_cmd(cmd):
        retcode = subprocess.call(cmd, shell=True)
        if retcode == 0:
            print "Success to execute cmd: " + cmd
            return 0
        else:
            print "Failed to execute cmd: " + cmd
            return -1

    @staticmethod
    def parse_cmd():
        parse = argparse.ArgumentParser(description='Update library on hdfs.')
        parse.add_argument('-c', '--clusters', nargs='*')
        parse.add_argument('-a', '--all', action='store_true')
        parse.add_argument('-v', '--version', type=str, help='flink version',
                           choices=['1.5'], required=True)
        return parse


def main():
    args = UpdateLib.parse_cmd().parse_args()
    all = args.all
    version = args.version
    if all:
        clusters = ConfUtils.get_all_clusters(version)
    else:
        clusters = args.clusters
    if not clusters or len(clusters) <= 0:
        print "No cluster assigned."
        exit(-1)
    UpdateLib.upload_lib_to_hdfs(clusters, version)


if __name__ == '__main__':
    main()
