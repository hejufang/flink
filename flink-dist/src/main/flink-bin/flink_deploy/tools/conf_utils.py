#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import yaml

CLUSTER_MAP = {
    "flink_independent_yarn": "flink",
    "flink_dw_yarn": "dw",
    "flink_lepad_yarn": "lepad",
    "flink_oryx_yarn": "oryx",
    "flink_wj_yarn": "wj",
    "flink_hl_yarn": "hl",
    "flink_topi_yarn": "topi",
    "flink_mva_aliyun_yarn": "maliva",
    "flink_sg_aliyun_yarn": "alisg",
    "flink_test_yarn": "test1"
}

COMMON = "common"


class ConfUtils(object):
    """ Use to parase conf """
    @staticmethod
    def get_flink_conf_dir(version):
        current_dir = os.path.split(os.path.realpath(__file__))[0]
        flink_conf_dir = \
            os.path.join(current_dir, "../deploy/flink-%s/conf" % version)
        return flink_conf_dir

    @staticmethod
    def get_flink_conf(cluster, version):
        dynamicParams = ['dc', 'clusterName', 'hdfs.prefix']
        current_dir = os.path.split(os.path.realpath(__file__))[0]
        origin_conf = ConfUtils.get_origin_conf(version)
        conf = origin_conf.get(COMMON)
        special_conf = origin_conf[cluster]
        for (k, v) in special_conf.items():
            conf[k] = v
        for (k, v) in conf.items():
            if isinstance(v, str):
                replaced_value = v
                for param in dynamicParams:
                    replaced_value = replaced_value.replace('${' + param + '}',
                                               conf.get(param))
                conf[k] = replaced_value
        upper_directory = os.path.join(current_dir, "..")
        if 'base_jar' in conf:
            conf['base_jar'] = \
                os.path.join(upper_directory, conf.get('base_jar'))
        if 'bin' in conf:
            conf['bin'] = \
                os.path.join(upper_directory, conf.get('bin'))
        return conf

    @staticmethod
    def get_origin_conf(version):
        current_dir = os.path.split(os.path.realpath(__file__))[0]
        flink_conf_file = \
            os.path.join(current_dir,
                         "../deploy/flink-%s/conf/flink-conf.yaml" % version)
        origin_conf = yaml.load(open(flink_conf_file))
        return origin_conf

    @staticmethod
    def get_all_clusters(version):
        origin_conf = ConfUtils.get_origin_conf(version)
        clusters = []
        for cluster in origin_conf:
            if cluster != COMMON:
                clusters.append(cluster)
        return clusters

    @staticmethod
    def replace_cluster_conf(old_cluster_name):
        new_cluster_name = old_cluster_name
        if old_cluster_name in CLUSTER_MAP:
            new_cluster_name = CLUSTER_MAP.get(old_cluster_name)
        return new_cluster_name


if __name__ == '__main__':
    print ConfUtils.get_flink_conf('flink_independent_yarn', version='1.5')
