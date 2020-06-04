#!/usr/bin/env python
# -*- coding: utf-8 -*-

import os
import sys

current_dir = os.path.dirname(os.path.abspath(__file__))
sys.path.append(os.path.join(current_dir, '..'))
from tools.yarn_util import YarnUtil

# TODO(zoudan): do it in a smarter way.
ALL_TAGS = {"cn": {"flink": {"tags": ["inf.hadoop.compute.flink"]},
                   "dw": {"tags": ["inf.hadoop.compute.dw"]},
                   "hl": {"tags": []},
                   "lepad": {"tags": ["inf.hadoop.compute.lepad"]},
                   "wj": {"tags": ["inf.hadoop.compute.wj.flink"]},
                   "hyrax": {"tags": ["inf.hadoop.compute.hyrax"]}},
            "sg": {"alisg": {
                "tags": ["alicloud.sg.inf.hadoop.compute.default.flink",
                         "alicloud.sg.inf.hadoop.compute.default.bigbang",
                         "alicloud.sg.inf.hadoop.compute.default.quick",
                         "alicloud.sg.inf.hadoop.compute.data_arch"]}},
            "va": {"maliva": {
                "tags": ["alicloud.va.m.inf.hadoop.compute.default.flink",
                         "alicloud.va.m.inf.hadoop.compute.default.bigbang",
                         "alicloud.va.m.inf.hadoop.compute.default.quick",
                         "alicloud.va.m.inf.hadoop.compute.default.data_arch"]}
            }
            }


class FlinkStatistics():
    @staticmethod
    def flink_node_count():
        all_node = 0
        for (region, clusters) in ALL_TAGS.items():
            region_count = 0
            for (cluster, cluster_info) in clusters.items():
                cluster_count = 0
                for tag in cluster_info['tags']:
                    cluster_count += FlinkStatistics.get_node_count(tag)
                print "%s %s node %s" % (region, cluster, cluster_count)
                all_node += cluster_count
                clusters[cluster]['node'] = cluster_count
                region_count += cluster_count
            ALL_TAGS[region]['node'] = region_count
        for region, info in ALL_TAGS.items():
            print region, info.get('node')

    @staticmethod
    def get_node_count(tag):
        cmd = "lh -s %s | wc -l" % tag
        r = os.popen(cmd)
        result = r.readlines()
        count = FlinkStatistics.get_num_result(result)
        return count

    @staticmethod
    # remove '\n' of each element of list
    def get_num_result(l):
        for element in l:
            e = element.replace("\n", "")
            num = int(e)
            return num

    @staticmethod
    def run():
        FlinkStatistics.flink_node_count()
        YarnUtil.get_flink_running_app_num_by_cluster("flink")


if __name__ == '__main__':
    flink_statistics = FlinkStatistics()
    flink_statistics.run()
