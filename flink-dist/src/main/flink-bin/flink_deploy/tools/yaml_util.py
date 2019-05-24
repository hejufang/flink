#!/usr/bin/env python
# -*-coding:utf-8 -*-

import copy
import os
import yaml
from collections import namedtuple
from kafka_util import KafkaUtil
from util import red


SpoutItem = namedtuple("SpoutItem", ['name', 'parallelism', 'partition_value'])
BoltItem = namedtuple('BoltItem', ['name', 'parallelism', 'is_batch', 'stream_from'])


class PyFlinkJobInfo():
    def __init__(self):
        self.spouts = []
        self.bolts = []


class YamlUtil(object):
    IS_THREAD_NUM_DETERMINED_BY_PARTITION = "is_thread_num_determined_by_partition"
    KAFKA_SPOUT_GEVENT_NAME = "kafka_spout_gevent.py"
    MULTI_KAFKA_SPOUT_NAME = "multi_kafka_spout.py"

    @staticmethod
    def get_yaml_info(yaml_conf_file):
        with open(yaml_conf_file, 'r') as stream:
            user_yaml_conf = yaml.load(stream)
        return user_yaml_conf

    """ Parses yaml file for a given yaml file """
    def __init__(self, yaml_file):
        if not os.path.isfile(yaml_file):
            error_msg = "%s don't exits" % yaml_file
            print red(error_msg)
            raise Exception(error_msg)

        with open(yaml_file) as yaml_file_stream:
            self.yaml_conf = yaml.load(yaml_file_stream)

    """ Returns as [("topic", "kafka_cluster", "consumer_group", "spout_name"), ] """
    def get_kafka_spout_info(self):
        spouts_conf = self.yaml_conf.get('spout')
        spout_common_conf = spouts_conf.get('common_args')
        spouts = spouts_conf.get('spout_list')
        kafka_spout_conf = []
        for spout in spouts:
            sp_conf = spout.get('args', {})
            for key, val in spout_common_conf.items():
                if key not in sp_conf:
                    sp_conf[key] = val
            spout_name = spout['spout_name']
            if 'script_name' not in sp_conf:
                if 'script_name' not in spout:
                    raise Exception('script_name not in spout.common_args, '
                                    'spout.spout_list, spout.spout_list.args')
                else:
                    sp_conf['script_name'] = spout['script_name']

            script_name = sp_conf['script_name']
            if 'kafka' not in script_name:
                continue
            if "multi_kafka_spout.py" in sp_conf["script_name"]:
                for kafka_source in sp_conf["kafka_sources"]:
                    topic = kafka_source["topic_name"]
                    kafka_cluster = kafka_source["kafka_cluster"]
                    kafka_spout_conf.append((topic, kafka_cluster, sp_conf["consumer_group"], spout_name, True))
            else:
                topic = sp_conf['topic_name']
                kafka_cluster_name = sp_conf['kafka_cluster']
                kafka_spout_conf.append((topic, kafka_cluster_name, sp_conf['consumer_group'], spout_name, False))
        return kafka_spout_conf

    def get_spout_name_list(self):
        spout_name_list = []
        spout_conf = self.yaml_conf.get("spout")
        if spout_conf is not None:
            spout_list = spout_conf.get("spout_list")
            for spout in spout_list:
                spout_name_list.append(spout['spout_name'])
        return spout_name_list

    def get_bolt_name_list(self):
        bolt_name_list = []
        bolt_conf = self.yaml_conf.get("bolt")
        if bolt_conf is not None:
            bolt_list = bolt_conf.get("bolt_list")
            if bolt_list is not None:
                for bolt in bolt_list:
                    bolt_name_list.append(bolt["bolt_name"])
        return bolt_name_list

    def get_batch_bolt_name_list(self):
        batch_bolt_name_list = []
        bolt_conf = self.yaml_conf.get("bolt")
        if bolt_conf is not None:
            bolt_list = bolt_conf.get("bolt_list")
            if bolt_list is not None:
                for bolt in bolt_list:
                    if "script_name" in bolt and "batch_bolt" in bolt["script_name"]:
                        batch_bolt_name_list.append(bolt["bolt_name"])
        return batch_bolt_name_list

    def get_alert_conf(self):
        return self.yaml_conf.get("alert")

    def get_owner(self):
        return self.yaml_conf.get("owners")

    def get_topology_name(self):
        topology_name = self.yaml_conf.get("topology_name")
        return topology_name

    def get_metric_namespace(self):
        common_args = self.yaml_conf.get("common_args")
        if common_args is not None:
            metric_namespace_prefix = common_args.get("metrics_namespace_prefix")
        else:
            metric_namespace_prefix = "storm." + self.get_topology_name()
        return metric_namespace_prefix

    def get_job_info(self, kafka_server_url):
        job_info = PyFlinkJobInfo()
        job_info.spouts = self.get_spout_info(kafka_server_url)
        job_info.bolts = self.get_bolt_info()
        return job_info

    def get_spout_info(self, kafka_server_url):
        result = []
        spouts_conf = self.yaml_conf.get('spout')
        spout_common_conf = spouts_conf.get('common_args')
        is_test = self.yaml_conf.get('topology_standalone_mode')
        spouts = spouts_conf.get('spout_list')
        for spout in spouts:
            sp_conf = copy.deepcopy(spout.get('args', {}))
            for key, val in spout_common_conf.items():
                if key not in sp_conf:
                    sp_conf[key] = val
            sp_conf.update(spout)
            sp_name = sp_conf['spout_name']
            # if it is not a kafka spout, read from config
            if 'kafka' not in sp_conf['script_name']:
                partition_num = sp_conf['partition']
                result.append(SpoutItem(sp_name, partition_num, 1))
                continue

            is_thread_num_determined_by_partition = \
                sp_conf.get('kafka_spout_ext', 1) == 1

            is_thread_num_determined_by_partition = \
                sp_conf.get(self.IS_THREAD_NUM_DETERMINED_BY_PARTITION,
                            is_thread_num_determined_by_partition)

            topic = sp_conf.get('topic_name')
            cluster = sp_conf.get('kafka_cluster')
            if self.KAFKA_SPOUT_GEVENT_NAME in sp_conf.get('script_name') \
                    or self.MULTI_KAFKA_SPOUT_NAME in sp_conf['script_name'] \
                    or not is_thread_num_determined_by_partition:
                parallelism = spout.get("thread_num")
                if not parallelism:
                    parallelism = spout_common_conf.get("thread_num")
                if self.MULTI_KAFKA_SPOUT_NAME not in sp_conf.get('script_name'):
                    # If the spout parallelism is larger than actual partition
                    # number, we adjust it to the actual partition number.
                    total_partition_num = KafkaUtil.get_kafka_partition(cluster,
                                                                        topic)
                    parallelism = min(parallelism, total_partition_num)
                if not parallelism:
                    raise Exception("thread_num needed")
                partition_value = parallelism
            else:
                if is_test == 1:
                    partition_num = sp_conf['partition']
                    parallelism = partition_num
                    partition_value = parallelism
                else:
                    if "partition_range" in sp_conf and \
                                    sp_conf['partition_range'] > 1:
                        partition_range_list = sp_conf['partition_range']
                        partition_list = self.parse_kafka_partition_list(
                            partition_range_list)
                        parallelism = len(partition_list)
                        total_partition_num = KafkaUtil.get_kafka_partition(
                            cluster,
                            topic)
                        partition_value = \
                            KafkaUtil.get_kafka_value(cluster, topic,
                                                      total_partition_num,
                                                      kafka_server_url)
                    else:
                        parallelism = KafkaUtil.get_kafka_partition(cluster,
                                                                    topic)
                        partition_value = \
                            KafkaUtil.get_kafka_value(cluster, topic,
                                                      parallelism,
                                                      kafka_server_url)
            result.append(SpoutItem(sp_name, parallelism, partition_value))
        return sorted(result, key=lambda x: x.partition_value)

    def parse_kafka_partition_list(self, partition_range_list):
        result = []
        for range in partition_range_list:
            if isinstance(range, int):
                result.append(range)
            elif '-' in range:
                strs = range.split('-')
                if (len(strs) >= 2):
                    start = int(strs[0])
                    end = int(strs[1])
                    i = start
                    while i <= end:
                        result.append(i)
                        i += 1
        return result

    def get_bolt_info(self):
        result = []
        bolts_conf = self.yaml_conf.get('bolt')
        bolts = bolts_conf.get('bolt_list')
        if bolts is None:
            return result
        for bolt in bolts:
            is_batch_bolt = False
            name = bolt.get('bolt_name')
            if "batch" in bolt.get('script_name'):
                is_batch_bolt = True
            stream_from = []
            for upstream in bolt.get('group_list'):
                stream_from = stream_from + upstream.get('stream_from')
            result.append(BoltItem(name, bolt.get('thread_num'), is_batch_bolt,
                                   stream_from))
        return result

