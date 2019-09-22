#!/usr/bin/env python
# -*- coding: utf-8 -*-


from http_util import HttpUtil
from metric_util import MetricUtil
from pykafkaclient.kafka_proxy2.kafka_proxy import KafkaProxy

KAFKA_SPEED_TEMPLATE = "%s.server.io.BytesInPerSec.Count{topic=%s}"
METRICS_PREFIX_QUERY_URL = "/queryClusterMetricsPrefix.do?cluster=%s"
CLUSTER_METRICS_PREFIX_KEY = "clusterMetricsPrefix"
KAFKA_SERVER_URL_DEFAULT = "http://kafka-config.byted.org"


class KafkaUtil(object):
    @staticmethod
    def get_kafka_partition(kafka_cluster, topic):
        try:
            kafka_proxy = KafkaProxy(topic=topic, cluster_name=kafka_cluster)
            partitions = kafka_proxy.get_kafka_producer().partitions_for(topic)
            return len(partitions)
        except Exception as e:
            print "Failed to get partitions for cluster: %s, topic: %s, please make sure " \
                  "you write the right kafka cluster & topic" % (kafka_cluster, topic)
            raise Exception(e)

    @staticmethod
    def get_kafka_metric_conf(kafka_cluster,
                              kafka_server_url=KAFKA_SERVER_URL_DEFAULT):
        url = (kafka_server_url + METRICS_PREFIX_QUERY_URL) % kafka_cluster
        (succ, resp) = HttpUtil.do_get(url, allow_redirects=False)
        if not succ:
            return {}
        else:
            return resp.get(CLUSTER_METRICS_PREFIX_KEY, {})

    @staticmethod
    def get_kafka_topic_prefix(kafka_cluster,
                               kafka_server_url=KAFKA_SERVER_URL_DEFAULT):
        kafka_cluster_info = \
            KafkaUtil.get_kafka_metric_conf(kafka_cluster, kafka_server_url)
        topic_metric_prefix =\
            kafka_cluster_info.get('topic_related_metric_prefix', "")
        if topic_metric_prefix:
            topic_metric_prefix = topic_metric_prefix.split(",")[0]
        return topic_metric_prefix

    @staticmethod
    def get_kafka_server_prefix(kafka_cluster,
                                kafka_server_url=KAFKA_SERVER_URL_DEFAULT):
        kafka_cluster_info = \
            KafkaUtil.get_kafka_metric_conf(kafka_cluster, kafka_server_url)
        server_metric_prefix = \
            kafka_cluster_info.get('broker_related_metric_prefix', "")
        return server_metric_prefix

    @staticmethod
    def get_kafka_client_prefix(kafka_cluster,
                                kafka_server_url=KAFKA_SERVER_URL_DEFAULT):
        kafka_cluster_info = \
            KafkaUtil.get_kafka_metric_conf(kafka_cluster, kafka_server_url)
        client_metric_prefix = \
            kafka_cluster_info.get('client_related_metric_prefix', "")
        return client_metric_prefix

    @staticmethod
    def get_kafka_value(cluster, topic, partition_num,
                        kafka_server_url=KAFKA_SERVER_URL_DEFAULT):
        metric_prefix = KafkaUtil.get_kafka_server_prefix(cluster, kafka_server_url)
        metric_name = KAFKA_SPEED_TEMPLATE % (metric_prefix, topic)
        dps = MetricUtil.get_metric_data('24h-ago', '', 'sum:1h-max:rate', metric_name)
        if dps is None or len(dps) == 0:
            return 0
        max_speed = max(dps.values())
        return max_speed * 1.0 / (1 << 10) / partition_num

    @staticmethod
    def is_aws(cluster):
        return 'aws' in cluster
