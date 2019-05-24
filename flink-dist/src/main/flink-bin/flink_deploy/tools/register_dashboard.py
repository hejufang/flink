#!/usr/bin/env python
# -*- coding:utf-8 -*-


from dashboard_template import *
from http_util import HttpUtil
from yaml_util import YamlUtil
from conf_utils import ConfUtils


class RegisterDashboard(object):
    """ Registers dashboard for a given topology """

    @staticmethod
    def populate_dashboard_parser(parser):
        parser.add_argument("-y", "--yaml", type=str, required=True, action="store",
                            dest="yaml_file", help="yaml config location")
        parser.add_argument('-v', '--version', type=str, help='flink version',
                            default='1.5')
        parser.add_argument('-n', "--cluster_name", type=str,
                            help="yarn cluster")
        parser.add_argument('-q', "--queue_name", type=str,
                            help="yarn queue name")

    @classmethod
    def from_args(cls, args):
        return cls(args.yaml_file, args.cluster_name, args.version)

    def __init__(self, yaml_file, cluster_name="flink", flink_version="1.5"):
        self.cluster_name = ConfUtils.replace_cluster_conf(cluster_name)
        self.flink_conf = ConfUtils.get_flink_conf(self.cluster_name, flink_version)
        self.kafka_server_url = self.flink_conf.get('kafka_server_url')
        self.yaml_util = YamlUtil(yaml_file)
        self.spouts = self.yaml_util.get_spout_name_list()
        self.bolts = self.yaml_util.get_bolt_name_list()
        self.batch_bolts = self.yaml_util.get_batch_bolt_name_list()
        self.topology_name = self.yaml_util.get_topology_name()
        self.kafka_spouts = self.yaml_util.get_kafka_spout_info()
        self.url = "https://grafana.byted.org/api/dashboards/db"
        self.authorization = "Bearer eyJrIjoiYjZMS0hPSXZybVpOOWJMS3pLRHkwaXRoWWI2RW1" \
                             "UT2oiLCJuIjoianN0b3JtIiwiaWQiOjF9"
        self.accept = "application/json"
        self.content_type = "application/json"
        self.headers = {
            "Authorization": self.authorization,
            "Accept": self.accept,
            "Content-Type": self.content_type
        }
        self.metrics_namespace_prefix = self.yaml_util.get_metric_namespace()
        self.flink_version = flink_version

    def register_dashboard(self):
        cluster_name = self.cluster_name
        if cluster_name is None or not cluster_name.strip():
            cluster_name = "flink_independent_yarn"
        data_source = self.flink_conf.get("dataSource")
        if data_source is None:
            data_source = "bytetsd"
        d = DashboardTemplate()
        dashboard = d.render_dashboard_template(self.topology_name, cluster_name, self.spouts,
                                                self.bolts, self.batch_bolts,
                                                self.kafka_spouts, data_source,
                                                self.metrics_namespace_prefix,
                                                self.flink_version,
                                                self.kafka_server_url)
        dashboard = dashboard.encode("utf-8")
        HttpUtil.do_post(self.url, headers=self.headers, data=dashboard)
