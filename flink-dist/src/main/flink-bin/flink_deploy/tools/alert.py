#!/usr/bin/env python
# -*- coding: utf-8 -*-

import json
import util

from conf_utils import ConfUtils
from lark_util import LarkUtils
from kafka_util import KafkaUtil
from http_util import HttpUtil
from util import red, green
from yaml_util import YamlUtil


class Alert(object):
    """
        Uses to register and unregister alarm
        1. get kafka spout info from yaml configuration file, then register lagsize alarm
    """


    KAFKA_OFFSET_ALERT_TEMPLATE = '''
        $lag_size = avg(q("max:%(prefix)s.%(topic)s.%(consumer_group)s.lag.size", "5m", ""))
        warn = $lag_size > %(max_offset)s
    '''

    KAFKA_OFFSET_ALERT_NAME = "flink job(%(topology_name)s), kafka top(%(topic)s), " \
                              "kafka cluster(%(kafka_cluster)s), consumer_group(%(consumer_group)s) " \
                              "has high lag size."

    METRIC_PREFIX = "flink:"

    JOB_RESTART_ALERT_TEMPLETE = '''
        $downtime = avg(q("max:flink.jobmanager.%(topology_name)s.downtime", "1m", ""))
        warn = $downtime > 0
    '''
    JOB_RESTART_ALERT_NAME = "flink job(%s) restart"
    FLINK_ALARM_ID = "1702"
    KAFKA_HIGH_LAG_HANDLE_WIKI = "https://wiki.bytedance.net/pages/viewpage.action?pageId=142757205"
    JOB_RESTART_HANDLE_WIKI = "https://wiki.bytedance.net/pages/viewpage.action?pageId=148308664"

    ROBOT_TOKEN = 'b-5f196d62-0108-4535-99e6-83071ff7ce2e'
    ALERT_OFFLINE_MESSAGE = "【Flink alert 下线】Flink 在 yaml 中配置报警的功能" \
                            "即将下线，请不要依赖 Flink yaml 文件中的报警配置。" \
                            "%s 该作业还在用 yaml 中配置注册报警。请在大禹上配置作业" \
                            "的报警, 详情见 https://bytedance.feishu.cn/space/doc/doccnZ6tilbVHt4XYdu8JJ#"
    @staticmethod
    def populate_alert_parser(parser):
        parser.add_argument("-y", "--yaml", type=str, required=True, action="store",
                            dest="yaml_file", help="yaml config location")
        parser.add_argument('-n', "--cluster_name", type=str, help="yarn cluster")
        parser.add_argument("-q", "--queue_name", type=str, help="yarn queue name")
        parser.add_argument('-u', '--user', type=str, action="store",
                            dest="user", default=None)
        parser.add_argument('-v', '--version', type=str, help='flink version',
                            choices=['1.3.2', '1.5'], default='1.5')

    def __init__(self, flink_conf, yaml_file, cluster_name, queue_name,
                 ms_base_url, user, ms_zone, version):
        self.flink_conf = flink_conf
        self.kafka_server_url = self.flink_conf.get('kafka_server_url')
        self.yaml_file = yaml_file
        self.cluster_name = cluster_name
        self.queue_name = queue_name
        self.ms_base_url = ms_base_url
        self.user = user
        self.lark_util = LarkUtils(Alert.ROBOT_TOKEN)
        self.yaml_util = YamlUtil(yaml_file)
        self.kafka_spout_conf = self.yaml_util.get_kafka_spout_info()
        self.topology_name = self.yaml_util.get_topology_name()
        self.topology_owner = self.yaml_util.get_owner()
        self.topology_alert_conf = self.yaml_util.get_alert_conf()
        self.ms_zone = ms_zone
        self.version = version

        if self.topology_alert_conf is None:
            self.topology_alert_conf = {}

        if self.topology_owner is None:
            self.topology_owner = user

        if self.topology_owner is None:
            raise Exception("no alert user")

    @classmethod
    def from_args(cls, args):
        yaml_file = args.yaml_file
        cluster_name = args.cluster_name
        queue_name = args.queue_name
        version = args.version
        flink_conf = ConfUtils.get_flink_conf(cluster_name, version)
        ms_base_url = flink_conf.get("ms_url")
        ms_zone = flink_conf.get("ms_zone")
        user = args.user
        return cls(flink_conf, yaml_file, cluster_name, queue_name,
                   ms_base_url, user, ms_zone, version)

    def register_offset_alert(self):
        is_register_ok = True
        for kafka_spout_info in self.kafka_spout_conf:
            topic, kafka_cluster, consumer_group, spout_name, is_multi_kafka = kafka_spout_info
            kafka_metric_prefix = \
                KafkaUtil.get_kafka_topic_prefix(kafka_cluster,
                                                 self.kafka_server_url)
            max_offset = '100000'
            if "alertConf" in self.topology_alert_conf:
                alertConf = self.topology_alert_conf.get("alertConf")
                if alertConf:
                    max_offset = alertConf.get(topic, '100000')
                else:
                    max_offset = '100000'
            bosun_alert = Alert.KAFKA_OFFSET_ALERT_TEMPLATE % {
                'prefix': kafka_metric_prefix,
                'topic': topic,
                'consumer_group': consumer_group,
                'max_offset': max_offset
            }

            alert_key = "%s:%s_%s_%s" % (Alert.METRIC_PREFIX + self.topology_name, kafka_metric_prefix, consumer_group, topic)

            alert_name = Alert.KAFKA_OFFSET_ALERT_NAME % {'topology_name': self.topology_name,
                                                                  'topic': topic,
                                                         'consumer_group': consumer_group,
                                                          'kafka_cluster': kafka_cluster}
            if not self.register_ms_alert(alert_key, bosun_alert, alert_name,
                                          Alert.KAFKA_HIGH_LAG_HANDLE_WIKI):
                is_register_ok = False
        return is_register_ok

    def register_job_restart_alert(self):
        alert_key = "%s:job_restart" % (Alert.METRIC_PREFIX + self.topology_name)
        alert_name = Alert.JOB_RESTART_ALERT_NAME % self.topology_name
        bosun_alert = Alert.JOB_RESTART_ALERT_TEMPLETE % {'topology_name': self.topology_name}
        return self.register_ms_alert(alert_key, bosun_alert, alert_name, Alert.JOB_RESTART_HANDLE_WIKI)

    def register_alert(self):
        if not self.topology_alert_conf or\
                not self.topology_alert_conf.get("alert_enable", True):
            print red("skip register alert")
            return
        try:
            self.lark_util.send_message_to_user_list(
                parse_user_list(self.topology_owner),
                Alert.ALERT_OFFLINE_MESSAGE % self.topology_name)
        except Exception, e:
            print "Failed to send alert to user."
            print e.message

        if not self.register_offset_alert():
            print red("failed to kafka lag alarm")
            return

        if self.topology_alert_conf.get("alarm_restart", False):
            if not self.register_job_restart_alert():
                print red("failed to register job restart alarm")

        print green("success to register alarm")

    def register_ms_alert(self, rule_alias, alarm_rule, alarm_name, handle_wiki):
        alarm_methods = self.topology_alert_conf.get("alert_methods", "1")
        if isinstance(alarm_methods, int):
            alarm_methods = str(alarm_methods)

        dingding_ids = self.topology_alert_conf.get('dingding_ids', "")
        if isinstance(dingding_ids, int):
            dingding_ids = str(dingding_ids)

        alarm_rule = {
            "alarm_id": 0,
            "alarm_methods": alarm_methods,
            "alarm_psm": "data.flink.job",
            "alarm_level": "1",
            "dingding_ids": dingding_ids,
            "owners": self.topology_owner,
            "alarm_target": "data.flink.job",
            "alarm_type": "1",
            "alarm_name": alarm_name,
            "alarm_rule": alarm_rule,
            "rule_alias": rule_alias,
            "handleSuggestion": handle_wiki
        }

        ms_zones = self.adjust_ms_zones()
        success = True
        for ms_zone in ms_zones:
            headers = None
            if ms_zone:
                headers = {"Ms-Zone": ms_zone}
            url = self.ms_base_url + "/msbackend/api/relation/v1/alarm_rule/add"
            (succ, result) = HttpUtil.do_post(url, data=json.dumps(alarm_rule), headers=headers)
            if not succ or result['error_code'] != 0:
                msg = "[%s] Register ms alert %s failed." % (ms_zone, rule_alias)
                print red(msg)
                print "the error info is ", result
                success = False
        return success

    def unregister_alert(self):
        ms_zones = self.adjust_ms_zones()
        success = True
        for ms_zone in ms_zones:
            url = self.ms_base_url + '/relation/v1/alarm_rule/rule_alias?alias_pattern=%s%%25' % \
                                     (Alert.METRIC_PREFIX + self.topology_name + ':')
            headers = None
            if ms_zone:
                headers = {"Ms-Zone": ms_zone}
            (succ, alerts) = HttpUtil.do_get(url, headers=headers)
            if not succ or not alerts:
                continue
            for rule in alerts:
                url = self.ms_base_url + '/relation/v1/alarm_rule/delete_by_alias'
                (succ, reps) = HttpUtil.do_post(url, data={"rule_alias": rule}, headers=headers)
                if not succ or reps['error_code'] != 0:
                    msg = "[%s] Unregister ms alert %s failed." % (ms_zone, rule)
                    print red(msg)
                    print "the error msg is ", reps
                    success = False
        if success:
            print green("Unregister alert success.")
        else:
            print red("Unregister alert failed")

    def adjust_ms_zones(self):
        ms_zones = []
        ms_zones.append(self.ms_zone)
        if self.ms_zone == "MVAALI":
            ms_zones.append("VA")
        elif self.ms_zone == "VA":
            ms_zones.append("MVAALI")
        return ms_zones


def parse_user_list(users):
    user_list = []
    for user in users.split(","):
        user_list.append(user.strip())
    return user_list
