#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import hdfs_util
import json
import kafka
import logging
import os
import pwd
import re
import shutil
import sys
import tempfile
import time
import traceback
import yaml
import zk_util

from alert import Alert
from conf_utils import ConfUtils
from flink_resource import FlinkResource
from http_util import HttpUtil
from kafka_util import KafkaUtil
from mysql_util import MysqlUtil
from pyutil.program import metrics3 as metrics
from register_dashboard import RegisterDashboard
from sr_utils import SmartResourcesUtils
from ss_lib.topology.util.utils import RESOURCES_NAME
from ss_lib.topology.util.utils import _create_storm_jar
from ss_lib.topology.util.utils import _open_jar
from ss_lib.topology.util.utils import expand_path
from util import red, green, out_and_info
from yarn_util import YarnUtil
from yaml_util import YamlUtil


CUR_PATH = os.path.abspath(os.curdir)
STOP_JOB_MAX_RETRY = 3
STOP_JOB_RETRY_INTERVAL_SECONDS = 3
TM_MEMORY_MB_MAX = 30 * 1024
TM_MEMORY_MB_MAX_CEILING = 100 * 1024
TM_VCORES_MAX = 8
TM_VCORES_MAX_CEILING = 47
DEFAULT_TOPIC_THRESHOLD = 100 * 1000


class NoEnoughResArgs(Exception):
    pass


class FlinkTopology(object):
    """
        uses to start and stop flink job
    """
    FLINK_RESOURCE_KEY = "flink_resource_args"
    START_LATENCY = 'acceleration.start.latency'
    BULDER_JAR_LATENCY = 'acceleration.build_jar.latency'
    REGISTER_ALERT_LATENCY = 'acceleration.register_alert.latency'
    REGISTER_DASHBOARD_LATENCY = 'acceleration.register_dashboard.latency'
    SR_CPU_OPTIMIZE = 'sr.resources.optimize.start.cpu'
    SR_MEM_OPTIMIZE = 'sr.resources.optimize.start.mem'
    METRICS_NAMESPACE_PREFIX = 'inf.flink'
    FLINK_JOB_TYPE_KEY = "flink.job_type"
    PYFLINK = "pyFlink"
    PYJSTORM = "pyJstorm"
    VALID_VERSIONS = ["1.5"]
    DEFAULT_VERSION = "1.5"
    RESOURCES_OPTIMIZE_WIKI = "https://wiki.bytedance.net/pages/viewpage.action?pageId=151754851"
    SKIP_UNIMPORTANCE_PROCESS = "skip_unimportance_process"

    @staticmethod
    def populate_lifecycle_parser(parser):
        parser.add_argument('-n', "--cluster_name", type=str,
                            help="yarn cluster")
        parser.add_argument('-q', "--queue_name", type=str,
                            help="yarn queue name")
        parser.add_argument('-y', "--yaml", type=str,
                            help="yaml config location")
        parser.add_argument('-f', '--flink_args', type=str,
                            help="args directly delivery to flink")
        parser.add_argument('-u', '--user', type=str, default=None)
        parser.add_argument('-c', '--config_dir', type=str, default=None)
        parser.add_argument('-r', '--batch_runner', type=str, default=None)
        parser.add_argument("-p", '--pid', default=False, action="store_true",
                            dest="save_pid")
        parser.add_argument('-tc', '--total_cores', type=int, default=None,
                            help='total cores')
        parser.add_argument('-v', '--version', type=str, help='flink version',
                            default='1.5')
        parser.add_argument('--is_simulate_http_timeout', default=False,
                            action='store_true', help="mock http timeout exception")

    @classmethod
    def from_args(cls, args):
        if args.version not in FlinkTopology.VALID_VERSIONS:
            args.version = FlinkTopology.DEFAULT_VERSION

        yaml_file = os.path.abspath(args.yaml)
        if not os.path.isfile(yaml_file):
            print red(args.yaml + " does not exists.")
            return

        if args.user:
            user = args.user
        else:
            try:
                user = pwd.getpwuid(os.getuid()).pw_name
            except Exception as e:
                user = 'tiger'
        if 'tiger' in user:
            print red("If you login as tiger, please use -u to specific your name")
            return
        if user:
            print "user = %s" % user
        else:
            print "No user specified."

        if args.queue_name is None:
            print red("Please specify queue name")
            return

        return cls(cluster_name=args.cluster_name, queue_name=args.queue_name,
                   yaml_file=yaml_file, config_dir=args.config_dir,
                   flink_args=args.flink_args, user=user,
                   batch_runner=args.batch_runner,
                   save_pid=args.save_pid, version=args.version,
                   is_simulate_http_timeout=args.is_simulate_http_timeout)

    def __init__(self, cluster_name, queue_name, yaml_file,
                 flink_args, user=None, config_dir=None,
                 batch_runner=None, save_pid=None, version='1.5',
                 is_simulate_http_timeout=False):
        HttpUtil.is_simulate_http_timeout = is_simulate_http_timeout
        if flink_args:
            self.flink_args = flink_args
        else:
            self.flink_args = ''
        self.kafka_topics = []
        self.config_dir = config_dir
        self.cluster_name = ConfUtils.replace_cluster_conf(cluster_name)
        self.flink_conf = ConfUtils.get_flink_conf(self.cluster_name, version)
        self.hadoop_conf_dir = self.flink_conf.get('HADOOP_CONF_DIR')
        self.queue_name = queue_name
        self.vcores = self.flink_conf.get('vcores')
        self.ms_base_url = self.flink_conf.get('ms_url')
        self.ms_zone = self.flink_conf.get('ms_zone')
        self.kafka_server_url = self.flink_conf.get('kafka_server_url')
        self.region = self.flink_conf.get('dc')
        self.user_yaml_conf = YamlUtil.get_yaml_info(yaml_file)
        self.yaml_util = YamlUtil(yaml_file)
        # skip not importance process, default value is False.
        # flink job yaml configuration override flink.conf
        self.skip_unimportance_process = self.flink_conf.get(
            FlinkTopology.SKIP_UNIMPORTANCE_PROCESS, False)
        self.skip_unimportance_process = self.user_yaml_conf.get(
            FlinkTopology.SKIP_UNIMPORTANCE_PROCESS, self.skip_unimportance_process)
        print "skip_unimportance_process = %s" % self.skip_unimportance_process
        self.is_restructure_mode = self.user_yaml_conf.get(
            'is_restructure_mode', True)
        if self.is_restructure_mode:
            self.job_type = FlinkTopology.PYFLINK
            self.base_jar = self.flink_conf.get('base_jar_new')
            self.bin = self.flink_conf.get('bin_new')
        else:
            self.job_type = FlinkTopology.PYJSTORM
            self.base_jar = self.flink_conf.get('base_jar')
            self.bin = self.flink_conf.get('bin')
        self.topology_name = self.user_yaml_conf.get('topology_name')
        self.flink_yarn_args = self.user_yaml_conf.get('flink_args', {})

        # add job type info to self.flink_yarn_args
        self.flink_yarn_args[FlinkTopology.FLINK_JOB_TYPE_KEY] = self.job_type
        self.sr_args = self.user_yaml_conf.get('sr_args', {})
        self.output_jar = os.path.join(CUR_PATH, self.topology_name + ".jar")
        self.yaml_file = yaml_file
        self.version = version
        self.user = user
        self.batch_runner = batch_runner
        self.save_pid = save_pid
        self.alert = Alert(self.flink_conf, self.yaml_file, self.cluster_name,
                           self.queue_name, self.ms_base_url, self.user,
                           self.ms_zone, self.version)
        self.dashboard = RegisterDashboard(self.yaml_file, self.cluster_name, self.version)
        self.zk_info = zk_util.get_zk_info(self.flink_conf)
        self.hdfs_info = hdfs_util.get_hdfs_info(self.flink_conf)
        self.flink_resource = self.get_flink_resource()
        if not self.flink_resource:
            print red("Please add flink resource info to topology_online.yaml.")
            sys.exit(-1)

        metrics.init({'metrics_namespace_prefix':
                          FlinkTopology.METRICS_NAMESPACE_PREFIX})
        metrics.define_store(FlinkTopology.START_LATENCY, 'us')
        metrics.define_store(FlinkTopology.BULDER_JAR_LATENCY, 'us')
        metrics.define_store(FlinkTopology.REGISTER_ALERT_LATENCY, 'us')
        metrics.define_store(FlinkTopology.REGISTER_DASHBOARD_LATENCY, 'us')
        metrics.define_store(FlinkTopology.REGISTER_DASHBOARD_LATENCY, 'us')
        metrics.define_store(FlinkTopology.SR_CPU_OPTIMIZE, 'num')
        metrics.define_store(FlinkTopology.SR_MEM_OPTIMIZE, 'num')
        metrics.define_tagkv('job_name', [self.topology_name])
        metrics.define_tagkv('cluster', [self.cluster_name])
        metrics.define_tagkv('queue', [self.queue_name])
        metrics.start()

    def get_flink_resource(self):
        resource_args = self.user_yaml_conf.get(self.FLINK_RESOURCE_KEY)
        if not resource_args:
            return None

        if not resource_args.has_key('use_large_memory'):
            resource_args['use_large_memory'] = False
        if not resource_args.has_key('use_large_vcores'):
            resource_args['use_large_vcores'] = False
        if resource_args.has_key('task_manager_slot') and \
                not resource_args.has_key('tm_slot'):
            resource_args['tm_slot'] = resource_args.get('task_manager_slot')
            resource_args.pop('task_manager_slot')
        return FlinkResource.create_by_dict(resource_args)

    def update_resources_if_sr_enabled(self):
        if not self.sr_args.get("enable_smart_resources", False):
            return self.flink_resource

        resource_args = self.user_yaml_conf.get(self.FLINK_RESOURCE_KEY)
        resource_args = self.update_resource_args(resource_args)

        if not resource_args:
            return self.flink_resource

        if not resource_args.has_key('use_large_memory'):
            resource_args['use_large_memory'] = False
        if not resource_args.has_key('use_large_vcores'):
            resource_args['use_large_vcores'] = False
        if resource_args.has_key('task_manager_slot') and \
                not resource_args.has_key('tm_slot'):
            resource_args['tm_slot'] = resource_args.get('task_manager_slot')
            resource_args.pop('task_manager_slot')
        return FlinkResource.create_by_dict(resource_args)

    def update_resource_args(self, resource_args):
        print green("update resource config by sr")
        try:
            job_info = self.yaml_util.get_job_info(self.kafka_server_url)
            new_resources_config = SmartResourcesUtils.generate_config(
                self.region,
                self.cluster_name,
                self.topology_name,
                job_info,
                self.sr_args)
        except Exception as e:
            print green(
                "sr update resource config error, use default\n" + e.message)
            msg = traceback.format_exc()
            print green(msg)
            return resource_args

        if new_resources_config:
            print green(
                "resource config from sr\n" + yaml.dump(new_resources_config,
                                                        default_flow_style=False))
            if not resource_args:
                resource_args = {}
            resource_args.update(new_resources_config["flink_resource_args"])
            self.flink_yarn_args.update(new_resources_config["flink_args"])

            metrics.emit_store(FlinkTopology.SR_CPU_OPTIMIZE,
                               resource_args["tm_num"] * resource_args[
                                   "tm_cores"] -
                               self.flink_resource.tm_num * self.flink_resource.tm_cores,
                               tagkv={'job_name': self.topology_name,
                                      'cluster': self.cluster_name,
                                      'queue': self.queue_name})
            metrics.emit_store(FlinkTopology.SR_MEM_OPTIMIZE,
                               resource_args["tm_num"] * resource_args[
                                   "tm_memory"] -
                               self.flink_resource.tm_num * self.flink_resource.tm_memory,
                               tagkv={'job_name': self.topology_name,
                                      'cluster': self.cluster_name,
                                      'queue': self.queue_name})
        else:
            print green("resource config from sr is none, use default config")

        return resource_args

    def export_env_to_shell(self):
        if self.hadoop_conf_dir:
            os.environ['UD_HADOOP_CONF_DIR'] = self.hadoop_conf_dir
            print("set environment variable UD_HADOOP_CONF_DIR = "
                  + self.hadoop_conf_dir)
        if self.config_dir:
            os.environ['FLINK_CONF_DIR'] = self.config_dir
            print(
                "set environment variable FLINK_CONF_DIR = " + self.config_dir)
        if self.batch_runner:
            os.environ['BATCH_RUNNER'] = self.batch_runner

    def commit_offset(self):
        print "Begin to handle commit offset."
        spouts_conf = self.user_yaml_conf.get('spout')
        spout_common_conf = spouts_conf.get('common_args')
        use_new_kafka_proxy = spout_common_conf.get("use_new_kafka_proxy",
                                                    False)
        use_double_cluster_kafka = spout_common_conf.get(
            "use_double_cluster_kafka", True)
        spouts = spouts_conf.get('spout_list')
        common_script_name = spout_common_conf.get("script_name", None)
        kafka_topics = []
        for spout in spouts:
            script_name = spout.get("script_name", common_script_name)
            sp_conf = spout.get('args', {})
            for key, val in spout_common_conf.items():
                if key not in sp_conf:
                    sp_conf[key] = val
            if "multi_kafka_spout.py" in script_name:
                start_whence_common = sp_conf.get("start_whence", None)
                start_offset_common = sp_conf.get("start_offset", None)
                for topic_conf in sp_conf.get("kafka_sources", []):
                    start_whence = topic_conf.get("start_whence",
                                                  start_whence_common)
                    start_offset = topic_conf.get("start_offset",
                                                  start_offset_common)
                    topic_meta = {'cluster': topic_conf['kafka_cluster'],
                                  'topic': topic_conf['topic_name'],
                                  'spout': spout.get('spout_name'),
                                  'consumer': sp_conf['consumer_group']}

                    kafka_topics.append(topic_meta)
                    if not start_whence or not start_offset:
                        continue
                    self.commit_kafka_offset(topic_conf['kafka_cluster'],
                                             topic_conf['topic_name'],
                                             sp_conf['consumer_group'],
                                             start_offset, start_whence,
                                             use_new_kafka_proxy,
                                             use_double_cluster_kafka)
            else:
                if "kafka_spout" in script_name:
                    topic_meta = {'cluster': sp_conf['kafka_cluster'],
                                  'topic': sp_conf['topic_name'],
                                  'spout': spout.get('spout_name'),
                                  'consumer': sp_conf['consumer_group']}
                    kafka_topics.append(topic_meta)
                if not 'start_whence' in sp_conf or not 'start_offset' in sp_conf:
                    continue
                self.commit_kafka_offset(sp_conf['kafka_cluster'],
                                         sp_conf['topic_name'],
                                         sp_conf['consumer_group'],
                                         sp_conf['start_offset'],
                                         sp_conf['start_whence'],
                                         use_new_kafka_proxy,
                                         use_double_cluster_kafka)

        for topic in kafka_topics:
            topic_related_metric_prefix = \
                KafkaUtil.get_kafka_topic_prefix(topic_meta['cluster'],
                                                 self.kafka_server_url)
            topic['metric_prefix'] = topic_related_metric_prefix
            alert_conf = self.user_yaml_conf.get('alert', {}).get('alertConf')
            threshold = DEFAULT_TOPIC_THRESHOLD
            if alert_conf and topic_meta.get('topic') in alert_conf:
                threshold = alert_conf.get(topic_meta.get('topic'))
            topic['threshold'] = threshold
        self.kafka_topics = kafka_topics

    def commit_kafka_offset(self, kafka_cluster, topic, consumer_group,
                            start_offset, start_whence, use_new_kafka_proxy,
                            use_double_cluster_kafka):
        print "seek kafka offset", kafka_cluster, topic, consumer_group, start_whence, start_offset
        if use_new_kafka_proxy:
            from pyutil.kafka_proxy2.kafka_proxy import KafkaProxy
            kafka_proxy = KafkaProxy(topic=topic, cluster_name=kafka_cluster,
                                     consumer_group=consumer_group)
            kafka_proxy.set_consumer_offset(start_offset, start_whence,
                                            force=True)
        elif use_double_cluster_kafka:
            print "use double kafka proxy to commit offset"
            from pykafkaclient.kafka_proxy2.kafka_proxy import KafkaProxy
            kafka_proxy = KafkaProxy(cluster_name=kafka_cluster,
                                     topic=topic,
                                     consumer_group=consumer_group,
                                     psm=self.topology_name,
                                     owner=self.user,
                                     ignore_dc_check=True,
                                     team="flink")
            kafka_proxy.set_consumer_offset(start_offset, start_whence,
                                            force=True)
        else:
            from pyutil.kafka_proxy.kafka_proxy import KafkaProxy
            kafka_proxy = KafkaProxy(topic=topic, cluster_name=kafka_cluster)
            kafka_client = kafka_proxy.get_kafka_client()
            kafka_client.load_metadata_for_topics(topic)
            partitions = kafka_client.topic_partitions[topic]
            for partition in partitions:
                consumer = kafka.SimpleConsumer(kafka_client, consumer_group,
                                                topic=topic,
                                                partitions=[partition])
                consumer.seek(start_offset, start_whence)
                consumer.commit(force=True)

    def build_jar(self):
        print green("build %s" % self.topology_name)
        print green("Begin to build topology......")
        resource_dir = os.path.join(CUR_PATH, RESOURCES_NAME)
        if not os.path.exists(resource_dir):
            err_msg = "Directory resource does exists in current directory."
            print red(err_msg)
            raise Exception(err_msg)
        base_jar = self.base_jar
        base_jar_expand = expand_path(base_jar)
        zip_file = _open_jar(base_jar)
        try:
            # Everything will be copied in a tmp directory
            tmp_dir = tempfile.mkdtemp()
            try:
                _create_storm_jar(
                    resource_dir=resource_dir,
                    base_jar=base_jar_expand,
                    output_jar=self.output_jar,
                    zip_file=zip_file,
                    tmp_dir=tmp_dir
                )
            finally:
                shutil.rmtree(tmp_dir)
        finally:
            zip_file.close()
        out_and_info("Build topology have finished.")

    def build_start_cmd(self):
        if self.save_pid:
            return self.bin + " " + "save_pid" + " " + self.output_jar + " " + \
                   self.yaml_file + " " + self.cluster_name + " " + self.flink_args
        else:
            return self.bin + " " + self.output_jar + " " + self.yaml_file + " " + \
                   self.cluster_name + " " + self.flink_args

    def ensure_dir(self, path):
        path = path.strip()
        path = path.rstrip("\\")
        if not os.path.exists(path):
            os.makedirs(path, int('777', 8))
            print(path + " create successfully!")
        else:
            print(path + " already exists!")

    def init_taskmanager_tmp_dirs(self):
        taskmanager_tmp_dirs = self.flink_conf.get("taskmanager.tmp.dirs",
                                                   "/opt/tmp/flink")
        flink_dist = taskmanager_tmp_dirs + "/flink-dist"
        flink_pid = taskmanager_tmp_dirs + "/flink-pid"
        self.ensure_dir(taskmanager_tmp_dirs)
        self.ensure_dir(flink_dist)
        self.ensure_dir(flink_pid)

    def start(self):
        start_time = time.time()
        if not self.skip_unimportance_process:
            if YarnUtil.is_exists(self.user,
                                  self.region,
                                  self.cluster_name,
                                  self.queue_name,
                                  self.topology_name):
                error_msg = "the job %s has exists on yarn queue %s" % \
                            (self.topology_name, self.queue_name)
                print red(error_msg)
                raise Exception(error_msg)

            self.flink_resource = self.update_resources_if_sr_enabled()

            try:
                tm_num = self.flink_resource.tm_num
                total_mem_mb = self.flink_resource.tm_memory * tm_num
                total_cpu = self.flink_resource.tm_cores * tm_num
                SmartResourcesUtils.update_latest_resources(self.region,
                                                            self.cluster_name,
                                                            self.topology_name,
                                                            total_mem_mb, total_cpu)
            except Exception as e:
                print "update app latest resources failed, " + e.message

            if not self.check_queue_resource(self.queue_name, self.flink_resource):
                return False

            if not self.check_config(self.flink_resource):
                return False

        self.build_jar()

        # TODO(zoudan): reture False if user config the log, so as to
        # stop the start action

        # res = self.build_jar()
        # if res:
        #     return False

        self.commit_offset()
        try:
            self.add_default_args()
        except NoEnoughResArgs as e:
            self.start_fail()
            return False
        run_mode = self.user_yaml_conf.get("topology_standalone_mode")
        if run_mode == 1:
            # Make sure the tmp dir exists when running on local mode.
            self.init_taskmanager_tmp_dirs()
        cmd = self.build_start_cmd()
        print green("Cmd: " + cmd)
        self.export_env_to_shell()
        ret = os.system(cmd)
        result = True
        if not self.skip_unimportance_process:
            if run_mode == 0:
                try:
                    self.save_job_meta()
                except Exception, e:
                    print red("Failed to save job meta to database.")
                    print e
            if ret != 0:
                print red("Start failed.")
                result = False
            else:
                try:
                    print green("Start success.")
                    if run_mode == 0:
                        print("run on cluster mode")
                        start_register_alert_time = time.time()
                        # unregister old alerts
                        self.alert.unregister_alert()
                        self.alert.register_alert()
                        metrics.emit_store(FlinkTopology.REGISTER_ALERT_LATENCY,
                                           (time.time() - start_register_alert_time)
                                           * 1000000,
                                           tagkv={'job_name': self.topology_name,
                                                  'cluster': self.cluster_name,
                                                  'queue': self.queue_name})
                        start_register_dashboard_time = time.time()
                        self.dashboard.register_dashboard()
                        metrics.emit_store(FlinkTopology.REGISTER_DASHBOARD_LATENCY,
                                           (time.time() - start_register_dashboard_time)
                                           * 1000000,
                                           tagkv={'job_name': self.topology_name,
                                                  'cluster': self.cluster_name,
                                                  'queue': self.queue_name})
                    else:
                        print(
                            "run on local mode,no need to register alert and dashboard")
                    metrics.emit_store(FlinkTopology.START_LATENCY,
                                       (time.time() - start_time) * 1000000,
                                       tagkv={'job_name': self.topology_name,
                                              'cluster': self.cluster_name,
                                              'queue': self.queue_name})
                    print red(
                        "=================================================================\n"
                        "强烈建议将线上作业迁移到 Dayu 平台 https://dayu.bytedance.net\n"
                        "（1）脚本提交方式，不保障作业挂了，自动拉起来，需要用户自己手动拉起来\n"
                        "（2）脚本提交方式，出现平台级故障时，平台不能帮助你运维，需要用户自己手动拉起来\n"
                        "（3）脚本提交方式，可能将未 merge 到 master 的本地代码提交到线上运行，留坑\n"
                        "=================================================================")
                except Exception, e:
                    print(e)
        return result

    def save_job_meta(self):
        dc = self.region
        cluster = self.cluster_name
        queue = self.queue_name
        job_name = self.topology_name
        owner = self.user

        application_name = job_name
        if self.topology_name and self.user:
            application_name = self.topology_name + "_" + self.user
        job_type = 'PyJstorm'
        version = self.version
        metrics_prefix = self.user_yaml_conf.get('common_args') \
            .get('metrics_namespace_prefix')
        topics = self.kafka_topics
        spout_list = self.user_yaml_conf.get('spout').get('spout_list')
        spouts = []
        if spout_list:
            for spout in spout_list:
                spouts.append(spout.get('spout_name'))
        bolt_list = self.user_yaml_conf.get('bolt').get('bolt_list')
        bolts = []
        if bolt_list:
            for bolt in bolt_list:
                bolts.append(bolt.get('bolt_name'))
        modify_time = time.strftime('%Y-%m-%d %H:%M:%S')
        job_meta = {'dc': dc, 'cluster': cluster, 'queue': queue,
                    'job_name': job_name, 'owner': owner,
                    'modify_time': modify_time,
                    'application_name': application_name, 'job_type': job_type,
                    'version': version, 'metrics_prefix': metrics_prefix,
                    'topics': json.dumps(topics), 'spouts': json.dumps(spouts),
                    'bolts': json.dumps(bolts)}
        mysql_utils = MysqlUtil()
        mysql_utils.update_job_meta(job_meta)

    def start_fail(self):
        print red("Start failed.")

    def stop(self):
        success = False
        app_id = None
        print green("start stop job : " + self.topology_name)
        retry = 0
        while True:
            try:
                job_states = YarnUtil.is_exists(self.user,
                                                self.region,
                                                self.cluster_name,
                                                self.queue_name,
                                                self.topology_name)
                if not job_states:
                    print green("job not exist")
                    break
                app_id = YarnUtil.get_running_application_id(self.user,
                                                             self.region,
                                                             self.cluster_name,
                                                             self.queue_name,
                                                             self.topology_name)
                success = YarnUtil.kill_job(self.user,
                                            self.region,
                                            self.cluster_name,
                                            self.queue_name,
                                            self.topology_name)
                print green("job stoped")
                break
            except Exception as e:
                retry = retry + 1
                if retry < STOP_JOB_MAX_RETRY:
                    print red("stop job failed, sleep %s seconds and retry" %
                              STOP_JOB_RETRY_INTERVAL_SECONDS)
                    time.sleep(STOP_JOB_RETRY_INTERVAL_SECONDS)
                else:
                    break

        if not self.skip_unimportance_process and success and app_id:
            # Reserve some time for the yarn kill application
            time.sleep(2)
            try:
                self.alert.unregister_alert()
                zk_util.clear_zk(self.zk_info, app_id)
                hdfs_util.clear_hdfs(self.hdfs_info,
                                     ConfUtils.get_flink_conf_dir(self.version),
                                     self.hadoop_conf_dir, app_id)
            except:
                traceback.print_exc()

        else:
            print 'Fail to clean zk & hdfs'
        return success

    def add_default_args(self):
        self.add_yarn_default()

    def add_yarn_default(self):
        # must set
        self.flink_args += ' -j ' + self.output_jar
        self.flink_args += ' -m yarn-cluster '
        flink_resource = self.flink_resource if self.flink_resource else FlinkResource()
        # yarn queue name
        if '-yqu' not in self.flink_args:
            self.flink_args += ' -yqu ' + self.queue_name
        # job manager memory
        if '-yjm' not in self.flink_args:
            if flink_resource.jm_memory:
                jm_memory = flink_resource.jm_memory
            else:
                jm_memory = 10240
            self.flink_args += ' -yjm ' + str(jm_memory)

        if '-yjv' not in self.flink_args:
            if flink_resource.jm_cores:
                jm_cores = flink_resource.jm_cores
            else:
                jm_cores = 1
            self.flink_args += " -yjv " + str(jm_cores)

        # task manager memory
        if '-ytm' not in self.flink_args:
            if flink_resource.tm_memory:
                tm_memory = flink_resource.tm_memory
            else:
                tm_memory = 10240
            self.flink_args += " -ytm " + str(tm_memory)
        # main class
        if '-c' not in self.flink_args:
            self.flink_args += ' -c storm.kafka.ExceptionMain '
        # yarn app name
        if '-ynm' not in self.flink_args:
            self.flink_args += ' -ynm ' + self.topology_name + "_" + self.user

        if self.flink_yarn_args:
            for key in self.flink_yarn_args:
                value = self.flink_yarn_args[key]
                if isinstance(value, basestring):
                    self.flink_args += ' -yD %s=%s' % (
                        key, self.flink_yarn_args[key].replace(" ", "#"))
                else:
                    self.flink_args += ' -yD %s=%s' % (
                        key, self.flink_yarn_args[key])

        if 'yarn.containers.vcores=' not in self.flink_args:
            if flink_resource.tm_cores:
                tm_cores = flink_resource.tm_cores
            else:
                tm_cores = self.vcores
            self.flink_args += ' -yD yarn.containers.vcores=%s ' % str(tm_cores)
        if 'containerized.heap-cutoff-ratio=' not in self.flink_args:
            self.flink_args += " -yD containerized.heap-cutoff-ratio=" \
                               + str(flink_resource.container_cutoff)
        if '-yd' not in self.flink_args:
            self.flink_args += ' -yd'
        self.flink_args += ' -yD metrics.reporter.opentsdb_reporter.jobname=%s' % self.topology_name
        # container number
        if ' -yn ' not in self.flink_args:
            if not flink_resource.tm_num:
                err_msg = "Can't get task manager counter."
                print red(err_msg)
                raise NoEnoughResArgs(err_msg)
            self.flink_args += ' -yn %s ' % flink_resource.tm_num
        # slots in per task manager
        if ' -ys ' not in self.flink_args:
            if not flink_resource.tm_slot:
                err_msg = "Can't get number of slots per task manager "
                print red(err_msg)
                raise NoEnoughResArgs(err_msg)
            self.flink_args += " -ys %s " % flink_resource.tm_slot
        if 'yarn.maximum-failed-containers' not in self.flink_args:
            # -1 represent to infinite times
            self.flink_args += ' -yD yarn.maximum-failed-containers=-1 '

        self.flink_args += " -yr " + "tm_num:" + str(
            self.flink_resource.tm_num) + \
                           ",tm_mem:" + str(self.flink_resource.tm_memory) + \
                           ",tm_cores:" + str(self.flink_resource.tm_cores) + \
                           ",tm_solt:" + str(self.flink_resource.tm_slot) + \
                           ",jm_mem:" + str(self.flink_resource.jm_memory)

        return

    def add_not_yarn_default(self):
        return

    def _yarn_mode(self):
        return 'yarn' in self.cluster_name

    def check_queue_resource(self, queue, flink_resource):
        try:
            mem_needed_mb = flink_resource.tm_num * flink_resource.tm_memory
            vcores_needed = flink_resource.tm_num * flink_resource.tm_cores
            queue_resource = YarnUtil.get_queue_resource(self.region,
                                                         self.cluster_name,
                                                         queue)
            mem_available_mb = queue_resource[YarnUtil.MEM_MAX_MB_KEY] - \
                               queue_resource[YarnUtil.MEM_USED_MB_KEY]
            vcores_available = queue_resource[YarnUtil.VCORES_MAX_KEY] - \
                               queue_resource[YarnUtil.VCORES_USED_KEY]
            if mem_available_mb < mem_needed_mb or vcores_available < vcores_needed:
                print red(
                    "#######################################################")
                print red("队列没有足够资源，mem(need:%d, available:%d), cores(need:%d"
                          ", avaliable:%s)" % (mem_needed_mb, mem_available_mb,
                                               vcores_needed, vcores_available))
                print red("处理建议：")
                print red("1. 优化job资源使用，资源优化指南: " + self.RESOURCES_OPTIMIZE_WIKI)
                print red("2. 联系 Flink 运维人员给队列扩容")
                print red(
                    "#######################################################")
                return False

            return True
        except Exception as e:
            return True

    def check_config(self, flink_resource):
        if flink_resource.use_large_vcores:
            max_vcores = TM_VCORES_MAX_CEILING
        else:
            max_vcores = TM_VCORES_MAX
        if flink_resource.tm_cores > max_vcores:
            print red("#######################################################")
            print red("tm_cores 最大为 %s，当前值： %s" % (
                str(max_vcores), str(flink_resource.tm_cores)))
            print red("#######################################################")
            return False

        if flink_resource.use_large_memory:
            max_memory = TM_MEMORY_MB_MAX_CEILING
        else:
            max_memory = TM_MEMORY_MB_MAX
        if flink_resource.tm_memory > max_memory:
            print red("#######################################################")
            print red("tm_memory 最大为 %s M，当前值：%s M" % (
                max_memory, str(flink_resource.tm_memory)))
            print red("#######################################################")
            return False

        job_info = self.yaml_util.get_job_info(self.kafka_server_url)
        max_parallelism = max([bolt.parallelism for bolt in job_info.bolts] +
                              [spout.parallelism for spout in job_info.spouts])
        total_solts = flink_resource.tm_num * flink_resource.tm_slot
        if total_solts < max_parallelism:
            print red("#######################################################")
            print red("solt 总数必须大于最大并发数：max_parallelism = %d" % max_parallelism)
            print red(
                "参数调整指南：https://wiki.bytedance.net/pages/viewpage.action?pageId=201216161")
            print red("#######################################################")
            return False

        return True


def parse_cmd():
    parse = argparse.ArgumentParser(description='restart cluster.')
    parse.add_argument('-a', '--action', type=str, choices=['start', 'stop'])
    parse.add_argument('-n', "--cluster_name", type=str, help="yarn cluster")
    parse.add_argument('-q', "--queue_name", type=str, help="yarn queue name")
    parse.add_argument('-y', "--yaml", type=str, help="yaml config location")
    parse.add_argument('-f', '--flink_args', type=str,
                       help="args directly delivery to flink")
    parse.add_argument('-j', '--base_jar', type=str, default=None)
    parse.add_argument('-d', '--detach', default=True, action='store_false')
    parse.add_argument('-u', '--user', type=str, default=None)
    parse.add_argument('-c', '--config_dir', type=str, default=None)
    parse.add_argument('-r', '--batch_runner', type=str, default=None)
    return parse


def ensure_no_log_conf(resource_dir):
    items = os.listdir(resource_dir)
    result = {}
    for f in items:
        if not os.path.isdir(os.path.join(resource_dir, f)):
            python_file = os.path.join(resource_dir, f)
            if not re.search("\.py$", python_file):
                continue
            res = check_log_conf_in_file(python_file)
            result[f] = res
    return result


def check_log_conf_in_file(python_file):
    result = []
    black_list = ['logging_config']
    with open(python_file) as f:
        line = f.readline()
        while line:
            for name in black_list:
                line = line.replace("\n", "").strip()
                log_conf_exist = name in line and \
                                 not re.search("^#", line) and \
                                 'import' not in line
                if log_conf_exist:
                    result.append(line)
            line = f.readline()
    return result


def main():
    args = parse_cmd().parse_args()
    yaml_file = os.path.abspath(args.yaml)
    if not os.path.isfile(yaml_file):
        print red(args.yaml + " does not exists.")
        return
    if args.user:
        user = args.user
    else:
        user = os.getlogin()
    if 'tiger' in user:
        print red("If you login as tiger, please use -u to specific your name")
        return

    queue_name = args.queue_name
    topology = FlinkTopology(cluster_name=args.cluster_name,
                             queue_name=queue_name,
                             yaml_file=yaml_file, config_dir=args.config_dir,
                             flink_args=args.flink_args, base_jar=args.base_jar,
                             user=user, batch_runner=args.batch_runner,
                             version=args.version)
    if args.action == 'start':
        topology.start()
    elif args.action == 'stop':
        topology.stop()


if __name__ == '__main__':
    """
    example: /opt/tiger/flink_deploy/tools/flink_topology.py start -y topology_online.yaml
        -n flink -q root.flink_cluster.online
    """
    main()
