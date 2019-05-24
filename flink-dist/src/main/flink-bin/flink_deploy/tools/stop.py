#!/usr/bin/env python
# -*- coding: utf-8 -*-

from conf_utils import ConfUtils
from util import *
from yarn_util import YarnUtil

import argparse
import hdfs_util
import os
import sys
import zk_util

"""
usage: stop.py [-h] [-r REGION] [-c CLUSTER] -a APP_ID
example: ﻿/opt/tiger/flink_deploy/tools/stop.py -r va -c awsva -a application_1515971269152_106898 

1. Known available region:﻿ va,sg,cn(default)
2. Known available cluster:﻿ cn: default,flink(default),dw ...
                            sg: alisg 
                            va: maliva
"""
DEFAULT_VERSION = "1.5"
VALID_VERSIONS = ["1.5"]


class YarnApplication:
    def get_cmd_parser(self):
        self.parser = argparse.ArgumentParser()
        self.parser.add_argument("-r", "--region", default="cn",
                                 help="where the cluster locates")
        self.parser.add_argument("-c", "--cluster", default="flink")
        self.parser.add_argument("-a", "--app_id", help="application id")
        self.parser.add_argument("-j", "--job_name", help="job name")
        self.parser.add_argument("-q", "--queue", help="queue name")
        self.parser.add_argument('-v', '--version', type=str,
                                 help='flink version', default="1.5")
        self.parser.add_argument('-u', "--user", type=str,
                                 help="specify user")
        return self.parser

    def __init__(self):
        self.parser = self.get_cmd_parser()
        args = self.parser.parse_args()
        self.region = args.region
        self.cluster = ConfUtils.replace_cluster_conf(args.cluster)
        self.flink_version = args.version
        if self.flink_version not in VALID_VERSIONS:
            self.flink_version = DEFAULT_VERSION
        self.application_id = args.app_id
        self.job_name = args.job_name
        self.queue = args.queue
        if args.user:
            self.user = args.user
        else:
            self.user = os.getlogin()
        if 'tiger' in self.user:
            print red("If you login as tiger, please use -u to specific your name")
            return
        self.config_dir = ConfUtils.get_flink_conf_dir(self.flink_version)
        self.flink_conf = ConfUtils.get_flink_conf(self.cluster,
                                                   self.flink_version)
        self.hadoop_conf_dir = self.flink_conf.get('HADOOP_CONF_DIR')
        self.zk_info = zk_util.get_zk_info(self.flink_conf)
        print self.zk_info
        self.hdfs_info = hdfs_util.get_hdfs_info(self.flink_conf)

    def kill(self):
        if self.application_id:
            success = YarnUtil.kill_app(self.user, self.region, self.cluster,
                                        self.application_id)
        else:
            success = YarnUtil.kill_job(self.user, self.region, self.cluster,
                                        self.queue, self.job_name)
        if success:
            if self.application_id:
                print green("success to kill application: " + self.application_id)
            else:
                print green("success to kill job: " + self.job_name)
            try:
                if self.application_id:
                    zk_util.clear_zk(self.zk_info,
                                     self.application_id)
                    hdfs_util.clear_hdfs(self.hdfs_info,
                                         self.config_dir,
                                         self.application_id)
            except Exception as e:
                print 'Fail to clean zk & hdfs'
        else:
            print red("failed to kill application: " + self.application_id)
            sys.exit(-1)


def main():
    app = YarnApplication()
    app.kill()


if __name__ == "__main__":
    main()
