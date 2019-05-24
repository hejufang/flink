#!/usr/bin/env python
#coding:utf-8
import json
import math
import os
import subprocess
import sys

from datetime import datetime


class SmartResourcesUtils(object):
    """ SmartResources Client wrapper """

    JM_JVM_MEMORY_DEFAULT = 1024
    TM_JVM_MEMORY_DEFAULT = 1024
    TM_JVM_NETWORK_DEFAULT = 10485760
    TM_MEMORY_MAX = 30 * 1024
    TM_CORES_MAX = 8
    TM_SLOT_MAX = 5

    NETWORK_BUFFER_KB = 32

    LATEST_RESOURCES_KEY_PREFIX = "sr_application_latest_resources_%s_%s_%s"

    MIN_DURTION_MINUTES = 24 * 60
    MEM_RESERVE_RATIO = 0.3
    CPU_RESERVE_RATIO = 0.1
    MAX_SLOT_PER_TM = 10
    MSG_SIZE_KB = 50

    ESTIMATER_CONF = {
        "cn": {
            "flink": "data.inf.sr_estimater.service.lf",
            "dw": "data.inf.sr_estimater.service.lf",
            "hl": "data.inf.sr_estimater.service.lf",
            "wj": "data.inf.sr_estimater.service.lf"
        },
        "sg": {
            "alisg": "data.inf.sr_estimater.service.alisg"
        },
        "va": {
            "awsva": "data.inf.sr_estimater.service.va",
            "maliva": "data.inf.sr_estimater.service.maliva"
        }
    }

    ESTIMATER_REDIS_CONF = {
        "cn": {
            "flink": "toutiao.redis.smart_resources.service.lf",
            "dw": "toutiao.redis.smart_resources.service.lf",
            "hl": "toutiao.redis.smart_resources.service.lf",
            "wj": "toutiao.redis.smart_resources.service.lf"
        },
        "sg": {
            "alisg": "toutiao.redis.smart_resources.service.alisg"
        },
        "va": {
            "awsva": "toutiao.redis.smart_resources.service.va",
            "maliva": "toutiao.redis.smart_resources.service.maliva"
        }
    }

    ESTIMATE_CMD = "/opt/tiger/jdk/jdk1.8/bin/java -cp %s com.bytedance.sr.utils.Estimater estimate -r %s -c %s -e %s -d %s -n %s"

    @staticmethod
    def estimate_application_resources_usage(region, cluster, app_name, durtion_min = 24 * 60):
        """
        :param region: yarn region, cn, va, alisg, etc...
        :param cluster: yarn cluster, flink, dw, etc...
        :param app_name: application name, without user name
        :param durtion_min: use recent {durtion_min} minutes records estimate usage
        :return: resources usage dict
        {
          "cpuTotalVcores": 1195,
          "durtion": 720,               // estimate based on the {durtion} minutes recoreds
          "memJvmCapMB": 438894,        // max jvm cap MB
          "memJvmUsedMB": 252009,       // max jvm used mem
          "memNonJvmUsedMB": 822055,    // max non jvm used mem
          "memTotalMB": 1063586         // max total mem, memJvmUsedMB + memNonJvmUsedMB > memTotalMB
        }
        """
        region_conf = SmartResourcesUtils.ESTIMATER_CONF.get(region)
        if not region_conf:
            raise Exception("unsupport region")

        estimater_service_name = region_conf.get(cluster)
        if not estimater_service_name:
            raise Exception("unsupport cluster")

        lib_path = os.path.join(os.path.dirname(__file__),
                                "./sr/lib/sr-utils-1.0-SNAPSHOT.jar")
        cmd = SmartResourcesUtils.ESTIMATE_CMD % (lib_path, region, cluster,
                                                  estimater_service_name,
                                                  durtion_min, app_name)
        try:
            process = subprocess.Popen(cmd,
                                       stdout=subprocess.PIPE,
                                       stderr=subprocess.PIPE,
                                       shell=True)
            result = process.communicate()
            return json.loads(result[0])
        except Exception as e:
            print result
            raise Exception("get resources usage error, " + e.message)

    @staticmethod
    def generate_by_usage(region, cluster, app_name, parallelism, sr_args):
        durtion_min = sr_args.get("min_durtion_minutes", SmartResourcesUtils.MIN_DURTION_MINUTES)
        usage = SmartResourcesUtils.estimate_application_resources_usage(region,
                                                                         cluster,
                                                                         app_name,
                                                                         durtion_min=durtion_min)
        print "resources from estimater server : " + str(usage)
        if not usage or usage["durtion"] < durtion_min:
            return None

        mem_reserve_ratio = sr_args.get("mem_reserve_ratio", SmartResourcesUtils.MEM_RESERVE_RATIO)
        cpu_reserve_ratio = sr_args.get("cpu_reserve_ratio", SmartResourcesUtils.CPU_RESERVE_RATIO)
        total_memory_mb = int(usage["memNonJvmUsedMB"] * (1 + mem_reserve_ratio))
        total_cores = int(usage["cpuTotalVcores"] * (1 + cpu_reserve_ratio))

        max_slot_per_tm = sr_args.get("max_slot_per_tm", SmartResourcesUtils.MAX_SLOT_PER_TM)
        if not max_slot_per_tm:
            min_tm_num = 1
        else:
            min_tm_num = int(math.ceil(parallelism * 1.0 / max_slot_per_tm))

        result = SmartResourcesUtils.calc_resources_args(parallelism,
                                                         total_memory_mb,
                                                         total_cores,
                                                         range(1, parallelism))
        return result;

    @staticmethod
    def calc_resources_args(parallelism, total_memory_mb, total_cores, tm_num_seeds):
        """ use specified tm_nums calc resources config, return the first matched """
        config_seeds = []
        need_degrade = True
        flink_conf_map = None
        for tm_num in tm_num_seeds:
            tm_py_mem = int(math.ceil(total_memory_mb * 1.0 / tm_num))
            tm_mem = tm_py_mem + SmartResourcesUtils.TM_JVM_MEMORY_DEFAULT
            if tm_mem >= SmartResourcesUtils.TM_MEMORY_MAX:
                continue

            tm_cores = int(total_cores * 1.0 / tm_num)
            if tm_cores > 8:
                continue
            if tm_cores == 0:
                need_degrade = True
                tm_cores = 1
            else:
                need_degrade = False

            tm_slot = int(math.ceil(parallelism * 1.0 / tm_num))
            heap_cutoff_ratio = math.trunc(tm_py_mem * 100 / tm_mem) / 100.0
            if heap_cutoff_ratio < 0.1:
                heap_cutoff_ratio = 0.1

            flink_conf_map = {
                "flink_resource_args": {
                    "tm_slot": tm_slot,
                    "tm_memory": tm_mem,
                    "tm_num": tm_num + (3 if tm_num > 10 else 1),
                    "tm_cores": tm_cores,
                    "jm_memory": SmartResourcesUtils.JM_JVM_MEMORY_DEFAULT
                },
                "flink_args": {
                    "containerized.heap-cutoff-ratio": heap_cutoff_ratio,
                    "taskmanager.network.memory.max": SmartResourcesUtils.TM_JVM_NETWORK_DEFAULT,
                    "taskmanager.network.memory.min": SmartResourcesUtils.TM_JVM_NETWORK_DEFAULT
                }
            }
            slot_waste = tm_slot * tm_slot - parallelism;
            config_seeds.append((need_degrade, slot_waste, flink_conf_map))

        def config_cmp(a, b):
            if a[0] and not b[0]: return 1
            if not a[0] and b[0]: return -1
            return a[1] - b[1]
        config_seeds.sort(config_cmp)
        if (len(config_seeds) > 0):
            return config_seeds[0][2]
        else:
            return None

    @staticmethod
    def update_latest_resources(region, cluster, app_name, total_mem_mb, total_cpu):
        redis = SmartResourcesUtils.build_estimater_redis_client(region, cluster)
        key = SmartResourcesUtils.LATEST_RESOURCES_KEY_PREFIX % (region, cluster, app_name)
        redis.rpush(key, json.dumps({
            "mem": total_mem_mb,
            "cpu": total_cpu,
            "starttime": str(datetime.now())}))
        if redis.llen(key) > 10:
            redis.lpop(key)

    @staticmethod
    def get_latest_resources(region, cluster, app_name):
        redis = SmartResourcesUtils.build_estimater_redis_client(region, cluster)
        key = SmartResourcesUtils.LATEST_RESOURCES_KEY_PREFIX % (region, cluster, app_name)
        resources_json = redis.lindex(key, -1)
        if resources_json:
            return json.loads(resources_json)
        return None

    @staticmethod
    def build_estimater_redis_client(region, cluster):
        region_conf = SmartResourcesUtils.ESTIMATER_REDIS_CONF.get(region)
        if not region_conf:
            raise Exception("unsupport region")

        estimater_redis_name = region_conf.get(cluster)
        if not estimater_redis_name:
            raise Exception("unsupport cluster")

        from pyutil import pyredis
        return pyredis.make_redis_client(estimater_redis_name)

    @staticmethod
    def generate_config(region, cluster, app_name, job_info, sr_args):
        """
        :param region: yarn region, cn, va, alisg, etc...
        :param cluster: yarn cluster, flink, dw, etc...
        :param app_name: application name, without user name
        :param job_info: see JstormJobInfo
        :param max_slot_per_tm:
        :param durtion_min: use recent {durtion_min} minutes records estimate usage
        :param cpu_reserve_ratio:
        :param mem_reserve_ratio:
        :param msg_size_kb: max size of per msg
        :return: flink resources config
        """
        if not job_info.bolts or len(job_info.bolts) == 0:
            return SmartResourcesUtils.generate_by_usage(region, cluster, app_name,
                                                         max([x.parallelism for x in job_info.spouts]),
                                                         sr_args)

        durtion_min = sr_args.get("min_durtion_minutes", SmartResourcesUtils.MIN_DURTION_MINUTES)
        try:
            usage = SmartResourcesUtils.estimate_application_resources_usage(region,
                                                                             cluster,
                                                                             app_name,
                                                                             durtion_min=durtion_min)
        except:
            usage = None

        if not sr_args.get("enable_force", False):
            if not usage or usage["durtion"] < durtion_min:
                return None

        if usage:
            mem_reserve_ratio = sr_args.get("mem_reserve_ratio",
                                            SmartResourcesUtils.MEM_RESERVE_RATIO)
            cpu_reserve_ratio = sr_args.get("cpu_reserve_ratio",
                                            SmartResourcesUtils.CPU_RESERVE_RATIO)
            total_nonjvm_memory_mb = int(usage["memNonJvmUsedMB"] * (1 + mem_reserve_ratio))
            total_jvm_memory_mb = int(usage["memJvmUsedMB"] * (1 + mem_reserve_ratio))
            total_cores = int(usage["cpuTotalVcores"] * (1 + cpu_reserve_ratio))
        else:
            total_nonjvm_memory_mb = None
            total_jvm_memory_mb = None
            total_cores = max([x.parallelism for x in job_info.spouts]) / 10
            if total_cores < 1:
                total_cores = 1

        # {
        #   name1 -> (name1, parallelism, input_channels, output_channels),
        #   name2-> (name2, parallelism, input_channels, output_channels)
        # }
        components = {}
        for spout in job_info.spouts:
            components[spout[0]] = (spout[0], spout[1], 0, 0)

        for bolt in job_info.bolts:
            components[bolt[0]] = (bolt[0], bolt[1], 0, 0)

        for bolt in job_info.bolts:
            input_channels = 0
            for stream in bolt.stream_from:
                upstream = components[stream]
                components[stream] = (upstream[0], upstream[1], upstream[2], upstream[3] + bolt[1])
                input_channels = input_channels + components[stream][1]
            components[bolt[0]] = (bolt[0], bolt[1], input_channels, components[bolt[0]][3])

        # [(name1, parallelism, SERIALIZER_BUFFER_KB, NETWORK_BUFFER_KB), ...]
        msg_size_kb = sr_args.get("msg_size_kb", SmartResourcesUtils.MSG_SIZE_KB)
        if msg_size_kb < SmartResourcesUtils.MSG_SIZE_KB:
            msg_size_kb = SmartResourcesUtils.MSG_SIZE_KB
        components = [(x[0], x[1], msg_size_kb * (x[2] + x[3]),
                       SmartResourcesUtils.NETWORK_BUFFER_KB * 3 * (x[2] + x[3] + 8))
                      for x in components.values()]
        components = sorted(components, key=lambda x : x[1])
        parallelisms = sorted(list(set([x[1] for x in components])))
        for idx in range(0, len(parallelisms) - 1):
            parallelisms = parallelisms[idx:]
            divisors = SmartResourcesUtils.get_all_divisors(parallelisms)
            for divisor in divisors:
                config = SmartResourcesUtils.calc_resources(divisor,
                                                            components,
                                                            total_nonjvm_memory_mb,
                                                            total_jvm_memory_mb,
                                                            total_cores)
                if config:
                    return config

        max_slot_per_tm = sr_args.get("max_slot_per_tm", SmartResourcesUtils.MAX_SLOT_PER_TM)
        tm_num_desired = int(components[-1][1] / max_slot_per_tm)

        # tm_num_desired -> max
        for tm_num in range(tm_num_desired, components[-1][1] + 1):
            config = SmartResourcesUtils.calc_resources(tm_num,
                                                        components,
                                                        total_nonjvm_memory_mb,
                                                        total_jvm_memory_mb,
                                                        total_cores)
            if config:
                return config

        # tm_num_desired -> min
        for tm_num in range(tm_num_desired, 0, -1):
            config = SmartResourcesUtils.calc_resources(tm_num,
                                                        components,
                                                        total_nonjvm_memory_mb,
                                                        total_jvm_memory_mb,
                                                        total_cores)
            if config:
                return config

        return None


    @staticmethod
    def calc_resources(tm_num, components, total_nonjvm_memory_mb, total_jvm_memory_mb, total_cores):
        parallelism_max = max([x[1] for x in components])
        tm_slot = math.ceil(parallelism_max / (tm_num * 1.0))

        tm_cores = int(round(total_cores / (tm_num * 1.0)))
        if tm_cores > SmartResourcesUtils.TM_CORES_MAX:
            return None
        if tm_cores == 0:
            tm_cores = 1

        jvm_memory_mb = 1024
        network_memory_kb = 0
        for component in components:
            num_per_tm = math.ceil(component[1] / (tm_num * 1.0))
            jvm_memory_mb = jvm_memory_mb + (component[2] + component[3]) * num_per_tm / 1024.0
            network_memory_kb = network_memory_kb + component[3] * num_per_tm

        if total_nonjvm_memory_mb:
            nonjvm_memory = total_nonjvm_memory_mb / (tm_num * 1.0)
        else:
            nonjvm_memory = jvm_memory_mb

        total_memory = int(round(nonjvm_memory + jvm_memory_mb))
        if total_memory > SmartResourcesUtils.TM_MEMORY_MAX:
            return None

        heap_cutoff_ratio = math.trunc(nonjvm_memory * 100 / total_memory) / 100.0
        network_fraction = int(math.ceil(math.trunc(network_memory_kb * 1000 / (jvm_memory_mb * 1024)) / 10.0)) / 100.0
        flink_conf_map = {
            "flink_resource_args": {
                "tm_slot": int(tm_slot),
                "tm_memory": int(total_memory),
                "tm_num": tm_num + (3 if tm_num > 10 else 1),
                "tm_cores": tm_cores
            },
            "flink_args": {
                "containerized.heap-cutoff-ratio": heap_cutoff_ratio,
                "taskmanager.network.memory.fraction": network_fraction,
                "taskmanager.network.memory.max": int(network_memory_kb * 1024),
                "taskmanager.network.memory.min": int(network_memory_kb * 1024)
            }
        }
        return flink_conf_map

    @staticmethod
    def get_all_divisors(values):
        min_val = min(values)
        divisors = [1]
        for i in range(2, min_val + 1):
            ok = True
            for val in values:
                if val % i != 0:
                    ok = False
                    break
            if ok:
                divisors.append(i)
        return divisors

if __name__ == '__main__':
    region = sys.argv[1]
    cluster = sys.argv[2]
    app_name = sys.argv[3]
    if len(sys.argv) == 4:
        print SmartResourcesUtils.estimate_application_resources_usage(region, cluster, app_name)
    else:
        print SmartResourcesUtils.estimate_application_resources_usage(region, cluster, app_name, sys.argv[4])
