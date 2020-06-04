#!/usr/bin/env python
# -*- coding:utf-8 -*-

import math
import sys
import yaml

class FlinkResourceUtil(object):
    """ generate flink resource config """

    TM_JVM_MEMORY_DEFAULT = 1024
    TM_JVM_NETWORK_DEFAULT = 104857600
    TM_MEMORY_MAX = 30 * 1024
    TM_SLOT_MAX = 10

    USER_CPU_METRICS = "inf.dtop.%s.used.USR.all"
    SYS_CPU_METRICS = "inf.dtop.%s.used.SYS.all"
    TOTAL_MEM_METRICS = "inf.dtop.%s.used.MEM.all"
    JVM_MEM_METRICS = "inf.dtop.%s.used.JHU.all"

    JSTORM_TOPOLOGY_MEMORY_TEMPLATE = "inf.jstorm.%s.topology.TotalRssMemory{topology=%s}"
    JSTORM_TOPOLOGY_CPU_TEMPLATE = "inf.jstorm.%s.topology.TotalCpu{topology=%s}"

    REGION_MAP = {
        "cn": {
            "flink": "cn",
            "dw": "cn"
        },
        "sg": {
            "alisg": "cn"
        },
        "va": {
            "maliva": "cn",
            "awsva": "cn"
        }
    }

    @staticmethod
    def generate(parallelism, total_memory_mb, total_cores, max_slot_per_tm=2):
        """
        :param parallelism:
        :param total_memory_mb:
        :param total_cores:
        :return: flink_resource_conf dict
        {
            "flink_resource_args": {
                    "tm_slot": xxx,
                    "tm_memory": xxx,
                    "tm_num": xxx,
                    "tm_cores": xxx
            },
            "flink_args": {
                "containerized.heap-cutoff-ratio": xxx
            }
        }
        """
        total_memory_mb = int(total_memory_mb * 0.7)
        total_cores = int(total_cores * 0.7)

        if not max_slot_per_tm:
            min_tm_num = 1
        else:
            min_tm_num = int(math.ceil(parallelism * 1.0 / max_slot_per_tm))

        need_degrade = False
        flink_conf_map = None
        for tm_num in [x for x in range(min_tm_num, parallelism + 1)]:
            tm_py_mem = int(math.ceil(total_memory_mb * 1.0 / tm_num))
            tm_mem = tm_py_mem + FlinkResourceUtil.TM_JVM_MEMORY_DEFAULT
            if tm_mem >= FlinkResourceUtil.TM_MEMORY_MAX:
                continue

            tm_cores = int(round(total_cores * 1.0 / tm_num))
            if tm_cores > 8:
                continue
            if tm_cores == 0:
                need_degrade = True
                tm_cores = 1

            tm_slot = int(math.ceil(parallelism * 1.0 / tm_num))
            heap_cutoff_ratio = math.trunc(tm_py_mem * 100 / tm_mem) / 100.0

            flink_conf_map = {
                "flink_resource_args": {
                    "tm_slot": tm_slot,
                    "tm_memory": tm_mem,
                    "tm_num": tm_num + (3 if tm_num > 10 else 1),
                    "tm_cores": tm_cores
                },
                "flink_args": {
                    "containerized.heap-cutoff-ratio": heap_cutoff_ratio,
                    "taskmanager.network.memory.max": FlinkResourceUtil.TM_JVM_NETWORK_DEFAULT
                }
            }
            if not need_degrade:
                return flink_conf_map

        tm_nums = range(1, min_tm_num)
        tm_nums.reverse()
        need_degrade = False
        for tm_num in [x for x in tm_nums]:
            tm_py_mem = int(math.ceil(total_memory_mb * 1.0 / tm_num))
            tm_mem = tm_py_mem + FlinkResourceUtil.TM_JVM_MEMORY_DEFAULT
            if tm_mem >= FlinkResourceUtil.TM_MEMORY_MAX:
                continue

            tm_cores = int(round(total_cores * 1.0 / tm_num))
            if tm_cores > 8:
                continue
            if tm_cores == 0:
                need_degrade = True
                tm_cores = 1
            else:
                need_degrade = False

            tm_slot = int(math.ceil(parallelism * 1.0 / tm_num))
            heap_cutoff_ratio = math.trunc(tm_py_mem * 100 / tm_mem) / 100.0

            flink_conf_map = {
                "flink_resource_args": {
                    "tm_slot": tm_slot,
                    "tm_memory": tm_mem,
                    "tm_num": tm_num + (3 if tm_num > 10 else 1),
                    "tm_cores": tm_cores
                },
                "flink_args": {
                    "containerized.heap-cutoff-ratio": heap_cutoff_ratio,
                    "taskmanager.network.memory.max": FlinkResourceUtil.TM_JVM_NETWORK_DEFAULT
                }
            }
            if not need_degrade:
                return flink_conf_map

        return flink_conf_map

    @staticmethod
    def dump(resource_conf_dict):
        """ flink_resource_conf dict -> yaml str """
        return yaml.dump(resource_conf_dict, default_flow_style=False)

    @staticmethod
    def generate_by_usage(parallelism, app_name, max_slot_per_tm=2, min_running_hour=0, region=None, cluster=None, metric_force=True):
        if metric_force:
            resources = FlinkResourceUtil.get_dtop(app_name, min_running_hour=min_running_hour, region=region, cluster=cluster)
        else:
            resources = FlinkResourceUtil.get_resources(app_name, min_running_hour=min_running_hour, region=region, cluster=cluster)
            if not resources:
                resources = FlinkResourceUtil.get_dtop(app_name, min_running_hour=min_running_hour, region=region, cluster=cluster)
        if not resources:
            return None

        return FlinkResourceUtil.generate(parallelism, resources[1] * 1.6, resources[0] * 1.6, max_slot_per_tm=max_slot_per_tm)

    @staticmethod
    def get_dtop(app_name, min_running_hour=0, region=None, cluster=None):
        from metric_util import MetricUtil
        user_cpu_metric = FlinkResourceUtil.USER_CPU_METRICS % app_name
        sys_cpu_metric = FlinkResourceUtil.SYS_CPU_METRICS % app_name
        total_mem_metric = FlinkResourceUtil.TOTAL_MEM_METRICS % app_name
        jvm_mem_metric = FlinkResourceUtil.JVM_MEM_METRICS % app_name

        if region:
            region = FlinkResourceUtil.REGION_MAP.get(region).get(cluster);
        user_cpu_data = MetricUtil.get_metric_data('24h-ago', '', 'max:1h-avg', user_cpu_metric, region=region)
        sys_cpu_data = MetricUtil.get_metric_data('24h-ago', '', 'max:1h-avg', sys_cpu_metric, region=region)
        total_mem_data = MetricUtil.get_metric_data('24h-ago', '', 'max:1h-avg', total_mem_metric, region=region)
        jvm_mem_data = MetricUtil.get_metric_data('24h-ago', '', 'max:1h-avg', jvm_mem_metric, region=region)

        if not user_cpu_data or not total_mem_data:
            raise Exception("dtop data empty")

        if len(user_cpu_data) < min_running_hour or len(total_mem_data) < min_running_hour:
            raise Exception("min running hour " + str(min_running_hour) + " hours, only running " + str(len(user_cpu_data)) + " hours")

        max_cpu = 0
        for ts,user_cpu in user_cpu_data.items():
            sys_cpu = sys_cpu_data.get(ts, 0)
            total = user_cpu + sys_cpu
            max_cpu = max(max_cpu, total)

        max_py_mem = 0
        for ts, total_mem in total_mem_data.items():
            jvm_mem = jvm_mem_data.get(ts, -1)
            if jvm_mem < 0:
                continue
            py_mem = total_mem - jvm_mem
            max_py_mem = max(max_py_mem, py_mem)

        if max_cpu <= 0 or max_py_mem <= 0:
            return None

        return (max_cpu, max_py_mem)

    @staticmethod
    def get_resources(app_name, min_running_hour=0, region=None, cluster=None):
        if region and region != "cn":
            return None

        idx = app_name.rfind("_")
        if idx != -1:
            app_name = app_name[:idx]

        region = "cn"
        if not cluster:
            cluster = "flink"

        try:
            from sr_utils import SmartResourcesUtils
            usage = SmartResourcesUtils.estimate_application_resources_usage(region, cluster, app_name)
            if usage["durtion"] < min_running_hour * 60:
                return None
            return (int(usage["cpuTotalVcores"]), int(usage["memNonJvmUsedMB"]))
        except Exception as e:
            print e
            return None

    @staticmethod
    def get_topology_cpu_core(cluster, topology):
        from metric_util import MetricUtil
        if not cluster:
            return None
        metric_name = FlinkResourceUtil.JSTORM_TOPOLOGY_CPU_TEMPLATE % (cluster, topology)
        metric_data = \
            MetricUtil.get_metric_data('24h-ago', '', 'sum:1h-max', metric_name)
        if not metric_data:
            raise Exception("no cpu metric data")
        return math.ceil(max(metric_data.values()) / 100.0)

    @staticmethod
    def get_topology_memoryMB(cluster, topology):
        from metric_util import MetricUtil
        if not cluster:
            return None
        metric_name = FlinkResourceUtil.JSTORM_TOPOLOGY_MEMORY_TEMPLATE % (cluster, topology)
        metric_data = MetricUtil.get_metric_data('24h-ago', '', 'sum:1h-max', metric_name)
        if metric_data is None:
            raise Exception("no mem metric data")
        # transfer to MB
        return max(metric_data.values()) / (1 << 20)

if __name__ == '__main__':
    region = sys.argv[1]
    cluster = sys.argv[2]
    app_name = sys.argv[3]
    parallelism = int(sys.argv[4])
    max_slot_per_tm = int(sys.argv[5])
    metric_force = False
    if len(sys.argv) == 7:
        metric_force = True

    config = FlinkResourceUtil.generate_by_usage(parallelism,
                                                 app_name,
                                                 max_slot_per_tm,
                                                 region=region,
                                                 cluster=cluster,
                                                 metric_force=metric_force)
    print FlinkResourceUtil.dump(config)



