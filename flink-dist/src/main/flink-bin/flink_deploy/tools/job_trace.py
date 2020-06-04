#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import json
from pyutil.redis.redis_proxy import make_redis_proxy_cli
from util import red, green


advises = {'TM_LOST_KILLED': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=148308354',
           'TM_CLOSE_CONNECTION': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=148308354',
           'TM_STATE_CHANGED': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=148308354',
           'OOM': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=148308356',
           'CALL_STACK': 'Root Cause, 排查自己的代码是否有问题',
           'SIGNAL_15': '',
           'OUTPUT_FILEDS_NOT_MATCH': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=148308352',
           'IMPORT_ERROR': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=148308350',
           'MEM_BEYOND_LIMIT': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=149693955',
           'KILLED_BY_YARN': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=149694006',
           'KILLED_BY_SYS': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=149716032',
           'YRAN_NM_LOST': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=151752143',
           'SPOUT_ONLY_AND_RETURN_NOT_EMPTY': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=166838154',
           'TMP_DIR_NOT_EXIST': 'https://wiki.bytedance.net/pages/viewpage.action?pageId=166838203',
           'PRINT_STH_IN_STD': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=166838277',
           'INSUFFICIENT_NETWORK_BUFFERS': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=158232925',
           'SS_CONF_NEED_UPDATE': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=166838328',
           'BIND_PORT_FAILED': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=193412268',
           'JM_INIT_FAILED': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=193412489',
           'TM_INIT_FAILED': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=193412424',
           'NO_ENOUGH_SLOTS': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=201216161',
           'JAR_CONFLICTS': 'Root Cause, 包冲突',
           'CORE_DUMPED': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=228315778',
           'CORE_NOT_DUMPED': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=228315778',
           'NETTY_COMMON_CONFLICT': 'Root Cause, https://wiki.bytedance.net/pages/viewpage.action?pageId=235617696'}


def _parase_args():
    parser = argparse.ArgumentParser(description = 'tarce job history')
    parser.add_argument('-a', '--app_id', type = str, required = True,
                        help = 'application id')
    parser.add_argument('-o', '--original', type = bool, required = False,
                        default=True, help = 'show original log')
    parser.add_argument('-e', '--earliest', type = int, required = False,
                        default=None, help = 'show earliest num lines logs')
    parser.add_argument('-l', '--latest', type = int, required = False,
                        default=None, help = 'show latest num lines logs')
    parser.add_argument('-b', '--begin', type = str, required = False,
                        default=None, help = 'show error log from the specified '
                                             'time, example : 2017-12-02_17:15:30')
    parser.add_argument('-c', '--count', type = int, required = False,
                        default=None, help = 'show specified count error logs')
    parser.add_argument('-u', '--union', type = str, required = False,
                        default='yes', help = 'union same error log or not, yes/no')
    return parser.parse_args()


def main():
    args = _parase_args()

    print_useage()

    key = "flink_trace_" + args.app_id

    for trace_info in fetch_traces(key, earliest=args.earliest, latest=args.latest,
                                   begin=args.begin, count=args.count, union=args.union):
        print "\n"
        tags = trace_info.get('tags', '')
        if tags:
            for tag in tags.split(','):
                print red("处理建议：" + advises.get(tag, "暂无"))
        print green(trace_info['transed-old'])
        if args.original:
            print trace_info['log']

    print_useage()


def fetch_traces(key, earliest = None, latest = None, begin = None, count = None, union = None):
    ret = []

    redis = build_redis_client()

    idx = 0
    while idx < redis.llen(key):
        for trace in redis.lrange(key, idx, idx + 49):
            trace_info = json.loads(trace)
            ret.append(trace_info)

        idx = idx + 50

    ret = sorted(ret, key = lambda x : x['time'])
    if union and union == 'yes':
        tmp = []
        last = None
        for trace in ret:
            if last == trace['tags']:
                continue
            last = trace['tags']
            tmp.append(trace)
        ret = tmp

    if earliest and len(ret) > earliest:
        ret = ret[:earliest]
    elif latest and len(ret) > latest:
        ret = ret[0 - latest:]
    elif begin:
        ret = filter(lambda x : x['time'] > begin, ret)
        if len(ret) > count:
            ret = ret[:count]

    return ret


def print_useage():
    print green("jobtrace useage:")
    print green("1. 请先根据处理建议参照 wiki 自行处理，处理不了的到 flink-users 群联系 @caojianhua @zhangguanghui")
    print green("2. 日志采集会有 3 分钟左右的延时")
    print green("3. 错误日志目前只保留了最新的 10000 条")
    print green("4. 错误日志目前做多保留 14 天")
    print green("5. 2017-12-03 之前启动的job，由于 databus4j 的问题会导致日志采集不全，需要重启")
    print green("6. 由于在将 job kill 之后yarn会将日志清除，所以如果需要登录机器查看详细日志，先不要 stop job")
    print green("\n\n")


def build_redis_client():
    # redis_conf = Conf("/opt/tiger/ss_conf/ss/redis.conf")
    # return make_redis_proxy_cli(redis_conf.get_values('redis_data_cache_misc'),
    #                             socket_timeout=3.0, socket_connect_timeout=1.0,
    #                             strict_redis=False)

    addrs = "10.6.16.160:3704,10.6.16.162:3706,10.6.17.48:3706,10.6.17.49:3702," \
            "10.6.17.50:3704,10.6.18.15:3705,10.6.18.223:3708,10.6.18.84:3702," \
            "10.6.27.146:3707,10.6.27.147:3706".split(",")
    return make_redis_proxy_cli(addrs, socket_timeout=3.0,
                                socket_connect_timeout=1.0, strict_redis=False)


if __name__ == '__main__':
    main()
