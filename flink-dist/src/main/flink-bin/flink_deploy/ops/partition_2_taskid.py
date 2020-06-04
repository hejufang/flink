#!/usr/bin/env python
# -*- coding:utf-8 -*-

import argparse


def _parse_args():
    parser = argparse.ArgumentParser(description="trans partition to taskid")
    parser.add_argument('-sp', '--spout_parallelism', type=str, required=True, help="spout parallelism")
    parser.add_argument('-tp', '--topic_partition', type=str, required=True, help="topic partition")
    parser.add_argument('-p', '--partition', type=str, required=True, help="partition")
    return parser.parse_args()

if __name__ == '__main__':
    args = _parse_args()
    tp = int(args.topic_partition)
    sp = int(args.spout_parallelism)
    p = int(args.partition)
    if sp >= tp:
        print "TaskId:%s" % (p + 1)
        exit(0)

    mod = tp / sp
    rem = tp % sp
    start = 0
    for i in range(0, sp, 1):
        end = start + mod + (1 if i < rem else 0)
        if p < end:
            print "TaskId:%s" % (i + 1)
            exit(0)

        start = end

    print "ERROR"