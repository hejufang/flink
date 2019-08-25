#!/usr/bin/env python
# -*- coding: utf-8 -*-

import argparse
import sys
from alert import Alert
from flink_topology import FlinkTopology
from register_dashboard import RegisterDashboard
from topology_builder import TopologyBuilder


COMMANDS = [
    ("start", "start topology", FlinkTopology.populate_lifecycle_parser),
    ("stop", "stop topology", FlinkTopology.populate_lifecycle_parser),
    ("register_alert", "register alert info", Alert.populate_alert_parser),
    ("unregister_alert", "unregister alert info", Alert.populate_alert_parser),
    ("gen", "gen topology", TopologyBuilder.populate_gen_parser),
    ("register_dashboard", "register_dashboard", RegisterDashboard.populate_dashboard_parser)
]


def get_cmd_parser():
    parser = argparse.ArgumentParser()
    subparsers = parser.add_subparsers(title="commands", dest="subparser_command")
    for name, desc, populate_parser in COMMANDS:
        subparser = subparsers.add_parser(name=name, help=desc)
        populate_parser(subparser)
    return parser


def main():
    exit_code = 0
    parser = get_cmd_parser()
    args = parser.parse_args(sys.argv[1:])
    if args.subparser_command == "start" or args.subparser_command == "stop":
        topology = FlinkTopology.from_args(args)
        if args.subparser_command == "start":
            exit_code = 0 if topology.start() else -1
        else:
            exit_code = 0 if topology.stop() else -1
    elif args.subparser_command == "register_alert" or args.subparser_command == "unregister_alert":
        alert = Alert.from_args(args)
        if args.subparser_command == "register_alert":
            alert.register_alert()
        else:
            alert.unregister_alert()
    elif args.subparser_command == "gen":
        tb = TopologyBuilder(args)
        tb.gen()
    elif args.subparser_command == "register_dashboard":
        r = RegisterDashboard.from_args(args)
        r.register_dashboard()
    else:
        pass
    exit(exit_code)


if __name__ == "__main__":
    # example: /opt/tiger/flink_deploy/tools/flink.py start
    #           -y topology_online.yaml -n flink -q root.flink_cluster.online
    main()
