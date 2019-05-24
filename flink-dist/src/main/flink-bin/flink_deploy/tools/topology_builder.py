#!/usr/bin/env python
# -*- coding:utf-8 -*-

from yaml.constructor import Constructor

import tornado.template as template
import yaml
import os, shutil


class BuildError(Exception):
    pass


class TopologyBuilder(object):
    """
        uses to construct the topology template
    """
    def __init__(self, args):
        self.args = args
        self.yaml_file = ""
        self.test_yaml_file = ""
        self.conf = {}
        self.test_conf = {}
        self.loader = template.Loader(self._get_lib_path("template"))
        self.topology_name, self.spouts, self.bolts, self.batch_bolts = self.parse_topology_args()

    @staticmethod
    def populate_gen_parser(parser):
        parser.add_argument("-n", "--name", action="store", dest="topology_name", required=True,
                            help="set topology name, eg: user_profile_topology")
        parser.add_argument("-s", "--spout", action="store", dest="spouts", required=True,
                            help="set kafka topic name, eg: kafka_topic1, kafka_topic2")
        parser.add_argument("-b", "--bolt", action="store", dest="bolts",
                            help="set calc component, eg: user_action_format_bolt")
        parser.add_argument("-d", "--batch_bolt", action="store", dest="batch_bolts",
                            help="set calc component, eg: user_action_format_bolt")

    def gen_yaml(self):
        self.yaml_file = self._gen_yaml("online")
        self.test_yaml_file = self._gen_yaml("test")

    def _gen_yaml(self, mode):
        assert mode == "online" or mode == "test"

        yaml_name = "topology_%s.yaml" % mode
        topology_name = self.topology_name
        if mode == "test":
            topology_name = self.topology_name + "_test"

        obj = self.loader.load("topology.yaml")
        data = obj.generate(topology_name=topology_name)
        yaml_file = self.get_working_path(yaml_name)

        with open(yaml_file, "w") as out:
            out.write(data)

        if mode == "online":
            self.conf = self.load_one_yaml(yaml_file=yaml_file)
        else:
            self.test_conf = self.load_one_yaml(yaml_file=yaml_file)
        return yaml_file

    def gen_env(self):
        if os.path.exists(self.topology_name):
            raise Exception("%s exists, choose another topology name." % self.topology_name)
        os.mkdir(self.topology_name)
        os.mkdir(self.get_working_path("resources"))
        shutil.copy(self._get_lib_path("template/dot-gitignore"), self.get_working_path(".gitignore"))

    def gen_spouts(self, spouts):
        for spout_name in spouts:
            spout_list = self.conf["spout"]["spout_list"] or []
            spout_confs = {spout["spout_name"]: spout for spout in spout_list}
            if spout_name in spout_confs:
                raise BuildError("you already have this spout in yaml: %s" % (spout_name))

            callee_path = self.get_working_path("resources", "%s_callee.py" % spout_name)
            if os.path.exists(callee_path):
                raise BuildError("you already have this spout callee: %s_callee.py" % (spout_name))

            obj = self.loader.load("callee.py")
            data = obj.generate(bolt_type="common")
            with open(callee_path, "w") as out:
                out.write(data)

            obj = self.loader.load("spout.yaml")
            data = obj.generate(topic_name=spout_name)
            self.insert_yarm_file(self.conf["spout"]["__end_line__"], data, indent=4)
            self.load_yaml()

    def gen_bolts(self, bolts):
        for bolt_name in bolts:
            bolt_list = self.conf["bolt"]["bolt_list"] or []
            bolt_confs = {bolt["bolt_name"]: bolt for bolt in bolt_list}
            if bolt_name in bolt_confs:
                raise BuildError("you already have this bolt in yaml: %s" % (bolt_name))

            callee_path = self.get_working_path("resources", "%s_callee.py" % bolt_name)
            if os.path.exists(callee_path):
                raise BuildError("you already have this bolt callee: %s_callee.py" % (bolt_name))

            obj = self.loader.load("callee.py")
            data = obj.generate(bolt_type="common")
            with open(callee_path, "w") as out:
                out.write(data)

            obj = self.loader.load("bolt.yaml")
            spout_list = self.conf["spout"]["spout_list"] or []
            spouts = [spout["spout_name"] for spout in spout_list]
            data = obj.generate(bolt_name=bolt_name, spouts=spouts, bolt_type="bolt")
            self.insert_yarm_file(self.conf["bolt"]["__end_line__"], data, indent=4)
            self.load_yaml()

    def gen_batch_bolts(self, bolts):
        for bolt_name in bolts:
            bolt_list = self.conf["bolt"]["bolt_list"] or []
            bolt_confs = {bolt["bolt_name"]: bolt for bolt in bolt_list}
            if bolt_name in bolt_confs:
                raise BuildError("you already have this bolt in yaml: %s" % (bolt_name))
            callee_path = self.get_working_path("resources", "%s_callee.py" % bolt_name)
            if os.path.exists(callee_path):
                raise BuildError("you already have this bolt callee: %s_callee.py" % (bolt_name))

            obj = self.loader.load("callee.py")
            data = obj.generate(bolt_type="batch")
            with open(callee_path, "w") as out:
                out.write(data)

            obj = self.loader.load("bolt.yaml")
            spout_list = self.conf["spout"]["spout_list"] or []
            spouts = [spout["spout_name"] for spout in spout_list]
            data = obj.generate(bolt_name=bolt_name, spouts=spouts, bolt_type="batch_bolt")
            self.insert_yarm_file(self.conf["bolt"]["__end_line__"], data, indent=4)
            self.load_yaml()

    def insert_yarm_file(self, line, content, indent=0):
        self.insert_one_yaml_file(self.yaml_file, line, content, indent)
        self.insert_one_yaml_file(self.test_yaml_file, line, content, indent)

    def insert_one_yaml_file(self, yaml_file, line, content, indent=0):
        if not os.path.exists(yaml_file):
            return
        with open(yaml_file, 'r+') as fp:
            buf = fp.readlines()
            while not buf[line-1].strip():
                line -= 1
            lines = content.split('\n')
            if indent:
                lines = [' '*indent + row + '\n' for row in lines if row.strip()]
            fp.seek(0)
            fp.writelines(buf[:line] + lines + buf[line:])

    def _get_lib_path(self, *paths):
        lib_dir = os.path.dirname(__file__)
        return os.path.join(lib_dir, *paths)

    def get_working_path(self, *paths):
        return os.path.join(self.topology_name, *paths)

    def load_yaml(self):
        self.conf = self.load_one_yaml(self.yaml_file)
        self.test_conf = self.load_one_yaml(self.test_yaml_file)

    def load_one_yaml(self, yaml_file):
        if not os.path.exists(yaml_file):
            raise BuildError("can't find %s file in working dir" % yaml_file)

        with open(yaml_file) as fp:
            loader = yaml.Loader(fp.read())

            def construct_mapping(node, deep=False):
                mapping = Constructor.construct_mapping(loader, node, deep=deep)
                mapping["__start_line__"] = node.start_mark.line
                mapping["__end_line__"] = node.end_mark.line
                return mapping

            loader.construct_mapping = construct_mapping
            conf = loader.get_single_data()
            return conf

    def parse_topology_args(self):
        spouts = []
        if self.args.spouts:
            for spout_name in self.args.spouts.split(","):
                spouts.append(spout_name.strip())

        bolts = []
        if self.args.bolts:
            for bolt_name in self.args.bolts.split(","):
                bolts.append(bolt_name.strip())

        batch_bolts = []
        if getattr(self.args, "batch_bolts", ""):
            for bolt_name in self.args.batch_bolts.split(","):
                batch_bolts.append(bolt_name.strip())

        return self.args.topology_name, spouts, bolts, batch_bolts

    def gen(self):
        self.gen_env()
        self.gen_yaml()
        self.gen_spouts(self.spouts)
        self.gen_bolts(self.bolts)
        self.gen_batch_bolts(self.batch_bolts)
