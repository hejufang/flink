#!/usr/bin/env python
# -*- coding: utf-8 -*-

import logging
import requests
import traceback


def red(msg):
    return "\x1B[1;31;40m%s\x1B[0m" % msg


def green(msg):
    return "\x1B[0;32;40m%s\x1B[0m" % msg


def gray(msg):
    return msg


def out_and_info(msg):
    print green(msg)
    logging.info(msg)
