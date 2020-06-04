#!/usr/bin/env python
# -*- coding: utf-8 -*-

import requests
import traceback
from util import red, gray, green


class HttpUtil(object):
    is_simulate_http_timeout = False

    @staticmethod
    def do_post(url, data=None, headers=None, timeout_sec=10, max_retry_times=3, **kwargs):
        retry_times = 0
        while retry_times < max_retry_times:
            try:
                if HttpUtil.is_simulate_http_timeout:
                    raise Exception("http timeout")
                result = requests.post(url, headers=headers, data=data, timeout=timeout_sec, **kwargs)
                if result.status_code != 200:
                    print red("failed to post %s" % url)
                    print red(result.status_code)
                    print red(result.text)
                else:
                    print green("success to post %s" % url)
                    return True, result.json()
            except Exception, e:
                print red("failed to post %s" % url)
                print e.message
                traceback.print_exc()
            retry_times += 1
        return False, {}

    @staticmethod
    def do_get(url, headers=None, timeout_sec=10, max_retry_times=3, **kwargs):
        retry_times = 0
        while retry_times < max_retry_times:
            try:
                if HttpUtil.is_simulate_http_timeout:
                    raise Exception("http timeout")

                result = requests.get(url, headers=headers, timeout=timeout_sec, **kwargs)
                if result.status_code != 200:
                    print red("failed to get %s" % url)
                    print red(result.status_code)
                    print red(result.text)
                else:
                    print green("success to get %s" % url)
                    return True, result.json()
            except Exception, e:
                print red("failed to get %s" % url)
                print e.message
                traceback.print_exc()
            retry_times += 1
        return False, {}
