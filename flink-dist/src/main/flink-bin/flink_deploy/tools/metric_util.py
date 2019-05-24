#!/usr/bin/env python
# -*- coding: utf-8 -*-


from http_util import HttpUtil


class MetricUtil(object):
    METRIC_TEMPLATE = "http://metrics.byted.org/proxy/api/query?" \
                      "_region=%s&start=%s&end=%s&m=%s:%s"

    @staticmethod
    def get_metric_url(start, end, agg, metric_name, region=None):
        if not region:
            if 'va_aws' in metric_name:
                region = "va"
            elif 'sg_aliyun' in metric_name:
                region = "alisg"
            elif 'sg_aws' in metric_name:
                region = "sg"
            else:
                region = "cn"
        return MetricUtil.METRIC_TEMPLATE % (region, start, end, agg, metric_name)

    @staticmethod
    def get_metric_data(start, end, agg, metric_name, retry=3, region=None):
        url = MetricUtil.get_metric_url(start, end, agg, metric_name, region=region)
        (succ, resp) = HttpUtil.do_get(url)
        if succ:
            if resp:
                return resp[0]['dps']
            else:
                return None
        return None
