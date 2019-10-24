#!/usr/bin/env python
# -*- coding:utf-8 -*-

from kafka_util import KafkaUtil
from string import Template


class DashboardTemplate(object):
    """ Generates the grafana dashboard template """

    def __init__(self):
        self.dashboard_template = Template('''
            {
                "dashboard": {
                    "annotations": {
                        "list": []
                    },
                    "editable": true,
                    "gnetId": null,
                    "graphTooltip": 0,
                    "hideControls": false,
                    "id": null,
                    "links": [],
                    "rows": [
                        {
                            "collapse": false,
                            "height": "250px",
                            "panels": [
                              {
                                "aliasColors": {},
                                "bars": false,
                                "datasource":"${datasource}",
                                "fill": 1,
                                "id": 11,
                                "legend": {
                                  "alignAsTable": true,
                                  "avg": true,
                                  "current": true,
                                  "max": false,
                                  "min": false,
                                  "rightSide": true,
                                  "show": true,
                                  "total": false,
                                  "values": true
                                },
                                "lines": true,
                                "linewidth": 1,
                                "links": [],
                                "nullPointMode": "null",
                                "percentage": false,
                                "pointradius": 5,
                                "points": false,
                                "renderer": "flot",
                                "seriesOverrides": [],
                                "span": 12,
                                "stack": false,
                                "steppedLine": false,
                                "targets": ${lagsize_targets},
                                "thresholds": [],
                                "timeFrom": null,
                                "timeShift": null,
                                "title": "LagSize",
                                "tooltip": {
                                  "shared": true,
                                  "sort": 0,
                                  "value_type": "individual"
                                },
                                "type": "graph",
                                "xaxis": {
                                  "mode": "time",
                                  "name": null,
                                  "show": true,
                                  "values": []
                                },
                                "yaxes": [
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  },
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  }
                                ]
                              }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                              {
                                "aliasColors": {},
                                "bars": false,
                                "datasource": "${datasource}",
                                "fill": 1,
                                "id": 12,
                                "legend": {
                                  "alignAsTable": true,
                                  "avg": true,
                                  "current": true,
                                  "max": false,
                                  "min": false,
                                  "rightSide": true,
                                  "show": true,
                                  "total": false,
                                  "values": true
                                },
                                "lines": true,
                                "linewidth": 1,
                                "links": [],
                                "nullPointMode": "null",
                                "percentage": false,
                                "pointradius": 5,
                                "points": false,
                                "renderer": "flot",
                                "seriesOverrides": [],
                                "span": 12,
                                "stack": false,
                                "steppedLine": false,
                                "targets": ${throughput_targets},
                                "thresholds": [],
                                "timeFrom": null,
                                "timeShift": null,
                                "title": "Throughput",
                                "tooltip": {
                                  "shared": true,
                                  "sort": 0,
                                  "value_type": "individual"
                                },
                                "type": "graph",
                                "xaxis": {
                                  "mode": "time",
                                  "name": null,
                                  "show": true,
                                  "values": []
                                },
                                "yaxes": [
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  },
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  }
                                ]
                              }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                          },
                          {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                              {
                                "aliasColors": {},
                                "bars": false,
                                "datasource": "${datasource}",
                                "fill": 1,
                                "id": 15,
                                "legend": {
                                  "alignAsTable": true,
                                  "avg": true,
                                  "current": true,
                                  "max": false,
                                  "min": false,
                                  "rightSide": true,
                                  "show": true,
                                  "total": false,
                                  "values": true
                                },
                                "lines": true,
                                "linewidth": 1,
                                "links": [],
                                "nullPointMode": "null",
                                "percentage": false,
                                "pointradius": 5,
                                "points": false,
                                "renderer": "flot",
                                "seriesOverrides": [],
                                "span": 12,
                                "stack": false,
                                "steppedLine": false,
                                "targets": ${latency_targets},
                                "thresholds": [],
                                "timeFrom": null,
                                "timeShift": null,
                                "title": "Spout Latency",
                                "tooltip": {
                                  "shared": true,
                                  "sort": 0,
                                  "value_type": "individual"
                                },
                                "type": "graph",
                                "xaxis": {
                                  "mode": "time",
                                  "name": null,
                                  "show": true,
                                  "values": []
                                },
                                "yaxes": [
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  },
                                  {
                                    "format": "short",
                                    "label": null,
                                    "logBase": 1,
                                    "max": null,
                                    "min": null,
                                    "show": true
                                  }
                                ]
                              }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                         {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 16,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${spout_qps_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Spout Qps",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 17,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${bolt_qps_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Bolt Qps",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 18,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${bolt_latency_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Bolt Latency",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 19,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${batch_bolt_key_size_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Batch Bolt Key Size",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": "250px",
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 1,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${jobinfo_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Job Info",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 2,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${slots_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "TaskManager/Slots",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 3,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${cpu_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "CPU",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 4,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${memory_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Memory",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 5,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "sort": null,
                                        "sortDesc": null,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${gc_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "GC",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 6,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${threads_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Thread Counts",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 7,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${checkpoint_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Check Point",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 8,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${network_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Network Memory",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 9,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${task_queue_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Task In/Out Queue",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        },
                        {
                            "collapse": false,
                            "height": 250,
                            "panels": [
                                {
                                    "aliasColors": {},
                                    "bars": false,
                                    "dashLength": 10,
                                    "dashes": false,
                                    "datasource": "${datasource}",
                                    "fill": 1,
                                    "id": 10,
                                    "legend": {
                                        "alignAsTable": true,
                                        "avg": true,
                                        "current": true,
                                        "max": true,
                                        "min": false,
                                        "rightSide": true,
                                        "show": true,
                                        "total": false,
                                        "values": true
                                    },
                                    "lines": true,
                                    "linewidth": 1,
                                    "links": [],
                                    "nullPointMode": "null",
                                    "percentage": false,
                                    "pointradius": 5,
                                    "points": false,
                                    "renderer": "flot",
                                    "seriesOverrides": [],
                                    "spaceLength": 10,
                                    "span": 12,
                                    "stack": false,
                                    "steppedLine": false,
                                    "targets": ${task_record_targets},
                                    "thresholds": [],
                                    "timeFrom": null,
                                    "timeShift": null,
                                    "title": "Task In/Out Records",
                                    "tooltip": {
                                        "shared": true,
                                        "sort": 0,
                                        "value_type": "individual"
                                    },
                                    "type": "graph",
                                    "xaxis": {
                                        "buckets": null,
                                        "mode": "time",
                                        "name": null,
                                        "show": true,
                                        "values": []
                                    },
                                    "yaxes": [
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        },
                                        {
                                            "format": "short",
                                            "label": null,
                                            "logBase": 1,
                                            "max": null,
                                            "min": null,
                                            "show": true
                                        }
                                    ]
                                }
                            ],
                            "repeat": null,
                            "repeatIteration": null,
                            "repeatRowId": null,
                            "showTitle": false,
                            "title": "Dashboard Row",
                            "titleSize": "h6"
                        }
                    ],
                    "schemaVersion": 14,
                    "style": "dark",
                    "tags": [],
                    "templating": {
                        "list": []
                    },
                    "time": {
                        "from": "now-1h",
                        "to": "now"
                    },
                    "timepicker": {
                        "refresh_intervals": [
                            "5s",
                            "10s",
                            "30s",
                            "1m",
                            "5m",
                            "15m",
                            "30m",
                            "1h",
                            "2h",
                            "1d"
                        ],
                        "time_options": [
                            "5m",
                            "15m",
                            "1h",
                            "6h",
                            "12h",
                            "24h",
                            "2d",
                            "7d",
                            "30d"
                        ]
                    },
                    "timezone": "",
                    "title": "flink-${cluster_name}-${jobname}",
                    "version": 14
                },
                "overwrite": true
            }
        ''')

        self.batch_bolt_key_size_template = Template('''
            {
                "aggregator": "avg",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}.${batch_bolt_name}.key_batch_size.avg"
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}.${batch_bolt_name}.key_batch_size.max"
            },
            {
                "aggregator": "min",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}.${batch_bolt_name}.key_batch_size.min"
            }
        ''')
        self.bolt_qps_template = Template('''
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.throughput",
                "alias": "${metrics_namespace_prefix}._${bolt_name}.throughput.max",
                "shouldComputeRate": true
            },
            {
                "aggregator": "avg",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.throughput",
                "alias": "${metrics_namespace_prefix}._${bolt_name}.throughput.avg",
                "shouldComputeRate": true
            },
            {
                "aggregator": "sum",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.emit",
                "shouldComputeRate": true
            }
        ''')
        self.bolt_latency_template = Template('''
            {
                "aggregator": "avg",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.latency.avg"
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.latency.avg",
                "alias": "${metrics_namespace_prefix}._${bolt_name}.latency.avg.max"
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${bolt_name}.latency.pct90"
            }
        ''')
        self.spout_qps_template = Template('''
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${spout_name}.emit",
                "shouldComputeRate": true,
                "tags": ${tags}
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "${metrics_namespace_prefix}._${spout_name}.throughput",
                "shouldComputeRate": true,
                "tags": ${tags}
            }
        ''')
        self.jobinfo_template = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "downsampleInterval": "",
                    "metric": "flink.jobmanager.${jobname}.downtime",
                    "refId": "A",
                    "tags": {}
                },
                {
                    "aggregator": "max",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.fullRestarts",
                    "refId": "B"
                },
                {
                    "aggregator": "max",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.restartingTime",
                    "refId": "C"
                }
            ]
        ''')
        self.slots_template = Template('''
            [
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.numRegisteredTaskManagers",
                    "refId": "A",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "downsampleInterval": "",
                    "metric": "flink.jobmanager.taskSlotsTotal",
                    "refId": "B",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.taskSlotsAvailable",
                    "refId": "C",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.cpu_template = Template('''
            [
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.CPU.Load",
                    "refId": "A",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.CPU.Load",
                    "refId": "C",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.memory_template = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "downsampleInterval": "",
                    "metric": "flink.jobmanager.Status.JVM.Memory.Heap.Max",
                    "refId": "A",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.Memory.Heap.Used",
                    "refId": "B",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.Memory.Heap.Max",
                    "refId": "C",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.Memory.Heap.Used",
                    "refId": "D",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.gc_template_1_3_2 = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.PSMarkSweep.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.PSMarkSweep.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.PSScavenge.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.PSScavenge.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.gc_template_1_5 = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.PS_MarkSweep.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.PS_MarkSweep.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.PS_Scavenge.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.PS_Scavenge.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.gc_template_1_9 = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.G1_Old_Generation.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.GarbageCollector.G1_Old_Generation.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.G1_Young_Generation.Count",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.GarbageCollector.G1_Young_Generation.Time",
                    "shouldComputeRate": true,
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.threads_template = Template('''
            [
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.Status.JVM.Threads.Count",
                    "refId": "A",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "max",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.JVM.Threads.Count",
                    "refId": "B",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.checkpoint_template = Template('''
            [
                {
                    "aggregator": "sum",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.lastCheckpointDuration",
                    "refId": "A"
                },
                {
                    "aggregator": "sum",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.lastCheckpointSize",
                    "refId": "B"
                },
                {
                    "aggregator": "sum",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.numberOfCompletedCheckpoints",
                    "refId": "C"
                },
                {
                    "aggregator": "sum",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.jobmanager.${jobname}.numberOfFailedCheckpoints",
                    "refId": "D"
                }
            ]
        ''')
        self.network_template = Template('''
            [
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.Network.TotalMemorySegments",
                    "refId": "A",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                },
                {
                    "aggregator": "sum",
                    "currentTagKey": "",
                    "currentTagValue": "",
                    "downsampleAggregator": "avg",
                    "downsampleFillPolicy": "none",
                    "metric": "flink.taskmanager.Status.Network.AvailableMemorySegments",
                    "refId": "B",
                    "tags": {
                        "jobname": "${jobname}"
                    }
                }
            ]
        ''')
        self.task_queue_template = Template('''
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "flink.taskmanager.${jobname}.${component}.buffers.inPoolUsage"
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "flink.taskmanager.${jobname}.${component}.buffers.outPoolUsage"
            }''')
        self.task_record_template = Template('''
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "flink.taskmanager.${jobname}.${component}.numRecordsInPerSecond.rate"
            },
            {
                "aggregator": "max",
                "downsampleAggregator": "avg",
                "downsampleFillPolicy": "none",
                "metric": "flink.taskmanager.${jobname}.${component}.numRecordsOutPerSecond.rate"
            }''')
        self.row_lagsize_template = Template('''
                    {
                      "aggregator": "max",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "metric": "${topic_related_metric_prefix}.${topic}.${consumer_group}.lag.size"
                    }
                ''')
        self.row_latency_template = Template('''
                    {
                      "aggregator": "max",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "metric": "${metrics_namespace_prefix}._${spout}.next.kafka.latency.avg",
                      "alias": "${metrics_namespace_prefix}._${spout}.next.kafka.latency.avg.max",
                      "tags": ${tags}
                    },
                    {
                      "aggregator": "avg",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "metric": "${metrics_namespace_prefix}._${spout}.next.kafka.latency.avg",
                      "alias": "${metrics_namespace_prefix}._${spout}.next.kafka.latency.avg.avg",
                      "tags": ${tags}
                    },
                    {
                      "aggregator": "max",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "metric": "${metrics_namespace_prefix}._${spout}.next.latency.avg",
                      "alias": "${metrics_namespace_prefix}._${spout}.next.latency.avg.max",
                      "tags": ${tags}
                    },
                    {
                      "aggregator": "avg",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "metric": "${metrics_namespace_prefix}._${spout}.next.latency.avg",
                      "alias": "${metrics_namespace_prefix}._${spout}.next.latency.avg.avg",
                      "tags": ${tags}
                    }
                ''')
        self.row_throughput_template = Template('''
                    {
                      "aggregator": "sum",
                      "currentTagKey": "",
                      "currentTagValue": "",
                      "downsampleAggregator": "avg",
                      "downsampleFillPolicy": "none",
                      "isCounter": true,
                      "metric": "${broker_related_metric_prefix}.server.io.MessagesInPerSec.Count",
                      "refId": "B",
                      "shouldComputeRate": true,
                      "tags": {
                        "topic": "${topic}"
                      }
                    }
                ''')

    def render_spout_qps_template(self, metrics_namespace_prefix, kafka_spouts):
        spout_qps_list = []
        for kafka_spout_info in kafka_spouts:
            topic, kafka_cluster, consumer_group, spout_name, is_multi_spout = kafka_spout_info
            spout_qps_list.append(self.spout_qps_template.substitute({
                "metrics_namespace_prefix": metrics_namespace_prefix,
                "spout_name": spout_name,
                "tags": '{"topic": "%s"}' % topic if is_multi_spout else "{}"
            }))
        spout_qps_str = "[" + ",".join(spout_qps_list) + "]"
        return spout_qps_str

    def render_bolt_qps_template(self, metrics_namespace_prefix, bolts):
        bolt_qps_list = []
        for bolt_name in bolts:
            bolt_qps_list.append(self.bolt_qps_template.substitute({
                "metrics_namespace_prefix": metrics_namespace_prefix,
                "bolt_name": bolt_name
            }))
        bolt_qps_str = "[" + ",".join(bolt_qps_list) + "]"
        return bolt_qps_str

    def render_bolt_latency_template(self, metrics_namespace_prefix, bolts):
        bolt_latency_list = []
        for bolt_name in bolts:
            bolt_latency_list.append(self.bolt_latency_template.substitute({
                "metrics_namespace_prefix": metrics_namespace_prefix,
                "bolt_name": bolt_name
            }))
        bolt_latency__str = "[" + ",".join(bolt_latency_list) + "]"
        return bolt_latency__str

    def render_batch_bolt_key_size_template(self, metrics_namespace_prefix,
                                            batch_bolts):
        batch_bolt_key_size_list = []
        for batch_bolt_name in batch_bolts:
            batch_bolt_key_size_list.append(
                self.batch_bolt_key_size_template.substitute({
                    "metrics_namespace_prefix": metrics_namespace_prefix,
                    "batch_bolt_name": batch_bolt_name
                }))
        batch_bolt_key_batch_str = "[" + ",".join(
            batch_bolt_key_size_list) + "]"
        return batch_bolt_key_batch_str

    def render_dashboard_template(self, topology_name, cluster_name, spouts,
                                  bolts, batch_bolts,
                                  kafka_spouts, data_source,
                                  metrics_namespace_prefix,
                                  flink_version, kafka_server_url):
        spout_qps_targets = self.render_spout_qps_template(
            metrics_namespace_prefix, kafka_spouts)

        bolt_qps_targets = self.render_bolt_qps_template(
            metrics_namespace_prefix, bolts)

        bolt_latency_targets = self.render_bolt_latency_template(
            metrics_namespace_prefix, bolts)

        batch_bolt_key_size_targets = self.render_batch_bolt_key_size_template(
            metrics_namespace_prefix, batch_bolts)

        latency_targets = self.render_latency(kafka_spouts,
                                              metrics_namespace_prefix)

        jobinfo_targets = self.jobinfo_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        slots_targets = self.slots_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        cpu_targets = self.cpu_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        memory_targets = self.memory_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        if flink_version == '1.5':
            gc_targets = self.gc_template_1_5.substitute({
                "datasource": data_source,
                "jobname": topology_name
            })
        elif flink_version == '1.9':
            gc_targets = self.gc_template_1_9.substitute({
                "datasource": data_source,
                "jobname": topology_name
            })
        else:
            gc_targets = self.gc_template_1_3_2.substitute({
                "datasource": data_source,
                "jobname": topology_name
            })

        threads_target = self.threads_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        checkpoint_targets = self.checkpoint_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        network_targets = self.network_template.substitute({
            "datasource": data_source,
            "jobname": topology_name
        })

        components = []
        for spout in spouts:
            components.append("Source_" + spout)
        components.extend(bolts)

        task_queue_targets = self.render_task_queue_template(topology_name,
                                                             components)

        task_record_targets = self.render_task_record_template(topology_name,
                                                               components)

        lagsize_targets = self.render_lagsize(kafka_spouts, kafka_server_url)

        throughput_targets = self.render_throughput(kafka_spouts, kafka_server_url)

        dashboard = self.dashboard_template.substitute({
            "jobinfo_targets": jobinfo_targets,
            "slots_targets": slots_targets,
            "cpu_targets": cpu_targets,
            "memory_targets": memory_targets,
            "gc_targets": gc_targets,
            "threads_targets": threads_target,
            "checkpoint_targets": checkpoint_targets,
            "network_targets": network_targets,
            "task_queue_targets": task_queue_targets,
            "task_record_targets": task_record_targets,
            "jobname": topology_name,
            "datasource": data_source,
            'cluster_name': cluster_name,
            "lagsize_targets": lagsize_targets,
            "latency_targets": latency_targets,
            "throughput_targets": throughput_targets,
            "spout_qps_targets": spout_qps_targets,
            "bolt_qps_targets": bolt_qps_targets,
            "bolt_latency_targets": bolt_latency_targets,
            "batch_bolt_key_size_targets": batch_bolt_key_size_targets
        })

        return dashboard

    def render_lagsize(self, kafka_spouts, kafka_server_url):
        lagsize_list = []
        for kafka_spout_info in kafka_spouts:
            topic, kafka_cluster, consumer_group, spout_name, is_multi_spout = kafka_spout_info
            lagsize_list.append(self.row_lagsize_template.substitute({
                "topic": topic,
                "topic_related_metric_prefix": KafkaUtil.get_kafka_topic_prefix(
                    kafka_cluster, kafka_server_url),
                "consumer_group": consumer_group
            }))
        lagsize_str = "[" + ",".join(lagsize_list) + "]"
        return lagsize_str

    def render_latency(self, kafka_spouts, metrics_namespace_prefix):
        latency_list = []
        for kafka_spout_info in kafka_spouts:
            topic, kafka_cluster, consumer_group, spout_name, is_multi_spout = kafka_spout_info
            latency_list.append(self.row_latency_template.substitute({
                "metrics_namespace_prefix": metrics_namespace_prefix,
                "spout": spout_name,
                "tags": '{"topic":"%s"}' % topic if is_multi_spout else "{}"
            }))
        latency_str = "[" + ",".join(latency_list) + "]"
        return latency_str

    def render_throughput(self, kafka_spouts, kafka_server_url):
        throughput_list = []
        for kafka_spout_info in kafka_spouts:
            topic, kafka_cluster, consumer_group, spout_name, is_multi_kafka = kafka_spout_info
            throughput_list.append(self.row_throughput_template.substitute({
                "broker_related_metric_prefix": KafkaUtil.get_kafka_server_prefix(
                    kafka_cluster, kafka_server_url),
                "topic": topic
            }))
        throughput_str = "[" + ",".join(throughput_list) + "]"
        return throughput_str

    def render_task_queue_template(self, topology_name, components):
        task_queue_list = []
        for comp in components:
            task_queue_list.append(self.task_queue_template.substitute({
                "jobname": topology_name,
                "component": comp
            }))

        task_queue_str = "[" + ",".join(task_queue_list) + "]"
        return task_queue_str

    def render_task_record_template(self, topology_name, components):
        task_record_list = []
        for comp in components:
            task_record_list.append(self.task_record_template.substitute({
                "jobname": topology_name,
                "component": comp
            }))
        task_record_str = "[" + ",".join(task_record_list) + "]"
        return task_record_str
