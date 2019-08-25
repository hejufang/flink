/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.monitor;

/**
 * Template for registering grafana dashboard.
 * */
public class Template {
	public static final String TEMPLATE = "{\n" +
			"	\"dashboard\": {\n" +
			"	\"annotations\": {\n" +
			"		\"list\": [\n" +
			"			{\n" +
			"				\"builtIn\": 1,\n" +
			"				\"datasource\": \"-- Grafana --\",\n" +
			"				\"enable\": true,\n" +
			"				\"hide\": true,\n" +
			"				\"iconColor\": \"rgba(0, 211, 255, 1)\",\n" +
			"				\"name\": \"Annotations & Alerts\",\n" +
			"				\"type\": \"dashboard\"\n" +
			"			}\n" +
			"		]\n" +
			"	},\n" +
			"	\"editable\": true,\n" +
			"	\"gnetId\": null,\n" +
			"	\"graphTooltip\": 0,\n" +
			"	\"hideControls\": false,\n" +
			"	\"id\": null,\n" +
			"	\"links\": [],\n" +
			"	\"rows\": [\n" + "${rows} \n]," +
			"	\"schemaVersion\": 14,\n" +
			"	\"style\": \"dark\",\n" +
			"	\"tags\": [],\n" +
			"	\"templating\": {\n" +
			"		\"list\": []\n" +
			"	},\n" +
			"	\"time\": {\n" +
			"		\"from\": \"now-6h\",\n" +
			"		\"to\": \"now\"\n" +
			"	},\n" +
			"	\"timepicker\": {\n" +
			"		\"refresh_intervals\": [\n" +
			"			\"5s\",\n" +
			"			\"10s\",\n" +
			"			\"30s\",\n" +
			"			\"1m\",\n" +
			"			\"5m\",\n" +
			"			\"15m\",\n" +
			"			\"30m\",\n" +
			"			\"1h\",\n" +
			"			\"2h\",\n" +
			"			\"1d\"\n" +
			"		],\n" +
			"		\"time_options\": [\n" +
			"			\"5m\",\n" +
			"			\"15m\",\n" +
			"			\"1h\",\n" +
			"			\"6h\",\n" +
			"			\"12h\",\n" +
			"			\"24h\",\n" +
			"			\"2d\",\n" +
			"			\"7d\",\n" +
			"			\"30d\"\n" +
			"		]\n" +
			"	},\n" +
			"	\"timezone\": \"\",\n" +
			"	\"title\": \"flink.${cluster}.${jobname}\",\n" +
			"	\"version\": 5\n" +
			"},\n" +
			"	\"overwrite\": true\n" +
			"}";

	public static final String JOB_INFO = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": \"250px\",\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 1,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.${jobname}.downtime\",\n" +
			"							\"refId\": \"A\"\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.${jobname}.fullRestarts\",\n" +
			"							\"refId\": \"B\"\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.${jobname}.restartingTime\",\n" +
			"							\"refId\": \"C\"\n" +
			"						}\n" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Job Info\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String LAG_SIZE = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 2,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" + "${targets} \n" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Lag Size\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"}";

	public static final String LAG_SIZE_TARGET = "{\n" +
			"							\"aggregator\": \"sum\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"${lag}\",\n" +
			"							\"refId\": \"A\"\n" +
			"						}\n";

	public static final String TM_SLOT = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 3,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" +
			"						{\n" +
			"							\"aggregator\": \"sum\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.numRegisteredTaskManagers\",\n" +
			"							\"refId\": \"A\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"sum\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"downsampleInterval\": \"\",\n" +
			"							\"metric\": \"flink.jobmanager.taskSlotsTotal\",\n" +
			"							\"refId\": \"B\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"sum\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.taskSlotsAvailable\",\n" +
			"							\"refId\": \"C\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						}\n" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"TaskManager/Slot\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"" +
			"}";

	public static final String MEMORY = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 4,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"downsampleInterval\": \"\",\n" +
			"							\"metric\": \"flink.jobmanager.Status.JVM.Memory.Heap.Max\",\n" +
			"							\"refId\": \"A\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.Status.JVM.Memory.Heap.Used\",\n" +
			"							\"refId\": \"B\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.Status.JVM.Memory.Heap.Max\",\n" +
			"							\"refId\": \"C\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.Status.JVM.Memory.Heap.Used\",\n" +
			"							\"refId\": \"D\",\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						}\n" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Memory\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"bytes\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String GC = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 5,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"sort\": null,\n" +
			"						\"sortDesc\": null,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.Status.JVM.GarbageCollector.PS_MarkSweep.Count\",\n" +
			"							\"refId\": \"A\",\n" +
			"							\"shouldComputeRate\": true,\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.jobmanager.Status.JVM.GarbageCollector.PS_MarkSweep.Time\",\n" +
			"							\"refId\": \"B\",\n" +
			"							\"shouldComputeRate\": true,\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.Status.JVM.GarbageCollector.PS_Scavenge.Count\",\n" +
			"							\"refId\": \"C\",\n" +
			"							\"shouldComputeRate\": true,\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"currentTagKey\": \"\",\n" +
			"							\"currentTagValue\": \"\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.Status.JVM.GarbageCollector.PS_Scavenge.Time\",\n" +
			"							\"refId\": \"D\",\n" +
			"							\"shouldComputeRate\": true,\n" +
			"							\"tags\": {\n" +
			"								\"jobname\": \"${jobname}\"\n" +
			"							}\n" +
			"						}\n" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"GC\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String QUEUE_LENGTH = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 6,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" + "${targets}" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Task In/Out Queue\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String QUEUE_LENGTH_TARGET = "{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${operator}.buffers.inputQueueLength\",\n" +
			"							\"refId\": \"I\"\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${operator}.buffers.outputQueueLength\",\n" +
			"							\"refId\": \"N\"\n" +
			"						}";

	public static final String POOL_USAGE = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 7,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" + "${targets}" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Task In/Out Pool Usage\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String POOL_USAGE_TARGET = "{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${operator}.buffers.inPoolUsage\",\n" +
			"							\"refId\": \"I\"\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${operator}.buffers.outPoolUsage\",\n" +
			"							\"refId\": \"N\"\n" +
			"						}";

	public static final String RECORD_NUM = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 8,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" + "${targets}" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"In/Out Record Number\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String RECORD_NUM_TARGET = "{\n" +
		"							\"aggregator\": \"max\",\n" +
		"							\"downsampleAggregator\": \"avg\",\n" +
		"							\"downsampleFillPolicy\": \"none\",\n" +
		"							\"metric\": \"flink.taskmanager.${jobname}.${operator}." +
		"numRecordsInPerSecond.rate\",\n" +
		"							\"refId\": \"K\"\n" +
		"						},\n" +
		"						{\n" +
		"							\"aggregator\": \"max\",\n" +
		"							\"downsampleAggregator\": \"avg\",\n" +
		"							\"downsampleFillPolicy\": \"none\",\n" +
		"							\"metric\": \"flink.taskmanager.${jobname}.${operator}." +
		"numRecordsOutPerSecond.rate\",\n" +
		"							\"refId\": \"N\"\n" +
		"						}";

	public static final String OPERATOR_LATENCY = "{\n" +
		"			\"collapse\": false,\n" +
		"			\"height\": 250,\n" +
		"			\"panels\": [\n" +
		"				{\n" +
		"					\"aliasColors\": {},\n" +
		"					\"bars\": false,\n" +
		"					\"dashLength\": 10,\n" +
		"					\"dashes\": false,\n" +
		"					\"datasource\": \"${datasource}\",\n" +
		"					\"fill\": 1,\n" +
		"					\"id\": 10,\n" +
		"					\"legend\": {\n" +
		"						\"alignAsTable\": true,\n" +
		"						\"avg\": true,\n" +
		"						\"current\": true,\n" +
		"						\"max\": true,\n" +
		"						\"min\": false,\n" +
		"						\"rightSide\": true,\n" +
		"						\"show\": true,\n" +
		"						\"total\": false,\n" +
		"						\"values\": true\n" +
		"					},\n" +
		"					\"lines\": true,\n" +
		"					\"linewidth\": 1,\n" +
		"					\"links\": [],\n" +
		"					\"nullPointMode\": \"null\",\n" +
		"					\"percentage\": false,\n" +
		"					\"pointradius\": 5,\n" +
		"					\"points\": false,\n" +
		"					\"renderer\": \"flot\",\n" +
		"					\"seriesOverrides\": [],\n" +
		"					\"spaceLength\": 10,\n" +
		"					\"span\": 12,\n" +
		"					\"stack\": false,\n" +
		"					\"steppedLine\": false,\n" +
		"					\"targets\": [\n" + "${targets}" +
		"					],\n" +
		"					\"thresholds\": [],\n" +
		"					\"timeFrom\": null,\n" +
		"					\"timeShift\": null,\n" +
		"					\"title\": \"Operator Latency\",\n" +
		"					\"tooltip\": {\n" +
		"						\"shared\": true,\n" +
		"						\"sort\": 0,\n" +
		"						\"value_type\": \"individual\"\n" +
		"					},\n" +
		"					\"type\": \"graph\",\n" +
		"					\"xaxis\": {\n" +
		"						\"buckets\": null,\n" +
		"						\"mode\": \"time\",\n" +
		"						\"name\": null,\n" +
		"						\"show\": true,\n" +
		"						\"values\": []\n" +
		"					},\n" +
		"					\"yaxes\": [\n" +
		"						{\n" +
		"							\"format\": \"ms\",\n" +
		"							\"label\": null,\n" +
		"							\"logBase\": 1,\n" +
		"							\"max\": null,\n" +
		"							\"min\": null,\n" +
		"							\"show\": true\n" +
		"						},\n" +
		"						{\n" +
		"							\"format\": \"short\",\n" +
		"							\"label\": null,\n" +
		"							\"logBase\": 1,\n" +
		"							\"max\": null,\n" +
		"							\"min\": null,\n" +
		"							\"show\": true\n" +
		"						}\n" +
		"					]\n" +
		"				}\n" +
		"			],\n" +
		"			\"repeat\": null,\n" +
		"			\"repeatIteration\": null,\n" +
		"			\"repeatRowId\": null,\n" +
		"			\"showTitle\": false,\n" +
		"			\"title\": \"Dashboard Row\",\n" +
		"			\"titleSize\": \"h6\"\n" +
		"		}";

	public static final String OPERATOR_LATENCY_TARGET = "{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${operator}." +
			"latency.mean\",\n" +
			"							\"refId\": \"K\"\n" +
			"						}";

	public static final String KAFKA_OFFSET = "{\n" +
			"			\"collapse\": false,\n" +
			"			\"height\": 250,\n" +
			"			\"panels\": [\n" +
			"				{\n" +
			"					\"aliasColors\": {},\n" +
			"					\"bars\": false,\n" +
			"					\"dashLength\": 10,\n" +
			"					\"dashes\": false,\n" +
			"					\"datasource\": \"${datasource}\",\n" +
			"					\"fill\": 1,\n" +
			"					\"id\": 9,\n" +
			"					\"legend\": {\n" +
			"						\"alignAsTable\": true,\n" +
			"						\"avg\": true,\n" +
			"						\"current\": true,\n" +
			"						\"max\": true,\n" +
			"						\"min\": false,\n" +
			"						\"rightSide\": true,\n" +
			"						\"show\": true,\n" +
			"						\"total\": false,\n" +
			"						\"values\": true\n" +
			"					},\n" +
			"					\"lines\": true,\n" +
			"					\"linewidth\": 1,\n" +
			"					\"links\": [],\n" +
			"					\"nullPointMode\": \"null\",\n" +
			"					\"percentage\": false,\n" +
			"					\"pointradius\": 5,\n" +
			"					\"points\": false,\n" +
			"					\"renderer\": \"flot\",\n" +
			"					\"seriesOverrides\": [],\n" +
			"					\"spaceLength\": 10,\n" +
			"					\"span\": 12,\n" +
			"					\"stack\": false,\n" +
			"					\"steppedLine\": false,\n" +
			"					\"targets\": [\n" + "${targets}" +
			"					],\n" +
			"					\"thresholds\": [],\n" +
			"					\"timeFrom\": null,\n" +
			"					\"timeShift\": null,\n" +
			"					\"title\": \"Kafka Offset\",\n" +
			"					\"tooltip\": {\n" +
			"						\"shared\": true,\n" +
			"						\"sort\": 0,\n" +
			"						\"value_type\": \"individual\"\n" +
			"					},\n" +
			"					\"type\": \"graph\",\n" +
			"					\"xaxis\": {\n" +
			"						\"buckets\": null,\n" +
			"						\"mode\": \"time\",\n" +
			"						\"name\": null,\n" +
			"						\"show\": true,\n" +
			"						\"values\": []\n" +
			"					},\n" +
			"					\"yaxes\": [\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"format\": \"short\",\n" +
			"							\"label\": null,\n" +
			"							\"logBase\": 1,\n" +
			"							\"max\": null,\n" +
			"							\"min\": null,\n" +
			"							\"show\": true\n" +
			"						}\n" +
			"					]\n" +
			"				}\n" +
			"			],\n" +
			"			\"repeat\": null,\n" +
			"			\"repeatIteration\": null,\n" +
			"			\"repeatRowId\": null,\n" +
			"			\"showTitle\": false,\n" +
			"			\"title\": \"Dashboard Row\",\n" +
			"			\"titleSize\": \"h6\"\n" +
			"		}";

	public static final String KAFKA_LATENCY = "{\n" +
		"			\"collapse\": false,\n" +
		"			\"height\": 250,\n" +
		"			\"panels\": [\n" +
		"				{\n" +
		"					\"aliasColors\": {},\n" +
		"					\"bars\": false,\n" +
		"					\"dashLength\": 10,\n" +
		"					\"dashes\": false,\n" +
		"					\"datasource\": \"${datasource}\",\n" +
		"					\"fill\": 1,\n" +
		"					\"id\": 11,\n" +
		"					\"legend\": {\n" +
		"						\"alignAsTable\": true,\n" +
		"						\"avg\": true,\n" +
		"						\"current\": true,\n" +
		"						\"max\": true,\n" +
		"						\"min\": false,\n" +
		"						\"rightSide\": true,\n" +
		"						\"show\": true,\n" +
		"						\"total\": false,\n" +
		"						\"values\": true\n" +
		"					},\n" +
		"					\"lines\": true,\n" +
		"					\"linewidth\": 1,\n" +
		"					\"links\": [],\n" +
		"					\"nullPointMode\": \"null\",\n" +
		"					\"percentage\": false,\n" +
		"					\"pointradius\": 5,\n" +
		"					\"points\": false,\n" +
		"					\"renderer\": \"flot\",\n" +
		"					\"seriesOverrides\": [],\n" +
		"					\"spaceLength\": 10,\n" +
		"					\"span\": 12,\n" +
		"					\"stack\": false,\n" +
		"					\"steppedLine\": false,\n" +
		"					\"targets\": [\n" + "${targets}" +
		"					],\n" +
		"					\"thresholds\": [],\n" +
		"					\"timeFrom\": null,\n" +
		"					\"timeShift\": null,\n" +
		"					\"title\": \"Kafka Latency\",\n" +
		"					\"tooltip\": {\n" +
		"						\"shared\": true,\n" +
		"						\"sort\": 0,\n" +
		"						\"value_type\": \"individual\"\n" +
		"					},\n" +
		"					\"type\": \"graph\",\n" +
		"					\"xaxis\": {\n" +
		"						\"buckets\": null,\n" +
		"						\"mode\": \"time\",\n" +
		"						\"name\": null,\n" +
		"						\"show\": true,\n" +
		"						\"values\": []\n" +
		"					},\n" +
		"					\"yaxes\": [\n" +
		"						{\n" +
		"							\"format\": \"ms\",\n" +
		"							\"label\": null,\n" +
		"							\"logBase\": 1,\n" +
		"							\"max\": null,\n" +
		"							\"min\": null,\n" +
		"							\"show\": true\n" +
		"						},\n" +
		"						{\n" +
		"							\"format\": \"short\",\n" +
		"							\"label\": null,\n" +
		"							\"logBase\": 1,\n" +
		"							\"max\": null,\n" +
		"							\"min\": null,\n" +
		"							\"show\": true\n" +
		"						}\n" +
		"					]\n" +
		"				}\n" +
		"			],\n" +
		"			\"repeat\": null,\n" +
		"			\"repeatIteration\": null,\n" +
		"			\"repeatRowId\": null,\n" +
		"			\"showTitle\": false,\n" +
		"			\"title\": \"Dashboard Row\",\n" +
		"			\"titleSize\": \"h6\"\n" +
		"		}";

	public static final String KAFKA_OFFSET_TARGET = "{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${kafka_source}.KafkaConsumer." +
			"current_offsets\",\n" +
			"							\"refId\": \"P\",\n" +
			"							\"shouldComputeRate\": true\n" +
			"						},\n" +
			"						{\n" +
			"							\"aggregator\": \"max\",\n" +
			"							\"downsampleAggregator\": \"avg\",\n" +
			"							\"downsampleFillPolicy\": \"none\",\n" +
			"							\"metric\": \"flink.taskmanager.${jobname}.${kafka_source}.KafkaConsumer." +
			"commit_rate\",\n" +
			"							\"refId\": \"R\"\n" +
			"						}";

	public static final String KAFKA_LATENCY_TARGET = "{\n" +
		"							\"aggregator\": \"max\",\n" +
		"							\"downsampleAggregator\": \"avg\",\n" +
		"							\"downsampleFillPolicy\": \"none\",\n" +
		"							\"metric\": \"flink.taskmanager.${jobname}.${kafka_source}.fetch_latency_avg\",\n" +
		"							\"refId\": \"P\"\n" +
		"						},\n" +
		"						{\n" +
		"							\"aggregator\": \"max\",\n" +
		"							\"downsampleAggregator\": \"avg\",\n" +
		"							\"downsampleFillPolicy\": \"none\",\n" +
		"							\"metric\": \"flink.taskmanager.${jobname}.${kafka_source}.fetch_latency_max\",\n" +
		"							\"refId\": \"R\"\n" +
		"						}";
}
