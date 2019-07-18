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

package org.apache.flink.monitor.utils;

import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.FileReader;
import java.io.IOException;

/**
 * Kafka util to parse topic information.
 * */
public class KafkaUtil {
	private static final String KAFKA_SS_CONF_FILE =
			"/opt/tiger/ss_conf/ss/internal_kafka_meta_conf.json";
	private static final Logger LOG = LoggerFactory.getLogger(KafkaUtil.class);
	public static String getKafkaTopicPrefix(String cluster) {
		JSONObject json = parse();
		JSONObject kafkaCluster = (JSONObject) json.get(cluster);
		String kafkaTopicPrefix = (String) kafkaCluster.get("topic_related_metric_prefix");
		return kafkaTopicPrefix;
	}

	public static JSONObject parse() {
		JSONParser parser = new JSONParser();
		JSONObject json = null;
		try {
			json = (JSONObject) parser.parse(new FileReader(KAFKA_SS_CONF_FILE));
		} catch (IOException e) {
			LOG.error("IOException while parsing kafka configuration file", e);
		} catch (ParseException e) {
			LOG.error("ParseException  while parsing kafka configuration file", e);
		}
		return json;
	}
}
