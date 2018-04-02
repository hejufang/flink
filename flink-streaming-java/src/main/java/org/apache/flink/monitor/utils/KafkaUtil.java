package org.apache.flink.monitor.utils;

import java.io.FileReader;
import java.io.IOException;
import org.json.simple.JSONObject;
import org.json.simple.parser.JSONParser;
import org.json.simple.parser.ParseException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
