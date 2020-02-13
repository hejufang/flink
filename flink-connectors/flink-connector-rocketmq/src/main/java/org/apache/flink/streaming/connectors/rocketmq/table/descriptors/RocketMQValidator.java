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

package org.apache.flink.streaming.connectors.rocketmq.table.descriptors;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * RocketMQ validator.
 */
public class RocketMQValidator extends ConnectorDescriptorValidator {
	public static final String CONNECTOR_TYPE_VALUE_ROCKETMQ = "rocketmq";

	// Server Config
	public static final String CONNECTOR_NAME_SERVER_ADDR = "connector.nameserver.address";
	public static final String CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN = "connector.rocketmq.namesrv.domain";
	public static final String CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP =
		"connector.rocketmq.namesrv.domain.subgroup";

	// Consumer config
	public static final String CONNECTOR_ROCKETMQ_CONSUMER_PSM = "connector.rocketmq.consumer.psm";
	public static final String CONNECTOR_CONSUMER_GROUP = "connector.consumer.group";
	public static final String CONNECTOR_CONSUMER_TOPIC = "connector.consumer.topic";
	public static final String CONNECTOR_CONSUMER_TAG = "connector.consumer.tag";
	public static final String CONNECTOR_CONSUMER_OFFSET_RESET_TO = "connector.consumer.offset.reset.to";
	public static final String CONNECTOR_CONSUMER_OFFSET_FROM_TIMESTAMP =
		"connector.consumer.offset.from.timestamp";

	// Producer config
	public static final String CONNECTOR_ROCKETMQ_PRODUCER_PSM = "connector.rocketmq.producer.psm";
	public static final String CONNECTOR_PRODUCER_GROUP = "connector.producer.group";
	public static final String CONNECTOR_PRODUCER_TOPIC = "connector.producer.topic";
	public static final String CONNECTOR_PRODUCER_TAG = "connector.producer.tag";
	public static final String CONNECTOR_TOPIC_SELECTOR = "connector.topic-selector";

	public static final String CONNECTOR_ROCKETMQ_PROPERTIES = "connector.rocketmq.properties";
	public static final String DEFAULT_TOPIC_SELECTOR = "DefaultTopicSelector";

	public static final String CONSUMER = "consumer";
	public static final String PRODUCER = "producer";

	public static final String CONNECTOR_BATCH_FLUSH_ENABLED = "connector.batch-flush-enabled";
	public static final String CONNECTOR_BATCH_SIZE = "connector.batch-size";
	public static final String CONNECTOR_ASYNC_MODE_ENABLED = "connector.async-mode-enabled";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateValue(CONNECTOR_TYPE, CONNECTOR_TYPE_VALUE_ROCKETMQ, false);

		properties.validateString(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN, true, 1);
		properties.validateString(CONNECTOR_NAME_SERVER_ADDR, true, 1);
		properties.validateString(CONNECTOR_ROCKETMQ_NAMESRV_DOMAIN_SUBGROUP, true, 1);

		properties.validateString(CONNECTOR_ROCKETMQ_CONSUMER_PSM, true, 1);
		properties.validateString(CONNECTOR_CONSUMER_GROUP, true, 1);
		properties.validateString(CONNECTOR_CONSUMER_TOPIC, true, 1);
		properties.validateString(CONNECTOR_CONSUMER_TAG, true, 1);
		properties.validateString(CONNECTOR_CONSUMER_OFFSET_RESET_TO, true, 1);
		properties.validateLong(CONNECTOR_CONSUMER_OFFSET_FROM_TIMESTAMP, true);

		properties.validateString(CONNECTOR_ROCKETMQ_PRODUCER_PSM, true, 1);
		properties.validateString(CONNECTOR_PRODUCER_GROUP, true, 1);
		properties.validateString(CONNECTOR_PRODUCER_TOPIC, true, 1);
		properties.validateString(CONNECTOR_PRODUCER_TAG, true, 1);
		properties.validateString(CONNECTOR_TOPIC_SELECTOR, true, 1);
		properties.validateBoolean(CONNECTOR_BATCH_FLUSH_ENABLED, true);
		properties.validateInt(CONNECTOR_BATCH_SIZE, true, 1);
		properties.validateBoolean(CONNECTOR_ASYNC_MODE_ENABLED, true);
	}
}
