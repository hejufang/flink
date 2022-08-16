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

package org.apache.flink.connector.rocketmq;

import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * RocketMQMetadata.
 */
public enum RocketMQMetadata implements DynamicSourceMetadataFactory.DynamicSourceMetadata {
	QUEUE_ID("queue_id", BigIntType.class),
	OFFSET("offset", BigIntType.class),
	TIMESTAMP("timestamp", BigIntType.class),
	BROKER_NAME("broker_name", VarCharType.class),
	MESSAGE_ID("message_id", VarCharType.class),
	TAG("tag", VarCharType.class);

	private final String metadata;

	private final Class<?> dataTypeClass;

	RocketMQMetadata(String metadata, Class<?> dataClass) {
		this.metadata = metadata;
		this.dataTypeClass = dataClass;
	}

	public static RocketMQMetadata findByName(String name) {
		for (RocketMQMetadata metadata: RocketMQMetadata.values()) {
			if (metadata.getMetadata().equals(name)) {
				return metadata;
			}
		}
		return null;
	}

	public static boolean contains(String name) {
		return findByName(name) != null;
	}

	public String getMetadata() {
		return metadata;
	}

	@Override
	public Class<?> getLogicalClass() {
		return dataTypeClass;
	}

	public static String getValuesString() {
		return Stream.of(RocketMQMetadata.values())
			.map(RocketMQMetadata::getMetadata)
			.collect(Collectors.joining(","));
	}
}
