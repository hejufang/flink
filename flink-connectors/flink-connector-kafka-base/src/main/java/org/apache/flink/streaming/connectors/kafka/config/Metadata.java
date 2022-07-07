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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.VarCharType;

import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * Metadata for kafka records.
 */
public enum Metadata implements DynamicSourceMetadataFactory.DynamicSourceMetadata {
	PARTITION("partition", BigIntType.class),
	OFFSET("offset", BigIntType.class),
	TIMESTAMP("timestamp", BigIntType.class),
	KEY("key", VarCharType.class);

	private final String metadata;

	private final Class<?> dataClass;

	Metadata(String metadata, Class<?> dataClass) {
		this.metadata = metadata;
		this.dataClass = dataClass;
	}

	public static Metadata findByName(String name) {
		for (Metadata metadata: Metadata.values()) {
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
		return dataClass;
	}

	public static String getValuesString() {
		return Stream.of(Metadata.values())
			.map(Metadata::getMetadata)
			.collect(Collectors.joining(","));
	}
}
