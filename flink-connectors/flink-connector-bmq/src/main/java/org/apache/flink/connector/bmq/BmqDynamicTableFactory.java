/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bmq;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

/**
 * Factory for BMQ batch table source.
 */
public class BmqDynamicTableFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = "bmq";
	// in case of using second unit timestamp, we limit min value of timestamp to MIN_TIMESTAMP
	// which is 2001/9/9 9:46:40 in Beijing time zone.
	private static final long MIN_TIMESTAMP = 1_000_000_000_000L;

	private static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines BMQ cluster name.");

	private static final ConfigOption<String> TOPIC = ConfigOptions
		.key("topic")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines BMQ topic name.");

	private static final ConfigOption<Long> START_TIME_MS = ConfigOptions
		.key("start-time-ms")
		.longType()
		.noDefaultValue()
		.withDescription("Required. It defines the start time for scanning.");

	private static final ConfigOption<Long> END_TIME_MS = ConfigOptions
		.key("end-time-ms")
		.longType()
		.noDefaultValue()
		.withDescription("Required. It defines the end time for scanning.");

	private static final ConfigOption<Boolean> IGNORE_UNHEALTHY_SEGMENT = ConfigOptions
		.key("ignore-unhealthy-segment")
		.booleanType()
		.defaultValue(false)
		.withDescription("Optional. If enabled, bmq connector will ignore unhealthy segments, which may lead to " +
			"data loss. Will throw an Exception by default.");

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(CLUSTER);
		set.add(TOPIC);
		set.add(START_TIME_MS);
		set.add(END_TIME_MS);

		set.add(FactoryUtil.FORMAT);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(IGNORE_UNHEALTHY_SEGMENT);
		return set;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = helper.discoverDecodingFormat(
			DeserializationFormatFactory.class,
			FactoryUtil.FORMAT);

		ReadableConfig config = helper.getOptions();

		String cluster = config.get(CLUSTER);
		String topic = config.get(TOPIC);
		long startMs = config.get(START_TIME_MS);
		long endMs = config.get(END_TIME_MS);
		boolean ignoreUnhealthySegment = config.get(IGNORE_UNHEALTHY_SEGMENT);

		if (startMs > endMs) {
			throw new ValidationException(String.format("%s should be larger than %s, but current setting is " +
					"%s=%d, %s=%d.",
				END_TIME_MS.key(),
				START_TIME_MS.key(),
				END_TIME_MS.key(), endMs,
				START_TIME_MS.key(), startMs));
		}
		if (startMs < MIN_TIMESTAMP) {
			throw new ValidationException(String.format("Your %s=%d is too small, %s and %s should be used as " +
					"milliseconds, are you setting them as seconds?",
				START_TIME_MS.key(), startMs,
				START_TIME_MS.key(),
				END_TIME_MS.key()));
		}

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return new BmqDynamicTableSource(
			cluster,
			topic,
			startMs,
			endMs,
			decodingFormat,
			producedDataType,
			ignoreUnhealthySegment);
	}
}
