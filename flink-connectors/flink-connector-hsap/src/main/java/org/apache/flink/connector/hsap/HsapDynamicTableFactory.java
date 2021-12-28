/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.hsap;

import org.apache.flink.api.common.io.ratelimiting.GuavaFlinkConnectorRateLimiter;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.hsap.HsapOptions.ADDR_LIST;
import static org.apache.flink.connector.hsap.HsapOptions.CONNECTION_PER_SERVER;
import static org.apache.flink.connector.hsap.HsapOptions.DB_NAME;
import static org.apache.flink.connector.hsap.HsapOptions.TABLE_NAME;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_INTERVAL;
import static org.apache.flink.table.factories.FactoryUtil.SINK_BUFFER_FLUSH_MAX_ROWS;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * hsap connector factory.
 */
public class HsapDynamicTableFactory implements DynamicTableSinkFactory {
	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();

		TableSchema tableSchema = context.getCatalogTable().getSchema();

		return new HsapDynamicTableSink(tableSchema, gethsapOptions(helper.getOptions(), tableSchema));
	}

	@Override
	public String factoryIdentifier() {
		return HsapOptions.HSAP_IDENTIFY;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(ADDR_LIST);
		set.add(DB_NAME);
		set.add(TABLE_NAME);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(PARALLELISM);
		set.add(RATE_LIMIT_NUM);
		set.add(CONNECTION_PER_SERVER);
		set.add(SINK_BUFFER_FLUSH_MAX_ROWS);
		set.add(SINK_BUFFER_FLUSH_INTERVAL);
		return set;
	}

	private HsapOptions gethsapOptions(ReadableConfig readableConfig, TableSchema tableSchema) {
		HsapOptions.Builder hsapOptions = HsapOptions.builder();

		// convert string to List<Pair<String, Integer>> .
		hsapOptions.setAddr(readableConfig.get(ADDR_LIST));
		hsapOptions.setTable(readableConfig.get(TABLE_NAME));
		hsapOptions.setDatabase(readableConfig.get(DB_NAME));

		readableConfig.getOptional(SINK_BUFFER_FLUSH_MAX_ROWS).ifPresent(hsapOptions::setBatchRowNum);
		readableConfig.getOptional(CONNECTION_PER_SERVER).ifPresent(hsapOptions::setConnectionPerServer);
		readableConfig.getOptional(RATE_LIMIT_NUM).ifPresent(
			limit -> {
				GuavaFlinkConnectorRateLimiter rateLimit = new GuavaFlinkConnectorRateLimiter();
				rateLimit.setRate(limit);
				hsapOptions.setRateLimiter(rateLimit);
			}
		);
		readableConfig.getOptional(SINK_BUFFER_FLUSH_INTERVAL).ifPresent(
			interval ->
				hsapOptions.setFlushIntervalMs(interval.toMillis())
		);
		readableConfig.getOptional(PARALLELISM).ifPresent(hsapOptions::setParallelism);

		return hsapOptions.build();
	}
}
