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

package org.apache.flink.connector.tos;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.table.factories.SerializationFormatFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * TOS connector factory.
 */
public class TosDynamicTableFactory implements DynamicTableSinkFactory {

	private static final String IDENTIFIER = "tos";

	private static final ConfigOption<String> BUCKET = ConfigOptions
		.key("bucket")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines tos bucket name.");

	private static final ConfigOption<String> ACCESS_KEY = ConfigOptions
		.key("access-key")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines tos access key.");

	private static final ConfigOption<String> OBJECT_KEY = ConfigOptions
		.key("object-key")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines tos object key.");

	private static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.defaultValue("default")
		.withDescription("Optional. It defines cluster.");

	private static final ConfigOption<String> PSM = ConfigOptions
		.key("psm")
		.stringType()
		.defaultValue("toutiao.tos.tosapi")
		.withDescription("Optional. It defines psm.");

	private static final ConfigOption<Integer> TIMEOUT_SECONDS = ConfigOptions
		.key("timeout-seconds")
		.intType()
		.defaultValue(10)
		.withDescription("Optional. It defines tos request timeout, the unit is seconds.");

	private static final ConfigOption<String> FORMAT = ConfigOptions
		.key("format")
		.stringType()
		.defaultValue("json")
		.withDescription("Build-in, Tos sink uses built-in 'json' format to organize "
			+ "and it can't be changed outside");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();
		final EncodingFormat<SerializationSchema<RowData>> format = helper.discoverEncodingFormat(
			SerializationFormatFactory.class,
			FORMAT);
		TableSchema tableSchema = context.getCatalogTable().getSchema();

		TosOptions.Builder tosOptionsBuilder = TosOptions.builder();
		tosOptionsBuilder.setBucket(helper.getOptions().get(BUCKET));
		tosOptionsBuilder.setAccessKey(helper.getOptions().get(ACCESS_KEY));
		tosOptionsBuilder.setObjectKey(helper.getOptions().get(OBJECT_KEY));
		tosOptionsBuilder.setCluster(helper.getOptions().get(CLUSTER));
		tosOptionsBuilder.setPsm(helper.getOptions().get(PSM));
		tosOptionsBuilder.setTosTimeoutSeconds(helper.getOptions().get(TIMEOUT_SECONDS));

		return new TosDynamicTableSink(tableSchema, tosOptionsBuilder.build(), format);
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(BUCKET);
		set.add(ACCESS_KEY);
		set.add(OBJECT_KEY);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(CLUSTER);
		set.add(PSM);
		set.add(TIMEOUT_SECONDS);
		return set;
	}
}
