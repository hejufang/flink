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

package org.apache.flink.state.table.factories;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.state.table.tables.KeyedStateDynamicTableSource;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.state.table.catalog.SavepointCatalogUtils.SAVEPOINT_KEYED_STATE_TABLE_NAME;

/**
 * Factory for StateMeta batch table source.
 */
public class KeyedStateDynamicTableSourceFactory implements DynamicTableSourceFactory {

	private static final String IDENTIFIER = SAVEPOINT_KEYED_STATE_TABLE_NAME;

	private static final ConfigOption<String> PATH = ConfigOptions
		.key("path")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines savepoint path.");

	private static final ConfigOption<String> OPERATOR_ID = ConfigOptions
		.key("operatorID")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines operatorID.");

	private static final ConfigOption<String> STATE_NAME = ConfigOptions
		.key("stateName")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines stateName.");

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(PATH);
		set.add(OPERATOR_ID);
		set.add(STATE_NAME);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		return set;
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {

		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);

		ReadableConfig config = helper.getOptions();
		String savepointPath = config.get(PATH);
		String operatorID = config.get(OPERATOR_ID);
		String stateName = config.get(STATE_NAME);

		DataType producedDataType = context.getCatalogTable().getSchema().toPhysicalRowDataType();

		return new KeyedStateDynamicTableSource(savepointPath, operatorID, stateName, producedDataType);

	}
}
