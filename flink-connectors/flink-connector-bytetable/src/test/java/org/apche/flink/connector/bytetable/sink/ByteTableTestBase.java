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

package org.apche.flink.connector.bytetable.sink;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import java.util.HashMap;
import java.util.Map;

/**
 * Base class for ByteTable test.
 */
public class ByteTableTestBase {

	protected static final String TABLE_NAME = "bytetable_mock_table";
	protected static final String CLUSTER_NAME = "bytetable_mock_cluster";
	protected static final String SERVICE_NAME = "my_service";
	protected static final String JOB_NAME = "my_job";
	protected static final String CONNECTOR = "bytetable";

	protected TableSchema createTableSchema() {
		DataTypes.Field field1 = DataTypes.FIELD("k1", DataTypes.INT());
		DataTypes.Field field2 = DataTypes.FIELD("k2", DataTypes.STRING());
		return TableSchema.builder()
			.field("rowKey", DataTypes.STRING().notNull())
			.field("columnFamily1", DataTypes.ROW(field1, field2))
			.primaryKey("rowKey")
			.build();
	}

	protected Map<String, String> getBasicOptions() {
		Map<String, String> options = new HashMap<>();
		options.put("connector", CONNECTOR);
		options.put("table-name", TABLE_NAME);
		options.put("cluster", CLUSTER_NAME);
		options.put("service", SERVICE_NAME);
		return options;
	}

	protected ReadableConfig getConfiguration() {
		Configuration conf = new Configuration();
		conf.setString(PipelineOptions.NAME, JOB_NAME);
		return conf;
	}
}
