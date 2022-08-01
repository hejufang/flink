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

import org.apache.flink.configuration.MemorySize;
import org.apache.flink.connector.bytetable.ByteTableDynamicTableFactory;
import org.apache.flink.connector.bytetable.options.ByteTableOptions;
import org.apache.flink.connector.bytetable.options.ByteTableWriteOptions;
import org.apache.flink.connector.bytetable.sink.ByteTableDynamicTableSink;
import org.apache.flink.connector.bytetable.source.ByteTableDynamicTableSource;
import org.apache.flink.connector.bytetable.util.BConstants;
import org.apache.flink.connector.bytetable.util.ByteTableMutateType;
import org.apache.flink.connector.bytetable.util.ByteTableSchema;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.catalog.CatalogTableImpl;
import org.apache.flink.table.catalog.ObjectIdentifier;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.factories.FactoryUtil;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.junit.Test;

import java.time.Duration;
import java.util.Map;

import static org.apache.flink.configuration.PipelineOptions.JOB_PSM_PREFIX;
import static org.junit.Assert.assertEquals;

/**
 * Test for {@link ByteTableDynamicTableSource} and {@link ByteTableDynamicTableSink} created
 * by {@link ByteTableDynamicTableFactory}.
 */
public class ByteTableDynamicTableFactoryTest extends ByteTableTestBase {

	@Test
	public void testCreateDynamicTableSourceWithParallelism() {
		TableSchema schema = createTableSchema();

		Configuration byteTableClientConf = HBaseConfiguration.create();
		//Connection impl should be com.bytedance.bytetable.hbase.BytetableConnection when use ByteTable.
		byteTableClientConf.set(BConstants.HBASE_CLIENT_CONNECTION_IMPL, BConstants.BYTETABLE_CLIENT_IMPL);
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_PSM, JOB_PSM_PREFIX + JOB_NAME);
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_CLUSTERNAME, CLUSTER_NAME);
		byteTableClientConf.set(BConstants.BYTETABLE_CLIENT_SERVICENAME, SERVICE_NAME);
		//ByteTable does not support RegionSizeCalculator, so set it false.
		byteTableClientConf.setBoolean(BConstants.HBASE_REGIONSIZECALCULATOR_ENABLE, false);

		ByteTableDynamicTableSource expected = new ByteTableDynamicTableSource(
			byteTableClientConf,
			"default" + ":" + TABLE_NAME,
			ByteTableSchema.fromTableSchema(schema),
			"null",
			null,
			10);

		Map<String, String> options = getBasicOptions();
		options.put("parallelism", "10");
		DynamicTableSource actual = createTableSource(schema, options);
		assertEquals(expected, actual);
	}

	@Test
	public void testCreateDynamicTableSinkWithParallelism() {
		TableSchema schema = createTableSchema();

		ByteTableOptions bytetableOptions = ByteTableOptions.builder()
			.setDatabase("default")
			.setTableName(TABLE_NAME)
			.setPsm(JOB_PSM_PREFIX + JOB_NAME)
			.setCluster(CLUSTER_NAME)
			.setService(SERVICE_NAME)
			.setConnTimeoutMs(1000)
			.setChanTimeoutMs(1000)
			.setReqTimeoutMs(5000)
			.setMutateType(ByteTableMutateType.MUTATE_SINGLE)
			.setParallelism(10)
			.build();
		ByteTableWriteOptions writeOptions = ByteTableWriteOptions.builder()
			.setBufferFlushMaxSizeInBytes(MemorySize.parse("2mb").getBytes())
			.setBufferFlushIntervalMillis(Duration.ofSeconds(1).toMillis())
			.setBufferFlushMaxRows(50)
			.setCellTTLMicroSeconds(Duration.ZERO.toMillis() * 1000)
			.setIgnoreDelete(true)
			.build();
		String nullStringLiteral = "null";
		ByteTableSchema byteTableSchema = ByteTableSchema.fromTableSchema(schema);

		ByteTableDynamicTableSink expected = new  ByteTableDynamicTableSink(
			byteTableSchema,
			bytetableOptions,
			writeOptions,
			nullStringLiteral);

		Map<String, String> options = getBasicOptions();
		options.put("parallelism", "10");
		assertEquals(expected, createTableSink(schema, options));
	}

	private DynamicTableSource createTableSource(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSource(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock source"),
			getConfiguration(),
			ByteTableDynamicTableFactoryTest.class.getClassLoader());
	}

	private DynamicTableSink createTableSink(TableSchema schema, Map<String, String> options) {
		return FactoryUtil.createTableSink(
			null,
			ObjectIdentifier.of("default", "default", "t1"),
			new CatalogTableImpl(schema, options, "mock sink"),
			getConfiguration(),
			ByteTableDynamicTableFactoryTest.class.getClassLoader());
	}
}
