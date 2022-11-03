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
 * software distributed under the License is distributed ON an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.table.planner.runtime.stream.sql;

import org.apache.flink.client.cli.CheckpointVerifyResult;
import org.apache.flink.runtime.jobgraph.JobGraph;
import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.table.api.bridge.java.internal.StreamTableEnvironmentImpl;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.table.planner.runtime.utils.TestData;
import org.apache.flink.test.checkpointing.CheckpointVerifyTestBase;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

/**
 * Tests for checkpoint verification of SQL jobs.
 */
public class CheckpointVerifyTests extends CheckpointVerifyTestBase {
	private static final String SOURCE_CREATION_WITHOUT_WATERMARK = "CREATE TABLE T1 (\n" +
		"  `stringVal` STRING,\n" +
		"  `intVal` INT,\n" +
		"  `longVal` BIGINT,\n" +
		"  `floatVal` FLOAT,\n" +
		"  `tsVal` TIMESTAMP(3),\n" +
		"  `proc` as PROCTIME()\n" +
		") WITH (\n" +
		"  'connector' = 'values',\n" +
		"  'data-id' = '%s',\n" +
		"  'table-source-class' = '%s',\n" +
		"  'changelog-mode' = '%s'\n" +
		")";
	private static final String SOURCE_CREATION_WITH_WATERMARK = "CREATE TABLE T1 (\n" +
		"  `stringVal` STRING,\n" +
		"  `intVal` INT,\n" +
		"  `longVal` BIGINT,\n" +
		"  `floatVal` FLOAT,\n" +
		"  `tsVal` TIMESTAMP(3),\n" +
		"  `proc` as PROCTIME()," +
		"  WATERMARK FOR tsVal AS tsVal - INTERVAL '5' SECOND\n" +
		") WITH (\n" +
		"  'connector' = 'values',\n" +
		"  'data-id' = '%s',\n" +
		"  'table-source-class' = '%s'\n" +
		")";

	// ------------------------------------------------------------------------
	//  For Unbounded Aggregation
	// ------------------------------------------------------------------------

	@Test
	public void testNormalAggregateChangeAggregationFunction() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  LAST_VALUE_IGNORE_RETRACT(stringVal),\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testNormalAggregateChangeDistinctAggregationFunction() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct intVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testNormalAggregateChangeGroupByKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  longVal," +
				"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testMiniBatchAggregate() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  LAST_VALUE_IGNORE_RETRACT(stringVal),\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		tableConfigs.getConfiguration()
			.setString("table.optimizer.agg-phase-strategy", "ONE_PHASE");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testGlobalAggregate() throws Exception {
		String beforeSQL =
			"SELECT\n" +
			"  intVal,\n" +
			"  SUM(floatVal),\n" +
			"  COUNT(distinct stringVal)\n" +
			"FROM\n" +
			"  T1\n" +
			"GROUP BY\n" +
			"  intVal";
		String afterSQL =
			"SELECT\n" +
			"  intVal,\n" +
			"  LAST_VALUE_IGNORE_RETRACT(stringVal),\n" +
			"  SUM(floatVal),\n" +
			"  COUNT(distinct stringVal)\n" +
			"FROM\n" +
			"  T1\n" +
			"GROUP BY\n" +
			"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testIncrementalAggregate() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  LAST_VALUE_IGNORE_RETRACT(stringVal),\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		tableConfigs.getConfiguration()
			.setString("table.optimizer.distinct-agg.split.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.optimizer.incremental-agg-enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.optimizer.two-stage-optimization.result-updating-input-enabled", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	// ------------------------------------------------------------------------
	//  For Window Aggregation
	// ------------------------------------------------------------------------

	@Test
	public void testWindowAggregateChangeAggregationFunction() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  LAST_VALUE_IGNORE_RETRACT(stringVal),\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testWindowAggregateChangeDistinctAggregationFunction() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct intVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testWindowAggregateChangeGroupByKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		String afterSQL =
			"SELECT\n" +
				"  intVal,\n" +
				"  SUM(floatVal),\n" +
				"  COUNT(distinct stringVal)\n" +
				"FROM\n" +
				"  T1\n" +
				"GROUP BY\n" +
				"  longVal," +
				"  intVal,\n" +
				"  tumble(tsVal, interval '1' minute)";
		TableConfig tableConfigs = TableConfig.getDefault();
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	// ------------------------------------------------------------------------
	//  For Deduplicate
	// ------------------------------------------------------------------------

	@Test
	public void testMiniBatchDeduplicateKeepLastChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  "I");
	}

	@Test
	public void testMiniBatchDeduplicateKeepLastChangePartitionKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal, stringVal \n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  "I");
	}

	@Test
	public void testMiniBatchDeduplicateKeepFirstChangePartitionKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal\n" +
				"                ORDER BY\n" +
				"                    proc\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal, stringVal \n" +
				"                ORDER BY\n" +
				"                    proc\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  "I");
	}

	@Test
	public void testDeduplicateKeepLastChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I");
	}

	@Test
	public void testKeepLastChangePartitionKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal\n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal, stringVal \n" +
				"                ORDER BY\n" +
				"                    proc DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I");
	}

	@Test
	public void testDeduplicateKeepFirstChangePartitionKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal\n" +
				"                ORDER BY\n" +
				"                    proc\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal, stringVal \n" +
				"                ORDER BY\n" +
				"                    proc\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I");
	}

	// ------------------------------------------------------------------------
	//  For Rank
	// ------------------------------------------------------------------------

	@Test
	public void testAppendOnlyTopNChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I");
	}

	@Test
	public void testAppendOnlyTopNPartitionKeys() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY longVal, stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I");
	}

	@Test
	public void testFastTop1ChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.enable-top1", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  "I");
	}

	@Test
	public void testUpdatableTopNChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            (\n" +
				"			 	 SELECT\n" +
				"					 intVal, stringVal, tsVal\n" +
				"				 FROM T1\n" +
				"				 GROUP BY intVal, stringVal, tsVal\n" +
				"			 ) t\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            (\n" +
				"			 	 SELECT\n" +
				"					 longVal, intVal, stringVal, tsVal\n" +
				"				 FROM T1\n" +
				"				 GROUP BY longVal, intVal, stringVal, tsVal\n" +
				"			 ) t\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.enable-stateless-agg", "true");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  "I");
	}

	@Test
	public void testRetractableTopNChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		String afterSQL =
			"SELECT\n" +
				"    *\n" +
				"FROM(\n" +
				"        SELECT\n" +
				"		     longVal,\n" +
				"			 intVal,\n" +
				"            ROW_NUMBER() over (\n" +
				"                PARTITION BY stringVal\n" +
				"                ORDER BY\n" +
				"                    tsVal DESC\n" +
				"            ) AS rn\n" +
				"        FROM\n" +
				"            T1\n" +
				"    ) p\n" +
				"WHERE\n" +
				"    rn = 1\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  "I,UA,UB,D");
	}


	// ------------------------------------------------------------------------
	//  For Lookup Join
	// ------------------------------------------------------------------------

	@Test
	public void testLookupJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN dim for system_time AS of proc AS T3\n" +
				"  ON T1.intVal = T3.id";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal,\n" +
				"  T1.stringVal\n" +
				"FROM\n" +
				"  T1 JOIN dim for system_time AS of proc AS T3\n" +
				"  ON T1.intVal = T3.id";
		testWithSQL(beforeSQL, afterSQL, null, false,  null);
	}

	// ------------------------------------------------------------------------
	//  For Interval Join
	// ------------------------------------------------------------------------

	@Test
	public void testIntervalJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n" +
				"  and T1.tsVal between T2.tsVal - INTERVAL '5' SECOND and T2.tsVal + INTERVAL '5' SECOND";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal,\n" +
				"  T1.stringVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n" +
				"  and T1.tsVal between T2.tsVal - INTERVAL '5' SECOND and T2.tsVal + INTERVAL '5' SECOND";
		testWithSQL(beforeSQL, afterSQL, null, false,  null);
	}

	@Test
	public void testIntervalJoinChangeJoinKey() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n" +
				"  and T1.tsVal between T2.tsVal - INTERVAL '5' SECOND and T2.tsVal + INTERVAL '5' SECOND";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal and T1.stringVal = T2.stringVal\n" +
				"  and T1.tsVal between T2.tsVal - INTERVAL '5' SECOND and T2.tsVal + INTERVAL '5' SECOND";
		testWithSQL(beforeSQL, afterSQL, null, false, "I");
	}

	// ------------------------------------------------------------------------
	//  For Equi-Join
	// ------------------------------------------------------------------------

	@Test
	public void testEquiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal,\n" +
				"  T1.stringVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		testWithSQL(beforeSQL, afterSQL, null, false,  null);
	}

	@Test
	public void testEquiJoinChangeJoinKey() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal and T1.stringVal = T2.stringVal\n";
		testWithSQL(beforeSQL, afterSQL, null, false, "I");
	}

	@Test
	public void testMiniBatchEquiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal,\n" +
				"  T1.stringVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enable-regular-join", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false,  null);
	}

	@Test
	public void testMiniBatchEquiJoinChangeJoinKey() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal\n";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1 JOIN T1 AS T2 ON T1.intVal = T2.intVal and T1.stringVal = T2.stringVal\n";
		TableConfig tableConfigs = TableConfig.getDefault();
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enable-regular-join", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.enabled", "true");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.allow-latency", "10ms");
		tableConfigs.getConfiguration()
			.setString("table.exec.mini-batch.size", "2");
		testWithSQL(beforeSQL, afterSQL, tableConfigs, false, "I");
	}

	@Test
	public void testSemiJoinChangeSelectList() throws Exception {
		String beforeSQL =
			"SELECT\n" +
				"  T1.longVal\n" +
				"FROM\n" +
				"  T1\n" +
				"WHERE T1.longVal IN\n" +
				"(\n" +
				"  SELECT\n" +
				"	 CAST(T1.intVal AS BIGINT)\n" +
				"  FROM T1\n" +
				")";
		String afterSQL =
			"SELECT\n" +
				"  T1.longVal,\n" +
				"  T1.stringVal\n" +
				"FROM\n" +
				"  T1\n" +
				"WHERE T1.longVal IN\n" +
				"(\n" +
				"  SELECT\n" +
				"	 CAST(T1.intVal AS BIGINT)\n" +
				"  FROM T1\n" +
				")";
		testWithSQL(beforeSQL, afterSQL, null, false,  null);
	}

	void testWithSQL(
			String beforeSQL,
			String afterSQL,
			TableConfig tableConfigs,
			boolean isCompatible,
			String sourceChangeMode) throws Exception {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv;
		EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.inStreamingMode()
			.useBlinkPlanner()
			.build();

		if (tableConfigs == null) {
			tableConfigs = new TableConfig();
		}
		tEnv = StreamTableEnvironmentImpl.create(env, settings, tableConfigs);
		// Source
		if (sourceChangeMode == null || sourceChangeMode.isEmpty()) {
			sourceChangeMode = "I";
		}
		String id = TestValuesTableFactory.registerData(TestData.data4WithTimestamp());
		String sql;
		if (sourceChangeMode.equals("I")) {
			sql = String.format(SOURCE_CREATION_WITH_WATERMARK, id, FiniteScanTableSource.class.getName());
		} else {
			sql = String.format(SOURCE_CREATION_WITHOUT_WATERMARK, id, FiniteScanTableSource.class.getName(), sourceChangeMode);
		}
		tEnv.executeSql(sql);
		// Dim
		tEnv.executeSql("CREATE TABLE dim (\n" +
			"  `id` INT,\n" +
			"  `name` STRING,\n" +
			"  `age` INT,\n" +
			"  `ts` TIMESTAMP(3)\n" +
			") WITH (\n" +
			"  'connector' = 'values',\n" +
			"  'lookup.later-join-latency' = '2s',\n" +
			"  'lookup-function-class' = 'org.apache.flink.table.planner.runtime.stream.sql.CheckpointVerifyTests$DimFunction'\n" +
			")");
		Table sinkTable = tEnv.sqlQuery(beforeSQL);
		tEnv.toRetractStream(sinkTable, RowData.class);
		JobGraph oldJob = env.getStreamGraph().getJobGraph();
		final String savepointPath = submitJobAndTakeSavepoint(clusterFactory, oldJob);

		Table sinkTable2 = tEnv.sqlQuery(afterSQL);
		tEnv.toRetractStream(sinkTable2, RowData.class);
		JobGraph newJob = env.getStreamGraph().getJobGraph();

		CheckpointVerifyResult checkpointVerifyResult = verifyCheckpoint(savepointPath, newJob);
		if (isCompatible) {
			Assert.assertEquals(CheckpointVerifyResult.SUCCESS, checkpointVerifyResult);
		} else {
			Assert.assertEquals(CheckpointVerifyResult.STATE_SERIALIZER_INCOMPATIBLE, checkpointVerifyResult);
		}
	}

	/**
	 * FiniteScanTableSource.
	 */
	public static class FiniteScanTableSource implements ScanTableSource,
			TestValuesTableFactory.TableSourceWithData {
		private List<Row> data;
		private ChangelogMode changelogMode;
		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
			return SourceFunctionProvider.of(new FiniteTestSource(data), false);
		}

		@Override
		public DynamicTableSource copy() {
			FiniteScanTableSource source = new FiniteScanTableSource();
			source.setTableSourceData(data);
			return source;
		}

		@Override
		public String asSummaryString() {
			return null;
		}

		@Override
		public void setTableSourceData(Collection<Row> rows) {
			data = new ArrayList<>();

		}

		@Override
		public void setChangelog(ChangelogMode changelogMode) {
			this.changelogMode = changelogMode;
		}
	}

	/**
	 * FiniteTestSource.
	 */
	public static class FiniteTestSource implements SourceFunction<RowData>, CheckpointListener {
		private static final long serialVersionUID = 1L;
		private final List<RowData> elements;
		private transient int numCheckpointsComplete;
		private volatile boolean running = true;

		public FiniteTestSource(Iterable<Row> elements) {
			this.elements = new ArrayList<>();
			for (Row row: elements) {
				this.elements.add(row2GenericRowData(row));
			}
		}

		private RowData row2GenericRowData(Row row) {
			GenericRowData rowData = new GenericRowData(row.getArity());
			for (int i = 0; i < row.getArity(); i++) {
				Object field = row.getField(i);
				if (field instanceof String) {
					rowData.setField(i, StringData.fromString((String) field));
				} else {
					rowData.setField(i, field);
				}
			}
			return rowData;
		}

		@Override
		public void run(SourceContext<RowData> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();
			synchronized (lock) {
				for (RowData t : elements) {
					ctx.collect(t);
				}
			}

			synchronized (lock) {
				while (running && numCheckpointsComplete < 1) {
					lock.wait(1);
				}
			}
		}

		@Override
		public void cancel() {
			running = false;
		}

		@Override
		public void notifyCheckpointComplete(long checkpointId) throws Exception {
			numCheckpointsComplete++;
		}
	}

	/**
	 * Dimension function.
	 */
	public static class DimFunction extends TableFunction<RowData> {
		public void eval(Object... keys) throws Exception {}
	}
}
