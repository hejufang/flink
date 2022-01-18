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

import org.apache.flink.table.factories.FactoryUtil;

import java.util.List;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.SCAN_STARTUP_TIMESTAMP_MILLIS;

/**
 * TestSqlGenerator.
 */
public class TestSqlGenerator {
	private static final String TEST_CREATE_TABLE_SQL_TEMPLATE = "create table %s (\n" +
		"    uid varchar,\n" +
		"    varchar_field varchar,\n" +
		"    int_field int,\n" +
		"    bigint_field bigint\n" +
		") WITH (\n" +
		"\t'connector' = 'rocketmq',\n" +
		"%s" +
		"\t'cluster' = '%s',\n" +
		"\t'topic' = '%s',\n" +
		"\t'group' = '%s',\n" +
		"\t'scan.startup-mode' = '%s'\n," +
		"\t'format' = 'json',\n" +
		"\t'scan.use-flip27-source' = 'true',\n" +
		"\t'scan.end-offset' = '%s',\n" +
		// Add discover interval to make test faster
		"\t'scan.discover-queue-interval' = '%s s',\n" +
		"\t'scan.consumer-factory-class' = 'org.apache.flink.connector.rocketmq.TestMQConsumerStaticFactory'\n" +
		")";

	private static final String TEST_SELECT_SQL_STRING = "select\n" +
		"    uid,\n" +
		"    varchar_field,\n" +
		"    int_field,\n" +
		"    bigint_field\n" +
		"from %s";

	protected static final String TEST_UNION_SELECT_SQL_STRING = " union all select\n" +
		"    uid,\n" +
		"    varchar_field,\n" +
		"    int_field,\n" +
		"    bigint_field\n" +
		"from %s\n";

	/**
	 * SqlGeneratorConfig.
	 */
	public static class SqlGeneratorConfig {
		private final String cluster;
		private final String topic;
		private final String consumerGroup;
		private final String startMode;
		private final long finalEndOffset;
		private final long discoverMs;
		private final long startTimestamp;
		private final String tableName;
		private final int parallelism;

		public SqlGeneratorConfig(
				String cluster,
				String topic,
				String consumerGroup,
				String startMode,
				long endOffset,
				long discoverMs,
				long startTimestamp,
				String tableName,
				int parallelism) {
			this.topic = topic;
			this.cluster = cluster;
			this.startMode = startMode;
			this.consumerGroup = consumerGroup;
			this.finalEndOffset = endOffset;
			this.discoverMs = discoverMs;
			this.startTimestamp = startTimestamp;
			this.tableName = tableName;
			this.parallelism = parallelism;
		}

		public String getTopic() {
			return topic;
		}

		public String getCluster() {
			return cluster;
		}

		public String getStartMode() {
			return startMode;
		}

		public String getConsumerGroup() {
			return consumerGroup;
		}

		public long getFinalEndOffset() {
			return finalEndOffset;
		}

		public long getDiscoverMs() {
			return discoverMs;
		}

		public long getStartTimestamp() {
			return startTimestamp;
		}

		public String getTableName() {
			return tableName;
		}

		public int getParallelism() {
			return parallelism;
		}
	}

	public static String generateCreateTableSql(SqlGeneratorConfig config) {
		StringBuilder additionalArgs = new StringBuilder();
		if (config.startMode.equals(RocketMQOptions.SCAN_STARTUP_MODE_VALUE_TIMESTAMP)) {
			additionalArgs.append(String.format(
				"\t'%s' = '%s'\n,", SCAN_STARTUP_TIMESTAMP_MILLIS.key(), config.getStartTimestamp()));
		}
		if (config.getParallelism() > 0) {
			additionalArgs.append(String.format(
				"\t'%s' = '%s'\n,", FactoryUtil.PARALLELISM.key(), config.getParallelism()));
		}
		return String.format(TEST_CREATE_TABLE_SQL_TEMPLATE, config.getTableName(), additionalArgs, config.cluster,
			config.topic, config.consumerGroup, config.startMode, config.finalEndOffset, config.getDiscoverMs());
	}

	public static String getSelectSql(List<SqlGeneratorConfig> sqlGeneratorConfigs) {
		StringBuilder stringBuilder = new StringBuilder();
		stringBuilder.append(String.format(TEST_SELECT_SQL_STRING, sqlGeneratorConfigs.get(0).getTableName()));
		for (int i = 1; i < sqlGeneratorConfigs.size(); i++) {
			stringBuilder.append(String.format(TEST_UNION_SELECT_SQL_STRING, sqlGeneratorConfigs.get(1).getTableName()));
		}

		return stringBuilder.toString();
	}
}
