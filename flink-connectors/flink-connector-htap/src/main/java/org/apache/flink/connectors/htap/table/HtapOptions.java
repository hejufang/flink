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

package org.apache.flink.connectors.htap.table;

import org.apache.flink.configuration.ConfigOption;

import static org.apache.flink.configuration.ConfigOptions.key;

/**
 * This class holds configuration constants used by htap connector.
 */
public class HtapOptions {
	public static final ConfigOption<Boolean> TABLE_EXEC_HTAP_INFER_SOURCE_PARALLELISM =
		key("table.exec.htap.infer-source-parallelism")
			.booleanType()
			.defaultValue(true)
			.withDescription(
				"If is false, parallelism of source are set by config.\n" +
					"If is true, source parallelism is inferred according to splits number.\n");

	public static final ConfigOption<Integer> TABLE_EXEC_HTAP_PARTITION_NUMBER_PER_SUBTASK =
		key("table.exec.htap.partition-number-per-subtask")
			.intType()
			.defaultValue(1)
			.withDescription(
				"This configs the partition number for each subtask when infer table source " +
					"parallelism with partition number. This is effective only when " +
					"'table.exec.htap.infer-source-parallelism-strategy' = 'partition-number'.");

	public static final ConfigOption<Integer> TABLE_EXEC_HTAP_ROW_NUMBER_PER_SUBTASK =
		key("table.exec.htap.row-number-per-subtask")
			.intType()
			.defaultValue(1000000)
			.withDescription(
				"This configs the partition number for each subtask when infer table source " +
					"parallelism with partition number. This is effective only when " +
					"'table.exec.htap.infer-source-parallelism-strategy' = 'row-number'");

	public static final ConfigOption<Integer> TABLE_EXEC_HTAP_INFER_SOURCE_PARALLELISM_MAX =
		key("table.exec.htap.infer-source-parallelism.max")
			.intType()
			.defaultValue(1000)
			.withDescription("Sets max infer parallelism for source operator.");

	public static final ConfigOption<String> TABLE_EXEC_HTAP_INFER_SOURCE_PARALLELISM_STRATEGY =
		key("table.exec.htap.infer-source-parallelism-strategy")
			.stringType()
			.defaultValue("partition-number")
			.withDescription(
				"Strategy for inferring parallelism for source operator. " +
					"Supported strategies: partition-number, row-number");

	public static final ConfigOption<String> HTAP_CLUSTER_NAME =
		key("htap.cluster.name")
			.stringType()
			.defaultValue("default-cluster")
			.withDescription("The name of current ByteHTAP cluster for monitoring");
}
