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

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.metric.SinkMetricsOptions;
import org.apache.flink.types.RowKind;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

/**
 * Tests for sink construct.
 */
public class ByteSQLDynamicTableSinkTest {
	private static final ByteSQLOptions.Builder optionBuilder = ByteSQLOptions.builder();
	static {
		optionBuilder
			.setConsul("dummy")
			.setDatabaseName("test")
			.setTableName("test")
			.setUsername("test")
			.setPassword("test");
	}

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testNonAppendOnlyMode() {
		ByteSQLInsertOptions.Builder insertOptionsBuilder = ByteSQLInsertOptions.builder();
		SinkMetricsOptions.Builder insertMetricsOptionsBuilder = SinkMetricsOptions.builder();
		ByteSQLDynamicTableSink sink = new ByteSQLDynamicTableSink(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			insertMetricsOptionsBuilder.build(),
			null
		);
		ChangelogMode upstreamChangeMode = ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
		thrown.expect(IllegalStateException.class);
		sink.getChangelogMode(upstreamChangeMode);
	}

	@Test
	public void testIgnoreNullMode() {
		ByteSQLInsertOptions.Builder insertOptionsBuilder = ByteSQLInsertOptions.builder();
		SinkMetricsOptions.Builder insertMetricsOptionsBuilder = SinkMetricsOptions.builder();
		insertOptionsBuilder.setIgnoreNull(true);
		ByteSQLDynamicTableSink sink = new ByteSQLDynamicTableSink(
			optionBuilder.build(),
			insertOptionsBuilder.build(),
			insertMetricsOptionsBuilder.build(),
			null
		);
		ChangelogMode upstreamChangeMode = ChangelogMode.newBuilder()
			.addContainedKind(RowKind.INSERT)
			.addContainedKind(RowKind.DELETE)
			.addContainedKind(RowKind.UPDATE_AFTER)
			.build();
		thrown.expect(IllegalStateException.class);
		sink.getChangelogMode(upstreamChangeMode);
	}
}
