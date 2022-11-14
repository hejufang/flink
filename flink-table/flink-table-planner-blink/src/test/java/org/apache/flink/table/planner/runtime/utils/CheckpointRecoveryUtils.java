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

package org.apache.flink.table.planner.runtime.utils;

import org.apache.flink.runtime.state.CheckpointListener;
import org.apache.flink.streaming.api.functions.source.SourceFunction;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.ScanTableSource;
import org.apache.flink.table.connector.source.SourceFunctionProvider;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.functions.TableFunction;
import org.apache.flink.table.planner.factories.TestValuesTableFactory;
import org.apache.flink.types.Row;

import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.concurrent.CountDownLatch;

/**
 * Utils for testing checkpoint recovery for SQL jobs.
 */
public class CheckpointRecoveryUtils {
	private static CountDownLatch latch;
	public static final String SOURCE_CREATION_WITHOUT_WATERMARK = "CREATE TABLE %s (\n" +
		"  `stringVal` STRING,\n" +
		"  `intVal` INT,\n" +
		"  `longVal` BIGINT,\n" +
		"  `floatVal` FLOAT,\n" +
		"  `doubleVal` DOUBLE,\n" +
		"  `tsVal` TIMESTAMP(3),\n" +
		"  `proc` as PROCTIME()\n" +
		") WITH (\n" +
		"  'connector' = 'values',\n" +
		"  'data-id' = '%s',\n" +
		"  'table-source-class' = '%s',\n" +
		"  'bounded' = '%s',\n" +
		"  'changelog-mode' = '%s'\n" +
		")";
	public static final String SOURCE_CREATION_WITH_WATERMARK = "CREATE TABLE %s (\n" +
		"  `stringVal` STRING,\n" +
		"  `intVal` INT,\n" +
		"  `longVal` BIGINT,\n" +
		"  `floatVal` FLOAT,\n" +
		"  `doubleVal` DOUBLE,\n" +
		"  `tsVal` TIMESTAMP(3),\n" +
		"  `proc` as PROCTIME(),\n" +
		"  WATERMARK FOR tsVal AS tsVal - INTERVAL '5' SECOND\n" +
		") WITH (\n" +
		"  'connector' = 'values',\n" +
		"  'data-id' = '%s',\n" +
		"  'table-source-class' = '%s',\n" +
		"  'bounded' = '%s'\n" +
		")";

	public static void setLatch(CountDownLatch latch) {
		CheckpointRecoveryUtils.latch = latch;
	}

	public static CountDownLatch getLatch() {
		return latch;
	}

	/**
	 * FiniteScanTableSource.
	 */
	public static class FiniteScanTableSource implements ScanTableSource,
		TestValuesTableFactory.TableSourceWithData {
		private List<Row> data;
		private ChangelogMode changelogMode;
		private boolean isBounded;
		@Override
		public ChangelogMode getChangelogMode() {
			return changelogMode;
		}

		@Override
		public ScanRuntimeProvider getScanRuntimeProvider(ScanContext runtimeProviderContext) {
			return SourceFunctionProvider.of(new FiniteTestSource(data, isBounded), false);
		}

		@Override
		public DynamicTableSource copy() {
			FiniteScanTableSource source = new FiniteScanTableSource();
			source.setTableSourceData(data);
			source.setChangelog(changelogMode);
			source.setBounded(isBounded);
			return source;
		}

		@Override
		public String asSummaryString() {
			return null;
		}

		@Override
		public void setTableSourceData(Collection<Row> rows) {
			this.data = new ArrayList<>(rows);
		}

		@Override
		public void setChangelog(ChangelogMode changelogMode) {
			this.changelogMode = changelogMode;
		}

		@Override
		public void setBounded(boolean isBounded) {
			this.isBounded = isBounded;
		}
	}

	/**
	 * FiniteTestSource.
	 */
	public static class FiniteTestSource implements SourceFunction<RowData>, CheckpointListener {
		private static final long serialVersionUID = 1L;
		private final List<Row> elements;
		private final boolean isBounded;
		private transient int numCheckpointsComplete;
		private volatile boolean running = true;

		public FiniteTestSource(Iterable<Row> elements, boolean isBounded) {
			this.elements = new ArrayList<>();
			elements.forEach(this.elements::add);
			this.isBounded = isBounded;
		}

		private RowData row2GenericRowData(Row row) {
			GenericRowData rowData = new GenericRowData(row.getArity());
			for (int i = 0; i < row.getArity(); i++) {
				Object field = row.getField(i);
				if (field instanceof String) {
					rowData.setField(i, StringData.fromString((String) field));
				} else if (field instanceof LocalDateTime) {
					rowData.setField(i, TimestampData.fromLocalDateTime((LocalDateTime) field));
				} else {
					rowData.setField(i, field);
				}
			}
			return rowData;
		}

		@Override
		public void run(SourceContext<RowData> ctx) throws Exception {
			final Object lock = ctx.getCheckpointLock();
			for (Row t : elements) {
				ctx.collect(row2GenericRowData(t));
				latch.countDown();
			}

			if (isBounded) {
				return;
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
	 * Mock dimension function.
	 */
	public static class MockDimFunction extends TableFunction<RowData> {
		public void eval(Object... keys) throws Exception {
			// Just for tests.
			// Table schema is `id` INT, `name` STRING, `age` INT, `ts` TIMESTAMP(3)
			if ((Integer) keys[0] > 2) {
				collect(GenericRowData.of(keys[0], StringData.fromString("Test"),
					1, TimestampData.fromEpochMillis(122)));
			}
		}
	}
}
