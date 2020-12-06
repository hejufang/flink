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

package org.apache.flink.connectors.bytesql;

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connectors.bytesql.utils.ByteSQLUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.table.descriptors.ByteSQLOptions;
import org.apache.flink.types.Row;

import com.bytedance.infra.bytesql4j.ByteSQLDB;
import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * OutputFormat for {@link ByteSQLUpsertTableSink}.
 */
public class ByteSQLUpsertOutputFormat extends RichOutputFormat<Tuple2<Boolean, Row>> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLUpsertOutputFormat.class);
	private static final String THREAD_POOL_NAME = "byteSQL-sink-function";

	private final ByteSQLOptions options;
	private final ByteSQLInsertOptions insertOptions;
	private final int[] pkFields;
	private final String upsertSQL;
	private final String deleteSQL;
	private final boolean isAppendOnly;

	private transient ByteSQLDB byteSQLDB;
	private transient List<Tuple2<Boolean, Row>> recordBuffer;
	private transient Map<Row, Tuple2<Boolean, Row>> keyToRows;
	private transient int batchCount = 0;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient volatile boolean closed = false;

	public ByteSQLUpsertOutputFormat(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			String[] fieldNames,
			int[] keyFieldIndices,
			boolean isAppendOnly) {
		this.options = options;
		this.insertOptions = insertOptions;
		List<String> nameList = Arrays.asList(fieldNames);
		if (!isAppendOnly) {
			this.pkFields = keyFieldIndices;
			String[] pkFieldNames = Arrays.stream(keyFieldIndices).mapToObj(nameList::get).toArray(String[]::new);
			this.deleteSQL = ByteSQLUtils.getDeleteStatement(options.getTableName(), pkFieldNames);
		} else {
			this.pkFields = null;
			this.deleteSQL = null;
		}
		this.upsertSQL = ByteSQLUtils.getUpsertStatement(options.getTableName(), fieldNames);
		this.isAppendOnly = isAppendOnly;
	}

	@Override
	public void configure(Configuration parameters) {
	}

	@Override
	public void open(int taskNumber, int numTasks) throws IOException {
		ByteSQLOption byteSQLOption = ByteSQLOption.build(
			options.getConsul(),
			options.getDatabaseName())
			.withUserName(options.getUsername())
			.withPassword(options.getPassword())
			.withRpcTimeoutMs(options.getConnectionTimeout());
		byteSQLDB = new ByteSQLDB(byteSQLOption);
		if (insertOptions.getBufferFlushIntervalMills() > 0) {
			scheduler = Executors.newScheduledThreadPool(
				1, new ExecutorThreadFactory(THREAD_POOL_NAME));
			scheduledFuture = scheduler.scheduleWithFixedDelay(
				() -> {
					synchronized (ByteSQLUpsertOutputFormat.this) {
						if (closed) {
							return;
						}
						try {
							flush();
						} catch (Exception e) {
							flushException = e;
						}
					}
				},
				insertOptions.getBufferFlushIntervalMills(),
				insertOptions.getBufferFlushIntervalMills(),
				TimeUnit.MILLISECONDS);
		}
		this.recordBuffer = new ArrayList<>();
		this.keyToRows = new HashMap<>();
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to ByteSQL failed.", flushException);
		}
	}

	@Override
	public synchronized void writeRecord(Tuple2<Boolean, Row> record) throws IOException {
		checkFlushException();
		recordBuffer.add(record);
		batchCount++;
		if (batchCount >= insertOptions.getBufferFlushMaxRows()) {
			flush();
		}
	}

	private Row getPrimaryKey(Row row) {
		if (isAppendOnly) {
			return row;
		}
		Row pks = new Row(pkFields.length);
		for (int i = 0; i < pkFields.length; i++) {
			pks.setField(i, row.getField(pkFields[i]));
		}
		return pks;
	}

	public synchronized void flush() {
		checkFlushException();
		for (int retryTimes = 1; retryTimes <= insertOptions.getMaxRetryTimes(); retryTimes++) {
			recordBuffer.forEach(tuple -> keyToRows.put(getPrimaryKey(tuple.f1), tuple));
			if (keyToRows.size() > 0) {
				ByteSQLTransaction transaction = null;
				String sql;
				try {
					transaction = byteSQLDB.beginTransaction();
					for (Map.Entry<Row, Tuple2<Boolean, Row>> entry : keyToRows.entrySet()) {
						Row pk = entry.getKey();
						Tuple2<Boolean, Row> tuple = entry.getValue();
						if (tuple.f0) {
							sql = ByteSQLUtils.generateActualSql(upsertSQL, tuple.f1);
						} else if (pkFields.length == 0){
							//Temporary fix, see INFOI-14662.
							sql = ByteSQLUtils.generateActualSql(deleteSQL, pk);
						}
						else {
							continue;
						}
						transaction.rawQuery(sql);
					}
					transaction.commit();
					keyToRows.clear();
					recordBuffer.clear();
					batchCount = 0;
					return;
				} catch (ByteSQLException e) {
					LOG.error(String.format("ByteSQL execute error, retry times = %d", retryTimes), e);
					// We should rollback explicitly as ByteSQL server is not dealing well with
					// rollback when transaction fail.
					if (transaction != null) {
						try {
							transaction.rollback();
						} catch (ByteSQLException byteSQLException) {
							LOG.info("This exception could be ignored as the server will clean up " +
								"the transaction after rollback fails.", e);
						}
					}
					if (!e.isRetryable() || retryTimes >= insertOptions.getMaxRetryTimes()) {
						throw new RuntimeException("Execution of ByteSQL statement failed.", e);
					}
					try {
						Thread.sleep(ThreadLocalRandom.current().nextInt(10) * 100L);
					} catch (InterruptedException e2) {
						throw new RuntimeException(e2);
					}
				}
			} else {
				return;
			}
		}
	}

	@Override
	public synchronized void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
		if (byteSQLDB != null) {
			if (recordBuffer != null && !recordBuffer.isEmpty()) {
				flush();
			}
			byteSQLDB.close();
		}
		checkFlushException();
	}
}
