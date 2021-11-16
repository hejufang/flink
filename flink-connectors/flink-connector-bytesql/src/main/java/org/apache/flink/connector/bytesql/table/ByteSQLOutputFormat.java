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

import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.bytesql.client.ByteSQLDBBase;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.table.executor.ByteSQLBatchStatementExecutor;
import org.apache.flink.connector.bytesql.table.executor.ByteSQLSinkExecutorBuilder;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
import java.util.concurrent.ThreadLocalRandom;
import java.util.concurrent.TimeUnit;

/**
 * OutputFormat for {@link ByteSQLDynamicTableSink}.
 */
public class ByteSQLOutputFormat extends RichOutputFormat<RowData> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLOutputFormat.class);
	private static final String THREAD_POOL_NAME = "byteSQL-sink-function";

	private final ByteSQLOptions options;
	private final ByteSQLInsertOptions insertOptions;
	private final boolean isAppendOnly;
	private final FlinkConnectorRateLimiter rateLimiter;
	private final RowType rowType;

	private transient ByteSQLDBBase byteSQLDB;
	private final ByteSQLBatchStatementExecutor byteSQLSinkExecutor;

	private transient int batchCount = 0;
	private transient ScheduledExecutorService scheduler;
	private transient ScheduledFuture<?> scheduledFuture;
	private transient volatile Exception flushException;
	private transient volatile boolean closed = false;

	public ByteSQLOutputFormat(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType,
			boolean isAppendOnly) {
		this.options = options;
		this.insertOptions = insertOptions;
		this.rowType = rowType;
		this.isAppendOnly = isAppendOnly;
		this.rateLimiter = options.getRateLimiter();
		this.byteSQLSinkExecutor = ByteSQLSinkExecutorBuilder.build(
			options,
			insertOptions,
			rowType,
			isAppendOnly);
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

		try {
			byteSQLDB = (ByteSQLDBBase) Class.forName(options.getDbClassName())
				.getMethod("getInstance", ByteSQLOption.class)
				.invoke(null, byteSQLOption);
				//.getConstructor().newInstance(byteSQLOption);
		} catch (IllegalAccessException | ClassNotFoundException |
				NoSuchMethodException | InvocationTargetException e) {
			throw new IllegalStateException("Something wrong happened in initiate a ByteSQLDB", e);
		}

		if (insertOptions.getBufferFlushIntervalMills() > 0) {
			scheduler = Executors.newScheduledThreadPool(
				1, new ExecutorThreadFactory(THREAD_POOL_NAME));
			scheduledFuture = scheduler.scheduleWithFixedDelay(
				() -> {
					synchronized (ByteSQLOutputFormat.this) {
						if (closed) {
							return;
						}
						try {
							if (flushException == null) {
								flush();
							}
						} catch (Exception e) {
							flushException = e;
						}
					}
				},
				insertOptions.getBufferFlushIntervalMills(),
				insertOptions.getBufferFlushIntervalMills(),
				TimeUnit.MILLISECONDS);
		}
		if (rateLimiter != null) {
			rateLimiter.open(getRuntimeContext());
		}
	}

	@Override
	public synchronized void writeRecord(RowData record) throws IOException {
		checkFlushException();
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
		if ((record.getRowKind() == RowKind.DELETE && insertOptions.isIgnoreDelete())
			|| record.getRowKind() == RowKind.UPDATE_BEFORE) {
			return;
		}
		byteSQLSinkExecutor.addToBatch(record);
		batchCount++;
		if (batchCount >= insertOptions.getBufferFlushMaxRows()) {
			flush();
		}
	}

	private void checkFlushException() {
		if (flushException != null) {
			throw new RuntimeException("Writing records to ByteSQL failed.", flushException);
		}
	}

	public synchronized void flush() {
		checkFlushException();
		ByteSQLTransaction transaction = null;
		for (int retryTimes = 1; retryTimes <= insertOptions.getMaxRetryTimes(); retryTimes++) {
			try {
				transaction = byteSQLDB.beginTransaction();
				byteSQLSinkExecutor.executeBatch(transaction);
				batchCount = 0;
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
				// The logic checking whether to retry is recommended by ByteSQL.
				// Will be updated in the future.
				if (retryTimes >= insertOptions.getMaxRetryTimes() || e instanceof DuplicatedEntryException)  {
					String errorMsg = "Execution of ByteSQL statement failed after retries.";
					if (insertOptions.isLogFailuresOnly()) {
						LOG.error(errorMsg);
					} else {
						throw new RuntimeException(errorMsg, e);
					}
				}
				try {
					Thread.sleep(ThreadLocalRandom.current().nextInt(10) * 100L);
				} catch (InterruptedException e2) {
					throw new RuntimeException(e2);
				}
			}
		}
	}

	@Override
	public void close() throws IOException {
		if (closed) {
			return;
		}
		closed = true;
		if (scheduledFuture != null) {
			scheduledFuture.cancel(false);
			scheduler.shutdown();
		}
		if (byteSQLDB != null) {
			if (!byteSQLSinkExecutor.isBufferEmpty()) {
				flush();
			}
			byteSQLDB.close();
		}
		checkFlushException();
	}
}
