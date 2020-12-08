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

import org.apache.flink.annotation.VisibleForTesting;
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
import org.apache.commons.lang3.ObjectUtils;
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
import java.util.function.Consumer;

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
	private final String[] fieldNames;

	private transient ByteSQLDB byteSQLDB;
	private transient List<Tuple2<Boolean, Row>> recordBuffer;
	/**
	 * Delete/retract messages cannot be merged with insert messages if null values are set to be ignore.
	 * Therefore we use a tuple as the value of map.
	 * f0 of the tuple is a flag that means a delete should be executed before the value f1 (if exist) being insert.
	 * f1 of the tuple is the value that will be inserted if it's not null.
	 * For detail for setting the tuple, see {@link #addRow}.
	 */
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
		this.fieldNames = fieldNames;
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
			recordBuffer.forEach(addRow(keyToRows));
			if (keyToRows.size() > 0) {
				ByteSQLTransaction transaction = null;
				String sql = "undefined";
				try {
					transaction = byteSQLDB.beginTransaction();
					for (Map.Entry<Row, Tuple2<Boolean, Row>> entry : keyToRows.entrySet()) {
						Row pk = entry.getKey();
						Tuple2<Boolean, Row> tuple = entry.getValue();
						if (tuple.f0) {
							if (pkFields.length == 0) {
								//Temporary fix, see INFOI-14662.
								sql = ByteSQLUtils.generateActualSql(deleteSQL, pk);
								transaction.rawQuery(sql);
							}
						}
						if (tuple.f1 != null) {
							if (insertOptions.isIgnoreNull()) {
								sql = generateUpsertSQLWithoutNull(tuple.f1);
							} else {
								sql = ByteSQLUtils.generateActualSql(upsertSQL, tuple.f1);
							}
							transaction.rawQuery(sql);
						}
					}
					transaction.commit();
					keyToRows.clear();
					recordBuffer.clear();
					batchCount = 0;
					sql = "undefined";
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
						throw new RuntimeException(String.format("Execution of ByteSQL statement " +
							"failed. The sql is %s.", sql), e);
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

	/**
	 * Generate a sql that ignore the null values.
	 */
	@VisibleForTesting
	protected String generateUpsertSQLWithoutNull(Row row) throws ByteSQLException {
		List<Object> fields = new ArrayList<>(row.getArity());
		List<String> fieldNameList = new ArrayList<>(row.getArity());
		int i = 0;
		while (i < row.getArity()) {
			if (row.getField(i) != null) {
				fields.add(row.getField(i));
				fieldNameList.add(fieldNames[i]);
			}
			i++;
		}
		String upsertFormatter = ByteSQLUtils
			.getUpsertStatement(options.getTableName(), fieldNameList.toArray(new String[]{}));
		return ByteSQLUtils.generateActualSql(upsertFormatter, Row.of(fields.toArray()));
	}

	/**
	 * Update the keyToRow map.
	 * For a given key, a initial tuple will depend on the first incoming message:
	 * 1. delete -> tuple(true, null);
	 * 2. update -> tuple(false, update row);
	 * After initialization, per each message coming, the tuple will be update:
	 * 1. tuple(true, null), delete -> tuple(true, null);
	 * 2. tuple(true, null), update -> tuple(true, update row);
	 * 3. tuple(false, update row), delete -> tuple(true, null);
	 * 4. tuple(false, update row), update -> tuple(false, update row(merged if null is ignored));
	 * 5. tuple(true, update row), delete -> tuple(true, null);
	 * 6. tuple(true, update row), update ->  tuple(true, update row(merged if null is ignored)).
	 */
	@VisibleForTesting
	protected Consumer<Tuple2<Boolean, Row>> addRow(Map<Row, Tuple2<Boolean, Row>> keyToRows) {
		if (insertOptions.isIgnoreNull()) {
			return (tuple) -> {
				Row primaryKey = getPrimaryKey(tuple.f1);
				if (!tuple.f0) {
					//if a newly coming message is delete, then ignore all former operation.
					keyToRows.put(primaryKey, new Tuple2<>(true, null));
				} else if (keyToRows.containsKey(primaryKey)) {
					Tuple2<Boolean, Row> value = keyToRows.get(primaryKey);
					value.f1 = mergeRow(tuple.f1, value.f1);
				} else {
					keyToRows.put(primaryKey, new Tuple2<>(false, tuple.f1));
				}
			};
		} else {
			return (tuple) -> keyToRows.put(getPrimaryKey(tuple.f1), getKeyToRowsValue(tuple));
		}
	}

	/**
	 * Merge two to-update row to one row. For each field, just get last not null value of two
	 * rows. If some values of both rows are null, then the merged values are null too.
	 * The oldRow can be a null row. Meanwhile, the newRow won't be null as only incoming
	 * not null rows will need this function.
	 */
	@VisibleForTesting
	protected static Row mergeRow(Row newRow, Row oldRow) {
		if (oldRow == null){
			return newRow;
		} else {
			Object[] mergedRowValues = new Object[newRow.getArity()];
			int i = 0;
			while (i < newRow.getArity()) {
				mergedRowValues[i] = ObjectUtils.firstNonNull(newRow.getField(i), oldRow.getField(i));
				i++;
			}
			return Row.of(mergedRowValues);
		}
	}

	/**
	 * Transform a incoming message to tuple for {@link #keyToRows}.
	 */
	private Tuple2<Boolean, Row> getKeyToRowsValue(Tuple2<Boolean, Row> row) {
		if (row.f0) {
			row.f0 = false;
		} else {
			row.f1 = null;
		}
		return row;
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
