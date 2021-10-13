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

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.RichOutputFormat;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.runtime.util.ExecutorThreadFactory;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.infra.bytesql4j.ByteSQLDB;
import com.bytedance.infra.bytesql4j.ByteSQLOption;
import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;
import com.bytedance.infra.bytesql4j.exception.DuplicatedEntryException;
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
import java.util.stream.IntStream;

/**
 * OutputFormat for {@link ByteSQLDynamicTableSink}.
 */
public class ByteSQLOutputFormat extends RichOutputFormat<RowData> {
	private static final long serialVersionUID = 1L;
	private static final Logger LOG = LoggerFactory.getLogger(ByteSQLOutputFormat.class);
	private static final String THREAD_POOL_NAME = "byteSQL-sink-function";

	private final ByteSQLOptions options;
	private final ByteSQLInsertOptions insertOptions;
	private final int[] pkFields;
	private final String upsertSQL;
	private final String deleteSQL;
	private final boolean isAppendOnly;
	private final String[] fieldNames;
	private final FieldGetter[] fieldGetters;
	private final FlinkConnectorRateLimiter rateLimiter;

	private transient ByteSQLDB byteSQLDB;
	private transient List<RowData> recordBuffer;
	/**
	 * Delete/retract messages cannot be merged with insert messages if null values are set to be ignore.
	 * Therefore we use a tuple as the value of map.
	 * f0 of the tuple is a flag that means a delete should be executed before the value f1 (if exist) being insert.
	 * f1 of the tuple is the value that will be inserted if it's not null.
	 * For detail for setting the tuple, see {@link #addRow}.
	 */
	private transient Map<RowData, Tuple2<Boolean, RowData>> keyToRows;
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
		List<String> nameList = rowType.getFieldNames();
		if (!isAppendOnly) {
			String[] pkFieldNames = insertOptions.getKeyFields();
			this.pkFields = Arrays.stream(pkFieldNames).mapToInt(nameList::indexOf).toArray();
			this.deleteSQL = ByteSQLUtils.getDeleteStatement(options.getTableName(), pkFieldNames);
		} else {
			this.pkFields = null;
			this.deleteSQL = null;
		}
		this.fieldNames = nameList.toArray(new String[]{});
		this.upsertSQL = ByteSQLUtils.getUpsertStatement(
			options.getTableName(), fieldNames, insertOptions.getTtlSeconds());
		this.isAppendOnly = isAppendOnly;
		this.fieldGetters = IntStream
			.range(0, fieldNames.length)
			.mapToObj(pos -> RowData.createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
		this.rateLimiter = options.getRateLimiter();
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
		byteSQLDB = ByteSQLDB.newInstance(byteSQLOption);
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
		this.recordBuffer = new ArrayList<>();
		this.keyToRows = new HashMap<>();
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
		recordBuffer.add(record);
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
		for (int retryTimes = 1; retryTimes <= insertOptions.getMaxRetryTimes(); retryTimes++) {
			keyToRows.clear();
			recordBuffer.forEach(addRow(keyToRows));
			if (keyToRows.size() > 0) {
				ByteSQLTransaction transaction = null;
				String sql = "undefined";
				try {
					transaction = byteSQLDB.beginTransaction();
					for (Map.Entry<RowData, Tuple2<Boolean, RowData>> entry : keyToRows.entrySet()) {
						RowData pk = entry.getKey();
						Tuple2<Boolean, RowData> tuple = entry.getValue();
						if (tuple.f0) {
							sql = ByteSQLUtils.generateActualSql(deleteSQL, pk, fieldGetters);
							transaction.rawQuery(sql);
						}
						if (tuple.f1 != null) {
							if (insertOptions.isIgnoreNull()) {
								sql = generateUpsertSQLWithoutNull(tuple.f1);
							} else {
								sql = ByteSQLUtils.generateActualSql(upsertSQL, tuple.f1, fieldGetters);
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
					// The logic checking whether to retry is recommended by ByteSQL.
					// Will be updated in the future.
					if (retryTimes >= insertOptions.getMaxRetryTimes() || e instanceof DuplicatedEntryException)  {
						String errorMsg = String.format("Execution of ByteSQL statement " +
							"failed after retry. The sql is %s.", sql);
						if (insertOptions.isLogFailuresOnly()) {
							LOG.error(errorMsg);
						}
						throw new RuntimeException(errorMsg, e);
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

	private RowData getPrimaryKey(RowData row) {
		if (isAppendOnly) {
			return row;
		}
		GenericRowData pks = new GenericRowData(pkFields.length);
		for (int i = 0; i < pkFields.length; i++) {
			pks.setField(i, fieldGetters[pkFields[i]].getFieldOrNull(row));
		}
		return pks;
	}

	/**
	 * Generate a sql that ignore the null values.
	 */
	@VisibleForTesting
	protected String generateUpsertSQLWithoutNull(RowData row) throws ByteSQLException {
		List<Object> fields = new ArrayList<>(row.getArity());
		List<String> fieldNameList = new ArrayList<>(row.getArity());
		int i = 0;
		while (i < row.getArity()) {
			if (!row.isNullAt(i)) {
				fields.add(fieldGetters[i].getFieldOrNull(row));
				fieldNameList.add(fieldNames[i]);
			}
			i++;
		}
		String upsertFormatter = ByteSQLUtils
			.getUpsertStatement(
				options.getTableName(), fieldNameList.toArray(new String[]{}), insertOptions.getTtlSeconds());
		return ByteSQLUtils.generateActualSql(upsertFormatter, GenericRowData.of(fields.toArray()), fieldGetters);
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
	protected Consumer<RowData> addRow(Map<RowData, Tuple2<Boolean, RowData>> keyToRows) {
		if (insertOptions.isIgnoreNull()) {
			return (record) -> {
				RowData primaryKey = getPrimaryKey(record);
				if (record.getRowKind() == RowKind.DELETE) {
					//if a newly coming message is delete, then ignore all former operation.
					keyToRows.put(primaryKey, new Tuple2<>(true, null));
				} else if (keyToRows.containsKey(primaryKey)) {
					Tuple2<Boolean, RowData> value = keyToRows.get(primaryKey);
					value.f1 = mergeRow(record, value.f1);
				} else {
					keyToRows.put(primaryKey, new Tuple2<>(false, record));
				}
			};
		} else {
			return (record) -> keyToRows.put(getPrimaryKey(record), getKeyToRowsValue(record));
		}
	}

	/**
	 * Merge two to-update row to one row. For each field, just get last not null value of two
	 * rows. If some values of both rows are null, then the merged values are null too.
	 * The oldRow can be a null row. Meanwhile, the newRow won't be null as only incoming
	 * not null rows will need this function.
	 */
	@VisibleForTesting
	protected RowData mergeRow(RowData newRow, RowData oldRow) {
		if (oldRow == null){
			return newRow;
		} else {
			GenericRowData merged = new GenericRowData(newRow.getArity());
			int i = 0;
			RowData select;
			while (i < newRow.getArity()) {
				select = newRow.isNullAt(i) ? oldRow : newRow;
				merged.setField(i, this.fieldGetters[i].getFieldOrNull(select));
				i++;
			}
			merged.setRowKind(newRow.getRowKind());
			return merged;
		}
	}

	/**
	 * Transform a incoming message to tuple for {@link #keyToRows}.
	 */
	@VisibleForTesting
	protected static Tuple2<Boolean, RowData> getKeyToRowsValue(RowData row) {
		if (row.getRowKind() != RowKind.DELETE) {
			return new Tuple2<>(false, row);
		} else {
			return new Tuple2<>(true, null);
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
			if (recordBuffer != null && !recordBuffer.isEmpty()) {
				flush();
			}
			byteSQLDB.close();
		}
		checkFlushException();
	}
}
