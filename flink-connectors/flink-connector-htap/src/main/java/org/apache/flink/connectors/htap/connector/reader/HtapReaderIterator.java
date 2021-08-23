/*
 * Licensed serialize the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file serialize You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed serialize in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.connectors.htap.exception.HtapConnectorException;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils.FlinkAggregateFunction;
import org.apache.flink.connectors.htap.table.utils.HtapTypeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.types.Row;

import com.bytedance.htap.HtapScanner;
import com.bytedance.htap.RowResult;
import com.bytedance.htap.RowResultIterator;
import com.bytedance.htap.exception.HtapException;
import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;
import com.bytedance.htap.meta.Type;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.List;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.atomic.AtomicInteger;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * HtapReaderIterator.
 */
@Internal
public class HtapReaderIterator {

	private static final Logger LOG = LoggerFactory.getLogger(HtapReaderIterator.class);

	// default MAX_BUFFER_SIZE is 64MB
	private static final int MAX_BUFFER_SIZE = 1 << 10 << 10 << 6;

	private final HtapScanner scanner;
	private RowResultIterator currentRowIterator;
	private final List<FlinkAggregateFunction> aggregateFunctions;
	private final DataType outputDataType;
	private final int groupByColumnSize;
	private final String tableName;
	private final int partitionId;
	private long conversionCostMs = 0L;
	private int roundCount = 0;

	private final StoreScanThread storeScanThread;
	private final Queue<RowResultIterator> iteratorLinkBuffer = new ConcurrentLinkedQueue<>();
	// As ConcurrentLinkedQueue.size() is O(n), we use this to record buffered iterator count.
	private final AtomicInteger iteratorBufferCount;

	public HtapReaderIterator(
			HtapScanner scanner,
			List<FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			int groupByColumnSize) throws IOException {
		this.scanner = scanner;
		this.aggregateFunctions = checkNotNull(
				aggregateFunctions, "aggregateFunctions could not be null");
		this.outputDataType = outputDataType;
		this.groupByColumnSize = groupByColumnSize;
		this.tableName = scanner.getTable().getName();
		this.partitionId = scanner.getPartitionId();
		this.iteratorBufferCount = new AtomicInteger(0);
		this.storeScanThread = new StoreScanThread();

		try {
			storeScanThread.start();
			updateCurrentRowIterator();
		} catch (Exception e) {
			storeScanThread.close();
			throw e;
		}
	}

	public void close() {
		// TODO: HtapScanner may need a close method
		// scanner.close();
		storeScanThread.close();
	}

	public boolean hasNext() throws IOException {
		if (currentRowIterator == null) {
			return false;
		}
		if (currentRowIterator.hasNext()) {
			return true;
		} else {
			updateCurrentRowIterator();
			if (currentRowIterator == null) {
				return false;
			}
			// the next batch scan may fetch empty data, need double check here
			return currentRowIterator.hasNext();
		}
	}

	public Row next() {
		long beforeConvert = System.currentTimeMillis();
		Row flinkRow = toFlinkRow(this.currentRowIterator.next());
		long afterConvert = System.currentTimeMillis();
		conversionCostMs += (afterConvert - beforeConvert);
		return flinkRow;
	}

	// Poll new row iterator from iteratorLinkBuffer or wait for background scan thread
	// make the currentRowIterator be null if no other result for this htap scanner.
	private void updateCurrentRowIterator() throws IOException {
		LOG.debug("table: {}, partition: {}, round: {}, conversionCost: {}ms",
				tableName, partitionId, roundCount, conversionCostMs);
		conversionCostMs = 0L;
		roundCount++;
		LOG.debug("Before fetch rows from buffer {}-{} in round[{}]",
				tableName, partitionId, roundCount);

		while (storeScanThread.isRunning && iteratorLinkBuffer.isEmpty()) {
			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
		if (storeScanThread.scanException != null) {
			throw storeScanThread.scanException;
		} else {
			iteratorBufferCount.getAndDecrement();
			// if scan is finished, will poll a null element from iteratorLinkBuffer
			this.currentRowIterator = iteratorLinkBuffer.poll();
		}

		LOG.debug("After fetch rows from buffer {}-{} in round[{}]",
				tableName, partitionId, roundCount);
	}

	private Row toFlinkRow(RowResult row) {
		Schema schema = row.getColumnProjection();
		RowType rowType = outputDataType != null ? (RowType) outputDataType.getLogicalType() : null;
		int cur = 0;
		// only when aggregate pushed down, conversion in Row works
		boolean aggregatedRow = rowType != null &&
			schema.getColumnCount() == rowType.getFieldCount() &&
			schema.getColumnCount() == groupByColumnSize + aggregateFunctions.size();

		Row values = new Row(schema.getColumnCount());
		for (ColumnSchema column: schema.getColumns()) {
			String name = column.getName();
			Type type = column.getType();
			int pos = schema.getColumnIndex(name);
			Object value = row.getObject(name);
			if (value instanceof Date && type.equals(Type.DATE)) {
				value = ((Date) value).toLocalDate();
			}
			// Scenarios to cast `value` or rewrite `value` occurs when:
			// -- 1. Aggregate pushdown works.
			// -- 2. Only aggregate result columns will be affected.
			// -- 2. SUM/SUM0 aggregation with `Byte`, `Short` and `Integer` will be casted from `Long`
			//       to their original type, as HtapStore's SumAgg returns `Long` for these fields.
			// -- 3. SUM/SUM0 aggregation with `Float` will be casted from `Float` to `Double`,
			//       HtapStore's SumAgg returns `Float` for `Float` fields but BatchExecExchange
			//       requires `Double`.
			// -- 4. SUM/SUM0 aggregation with `Bigint` will be casted from `Decimal`
			//	     to their original type, as HtapStore's SumAgg returns `Decimal` for these fields.
			// -- 5. Aggregations with `Timestamp` will be casted to `LocalDateTime`,
			//       as BatchExecExchange requires.
			// -- 6. SUM0 aggregate's result will be rewrite as 0 if HtapStore returns `null`.
			if (aggregatedRow && cur++ >= groupByColumnSize) {
				LogicalType logicalType = rowType.getTypeAt(cur - 1);
				FlinkAggregateFunction aggFunction = aggregateFunctions.get(cur - groupByColumnSize - 1);
				// rewrite if aggregate function is SUM_0
				if (aggFunction == FlinkAggregateFunction.SUM_0  && value == null) {
					if (logicalType instanceof FloatType) {
						value = 0.0F;
					} else if (logicalType instanceof DoubleType) {
						value = 0.0D;
					} else if (logicalType instanceof DecimalType) {
						value = new BigDecimal(0);
					} else {
						value = 0L;
					}
				}
				try {
					// convert from HtapStore aggregate result dataType to Flink aggregate result dataType.
					if (value instanceof Long) {
						Long longValue = (Long) value;
						if (logicalType instanceof TinyIntType) {
							value = HtapTypeUtils.convertToByte(longValue);
						} else if (logicalType instanceof SmallIntType) {
							value = HtapTypeUtils.convertToShort(longValue);
						} else if (logicalType instanceof IntType) {
							value = HtapTypeUtils.convertToInt(longValue);
						}
					} else if (value instanceof Float) {
						Float floatValue = (Float) value;
						if (logicalType instanceof DoubleType) {
							value = Double.valueOf(floatValue);
						}
					} else if (value instanceof Timestamp) {
						Timestamp timestampValue = (Timestamp) value;
						if (logicalType instanceof TimestampType) {
							value = timestampValue.toLocalDateTime();
						}
					} else if (value instanceof BigDecimal) {
						if (logicalType instanceof BigIntType) {
							value = HtapTypeUtils.convertToLong((BigDecimal) value);
						}
					}
				} catch (Exception e) {
					throw new IllegalArgumentException("Conversion Error in column: " + name, e);
				}
			}
			values.setField(pos, value);
		}
		return values;
	}

	/**
	 * Each partition should have and can only have one thread scan serial in HTAP AP store.
	 */
	private class StoreScanThread extends Thread {

		public volatile boolean isRunning = true;
		public volatile IOException scanException = null;
		private volatile int scanRoundCount = 0;

		@Override
		public void run() {
			try {
				while (isRunning && scanner.hasMoreRows()) {
					if (iteratorBufferCount.get() * scanner.getBatchSizeBytes() <
							MAX_BUFFER_SIZE) {
						scanRoundCount++;
						iteratorBufferCount.getAndIncrement();
						LOG.debug("Before fetch rows from store {}-{} in round[{}]",
								tableName, partitionId, scanRoundCount);
						iteratorLinkBuffer.add(scanner.nextRows());
						LOG.debug("After fetch rows from store {}-{} in round[{}]",
								tableName, partitionId, scanRoundCount);
					} else {
						LOG.debug("buffer is full for {}-{} in round[{}] with max {} Bytes, " +
								"current buffer iterator count {}, scan batch bytes {}",
								tableName, partitionId, scanRoundCount, MAX_BUFFER_SIZE,
								iteratorBufferCount.get(), scanner.getBatchSizeBytes());
						Thread.sleep(10);
					}
				}
			} catch (Throwable t) {
				if (t instanceof HtapException) {
					HtapException htapException = (HtapException) t;
					scanException = new HtapConnectorException(
							htapException.getErrorCode(), htapException.getMessage());
				} else {
					scanException = new IOException(t);
				}
				LOG.error("scan HTAP store error", t);
			} finally {
				isRunning = false;
			}
		}

		public void close() {
			isRunning = false;
		}
	}
}
