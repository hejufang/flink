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

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.connectors.htap.connector.executor.PrioritizedScanner;
import org.apache.flink.connectors.htap.connector.executor.ScanExecutor;
import org.apache.flink.connectors.htap.table.utils.HtapAggregateUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.types.Row;

import com.bytedance.htap.HtapScanner;
import com.bytedance.htap.RowResultIterator;
import com.bytedance.htap.exception.HtapException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * HtapThreadPoolReaderIterator use thread pool to read from storage.
 */
public class HtapThreadPoolReaderIterator extends AbstractHtapResultIterator {
	private static final Logger LOG = LoggerFactory.getLogger(HtapThreadPoolReaderIterator.class);
	private final LinkedBlockingQueue<RowResultIteratorWrapper> linkedBlockingQueue = new LinkedBlockingQueue<>();
	private final PrioritizedScanner prioritizedScanner;
	private final int scanRetryTimes;
	private final ScanExecutor scanExecutor;
	private CompletableFuture cacheNeedDataFuture;
	private AtomicBoolean isScanBlock = new AtomicBoolean(false);
	private final AtomicBoolean isFinish = new AtomicBoolean(false);
	private int scanCount = 0;
	private int retryTimes = 0;
	private volatile IOException scanException = null;

	public HtapThreadPoolReaderIterator (
			HtapScanner scanner,
			List<HtapAggregateUtils.FlinkAggregateFunction> aggregateFunctions,
			DataType outputDataType,
			int groupByColumnSize,
			String subTaskFullName,
			int maxScanThread,
			int scanRetryTimes) {
		super(scanner, aggregateFunctions, outputDataType, groupByColumnSize, subTaskFullName);
		this.scanRetryTimes = scanRetryTimes;
		cacheNeedDataFuture = new CompletableFuture<>();
		cacheNeedDataFuture.complete(null);
		prioritizedScanner = new PrioritizedScanner(this);
		scanExecutor = ScanExecutor.getInstance(maxScanThread);
		scanExecutor.enqueueScanners(prioritizedScanner);
	}

	@Override
	public CompletableFuture<Void> work() {
		try {
			long startMillis = System.currentTimeMillis();
			RowResultIterator rowResults = scanner.nextRows();
			linkedBlockingQueue.add(new RowResultIteratorWrapper(rowResults, false));
			totalSizeInBuffer.getAndAdd(rowResults.length());

			scanCount++;
			long costMillis = System.currentTimeMillis() - startMillis;
			LOG.debug("{} fetched rows:{} from store {}-{} in round[{}], cost {} ms, buffer size: {}",
				subTaskFullName, rowResults.getNumRows(), tableName,
				partitionId.getId(), scanCount, costMillis, totalSizeInBuffer.get());

			// add empty RowResultIteratorWrapper to queue to indicate scan finished
			if (!scanner.hasMoreRows()) {
				linkedBlockingQueue.add(new RowResultIteratorWrapper(rowResults, true));
				LOG.debug("{} finish fetch from store {}-{} after round[{}]",
					subTaskFullName, tableName,
					partitionId.getId(), scanCount);
				isFinish.set(true);
				return CompletableFuture.completedFuture(null);
			} else {
				return cacheNeedData();
			}
		} catch (HtapException htapException) {
			if (isRetryAble(htapException)) {
				retryTimes++;
				if (retryTimes <= scanRetryTimes) {
					LOG.error("{} scan HTAP store with HtapException, will do {} retry",
						subTaskFullName, retryTimes, htapException);
					return CompletableFuture.completedFuture(null);
				}
				LOG.error("{} scan HTAP store with HtapException, already retry {} times, stop retry and abort",
					subTaskFullName, scanRetryTimes, htapException);
			} else {
				LOG.error("{} scan HTAP store with HtapException, abort scan", subTaskFullName, htapException);
			}
			scanException = htapException;
			isFinish.set(true);
			linkedBlockingQueue.add(new RowResultIteratorWrapper(null, true));
			return CompletableFuture.completedFuture(null);
		} catch (Throwable t) {
			scanException = new IOException(t);
			isFinish.set(true);
			linkedBlockingQueue.add(new RowResultIteratorWrapper(null, true));
			LOG.error("{} scan HTAP store exception, abort scan", subTaskFullName, t);
			return CompletableFuture.completedFuture(null);
		}
	}

	@Override
	public void close() {
		// if close early, need to remove scanner from ScanExecutor
		if (!isFinish.get()) {
			scanExecutor.removeScanner(prioritizedScanner);
			isFinish.set(true);
		}
	}

	@Override
	public boolean hasNext() throws IOException {
		if (currentRowIterator != null && currentRowIterator.hasNext()) {
			return true;
		} else {
			try {
				LOG.debug("{} store {}-{} start to take a new Iterator. Queue size: {}", subTaskFullName,
					tableName, partitionId.getId(), linkedBlockingQueue.size());
				RowResultIteratorWrapper rowResultIteratorWrapper = linkedBlockingQueue.take();
				LOG.debug("{} store {}-{} took a new Iterator. IsEmpty: {}", subTaskFullName,
					tableName, partitionId.getId(), rowResultIteratorWrapper.empty());
				if (scanException != null) {
					throw scanException;
				}
				if (rowResultIteratorWrapper.empty()) {
					isFinish.set(true);
					return false;
				}
				currentRowIterator = rowResultIteratorWrapper.getRowResultIterator();
				if (currentRowIterator != null) {
					totalSizeInBuffer.getAndAdd(-1 * currentRowIterator.length());
				}
				if (isScanBlock.get() && !isCacheFull()) {
					cacheNeedDataFuture.complete(null);
					isScanBlock.set(false);
				}
				// the next batch scan may fetch empty data, need double check here
				return currentRowIterator.hasNext();
			} catch (InterruptedException e) {
				throw new IOException(e);
			}
		}
	}

	@Override
	public Row next(Row reuse) {
		return toFlinkRow(this.currentRowIterator.next(), reuse);
	}

	private CompletableFuture<Void> cacheNeedData() {
		if (isCacheFull()) {
			cacheNeedDataFuture = new CompletableFuture<>();
			LOG.debug("{} store {}-{} create new CompletableFuture because cache full",
				subTaskFullName, tableName, partitionId.getId());
			isScanBlock.set(true);
		}
		return cacheNeedDataFuture;
	}

	private boolean isCacheFull() {
		return totalSizeInBuffer.get() >= MAX_BUFFER_SIZE;
	}

	@Override
	public boolean isFinish() {
		return isFinish.get();
	}

	private boolean isRetryAble(HtapException htapException) {
		return htapException.getErrorCode() > 10000 && htapException.getErrorCode() < 20000;
	}

	@Override
	public String toString() {
		return String.format("subTask: %s, tableName:  %s, partitionId: %d",
			subTaskFullName, tableName, partitionId.getId());
	}
}
