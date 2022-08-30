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

package org.apache.flink.connector.base.source.reader.mocks;

import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.mocks.MockSourceSplit;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.SimpleCounter;

import java.util.Collection;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Supplier;

/**
 * A mock SourceReader class.
 */
public class MockSourceReader
		extends SingleThreadMultiplexSourceReaderBase<int[], Integer, MockSourceSplit, AtomicInteger> {

	public MockSourceReader(FutureNotifier futureNotifier,
							FutureCompletingBlockingQueue<RecordsWithSplitIds<int[]>> elementsQueue,
							Supplier<SplitReader<int[], MockSourceSplit>> splitFetcherSupplier,
							Configuration config,
							SourceReaderContext context) {
		super(futureNotifier, elementsQueue, splitFetcherSupplier, new MockRecordEmitter(), config, context);
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {

	}

	@Override
	protected AtomicInteger initializedState(MockSourceSplit split) {
		return new AtomicInteger(split.index());
	}

	@Override
	protected Counter getRecordsOutMetric() {
		return new SimpleCounter();
	}

	@Override
	protected MockSourceSplit toSplitType(String splitId, AtomicInteger splitState) {
		return new MockSourceSplit(Integer.parseInt(splitId), splitState.get());
	}
}
