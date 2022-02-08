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

package org.apache.flink.rocketmq.source.reader;

import org.apache.flink.api.common.state.ListState;
import org.apache.flink.api.common.state.ListStateDescriptor;
import org.apache.flink.api.common.state.OperatorStateStore;
import org.apache.flink.api.common.typeinfo.TypeHint;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.connector.base.source.reader.RecordEmitter;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.SingleThreadMultiplexSourceReaderBase;
import org.apache.flink.connector.base.source.reader.splitreader.SplitReader;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.rocketmq.source.split.RocketMQSplit;
import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;
import org.apache.flink.rocketmq.source.split.RocketMQSplitState;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.rocketmq.clientv2.message.MessageQueue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collection;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.function.Supplier;
import java.util.stream.Collectors;

import static org.apache.flink.connector.rocketmq.RocketMQOptions.OFFSETS_STATE_NAME;
import static org.apache.flink.connector.rocketmq.RocketMQUtils.hashCodeOfMessageQueue;

/**
 * The source reader for RocketMQ queues.
 * */
public class RocketMQSourceReader<OUT>
		extends SingleThreadMultiplexSourceReaderBase<
				Tuple3<OUT, Long, Long>, OUT, RocketMQSplit, RocketMQSplitState> {
	private static final Logger LOG = LoggerFactory.getLogger(RocketMQSourceReader.class);

	private transient ListState<Tuple2<MessageQueue, Long>> oldOffsetListState;

	// Offset information of all queues will keep in here.
	private transient Map<RocketMQSplitBase, Long> splitOffsetMap;
	private transient List<Tuple2<MessageQueue, Long>> lastSnapshotTupleList;
	private final String readerUid;

	// split state that assigned to this reader
	private transient Set<RocketMQSplitBase> assignedSplitsSet;

	public RocketMQSourceReader(
			FutureNotifier futureNotifier,
			FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<OUT, Long, Long>>> elementsQueue,
			Supplier<SplitReader<Tuple3<OUT, Long, Long>, RocketMQSplit>> splitFetcherSupplier,
			RecordEmitter<Tuple3<OUT, Long, Long>, OUT, RocketMQSplitState> recordEmitter,
			RocketMQConfig<OUT> config,
			SourceReaderContext context) {
		super(futureNotifier, elementsQueue, splitFetcherSupplier, recordEmitter, new Configuration(), context);
		readerUid = String.format("[SubTask: %s, cluster: %s, topic: %s, group: %s] ",
			context.getSubTaskId(), config.getCluster(), config.getTopic(), config.getGroup());
	}

	@Override
	protected void onSplitFinished(Collection<String> finishedSplitIds) {
		// do nothing
	}

	@Override
	protected RocketMQSplitState initializedState(RocketMQSplit split) {
		return (RocketMQSplitState) split;
	}

	@Override
	protected RocketMQSplit toSplitType(String splitId, RocketMQSplitState splitState) {
		return splitState.toRocketMQSplit();
	}

	@Override
	public List<RocketMQSplit> snapshotState() {
		List<RocketMQSplit> rocketMQSplits = super.snapshotState();

		Map<MessageQueue, Long> lastTuple2offsetMap = lastSnapshotTupleList.stream()
			.filter(x -> hashCodeOfMessageQueue(x.f0, context.getReaderParallelism()) == context.getSubTaskId())
			.collect(Collectors.toMap(t -> t.f0, t -> t.f1));
		for (RocketMQSplit rocketMQSplit: rocketMQSplits) {
			RocketMQSplitBase splitBase = rocketMQSplit.getRocketMQBaseSplit();
			MessageQueue messageQueue =
				new MessageQueue(splitBase.getTopic(), splitBase.getBrokerName(), splitBase.getQueueId());
			lastTuple2offsetMap.put(messageQueue, rocketMQSplit.getStartingOffset());
		}

		lastSnapshotTupleList = lastTuple2offsetMap.entrySet().stream()
			.map(t -> new Tuple2<>(t.getKey(), t.getValue())).collect(Collectors.toList());
		try {
			oldOffsetListState.update(lastSnapshotTupleList);
			LOG.info("Reader {} , lastSnapshotTupleList size {}", readerUid, lastSnapshotTupleList.size());
		} catch (Exception e) {
			throw new FlinkRuntimeException("RocketMQ source reader", e);
		}

		// We should always return empty here. Because the splits will be restored when job restarts.
		// And SourceOperator will add these splits back to Reader.
		// We don't need add split by the information in the checkpoint, all add split operation is triggerred
		// by AddSplit event.
		return Collections.emptyList();
	}

	public void initializeState(boolean isStored, OperatorStateStore operatorStateStore) throws Exception {
		LOG.info("Init {} state, isStored {}", readerUid, isStored);
		this.oldOffsetListState = operatorStateStore.getUnionListState(new ListStateDescriptor<>(
			OFFSETS_STATE_NAME, TypeInformation.of(new TypeHint<Tuple2<MessageQueue, Long>>() {})
		));

		splitOffsetMap = new HashMap<>();
		if (isStored) {
			recoverFromState();
		} else {
			lastSnapshotTupleList = new ArrayList<>();
		}

		assignedSplitsSet = new HashSet<>();
	}

	private void recoverFromState() throws Exception {
		Iterable<Tuple2<MessageQueue, Long>> oldStateIterable = oldOffsetListState.get();
		// recover from state.
		Map<MessageQueue, Long> messageQueue2OffsetMap = new HashMap<>();
		if (oldStateIterable != null) {
			oldStateIterable.forEach(
				queueAndOffset -> {
					RocketMQSplitBase splitBase = createRocketMQSplitBase(queueAndOffset.f0);
					splitOffsetMap.compute(splitBase, (mqSplitBase, offset) -> {
						if (offset != null) {
							return Math.max(queueAndOffset.f1, offset);
						}
						return queueAndOffset.f1;
					});
					messageQueue2OffsetMap.compute(queueAndOffset.f0, (mq, offset) -> {
						if (offset != null) {
							return Math.max(queueAndOffset.f1, offset);
						}
						return queueAndOffset.f1;
					});
				}
			);
		}
		LOG.info("Reader {} recover from state, size is {}", readerUid, messageQueue2OffsetMap.size());
		lastSnapshotTupleList =
			messageQueue2OffsetMap.entrySet().stream()
				.map(x -> new Tuple2<>(x.getKey(), x.getValue())).collect(Collectors.toList());
	}

	@Override
	public synchronized void addSplits(List<RocketMQSplit> splits) {
		List<RocketMQSplit> newSplits = new ArrayList<>();
		for (RocketMQSplit rocketMQSplit: splits) {
			RocketMQSplitBase splitBase = rocketMQSplit.getRocketMQBaseSplit();
			if (assignedSplitsSet.contains(splitBase)) {
				assignedSplitsSet.add(splitBase);
				LOG.warn("RocketMQ split [broker: {}, queueId: {}] already exists in {}",
					splitBase.getBrokerName(), splitBase.getQueueId(), readerUid);
			} else {
				newSplits.add(rocketMQSplit);
			}
		}

		List<RocketMQSplit> rocketMQSplits = new ArrayList<>();
		for (RocketMQSplit rocketMQSplit: newSplits) {
			RocketMQSplitBase splitBase = rocketMQSplit.getRocketMQBaseSplit();
			Long offset = splitOffsetMap.get(splitBase);
			RocketMQSplitState rocketMQSplitState = new RocketMQSplitState(rocketMQSplit, offset);
			rocketMQSplits.add(rocketMQSplitState);
			LOG.info("Reader {} find message queue[broker: {}, queueId: {}] get offset {} in checkpoint",
				readerUid, splitBase.getBrokerName(), splitBase.getQueueId(), offset);
		}
		super.addSplits(rocketMQSplits);
	}

	private RocketMQSplitBase createRocketMQSplitBase(MessageQueue messageQueue) {
		return new RocketMQSplitBase(
			messageQueue.getTopic(), messageQueue.getBrokerName(), messageQueue.getQueueId());
	}
}
