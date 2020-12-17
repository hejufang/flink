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

package org.apache.flink.streaming.connectors.kafka.internal;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.streaming.connectors.kafka.internals.ClosableBlockingQueue;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaCommitCallback;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionState;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaTopicPartitionStateSentinel;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.clients.consumer.OffsetCommitCallback;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.slf4j.Logger;

import javax.annotation.Nonnull;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.atomic.AtomicReference;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The thread the runs the {@link KafkaConsumer}, connecting to the brokers and polling records.
 * The thread pushes the data into a {@link Handover} to be picked up by the fetcher that will
 * deserialize and emit the records.
 *
 * <p><b>IMPORTANT:</b> This thread must not be interrupted when attempting to shut it down.
 * The Kafka consumer code was found to not always handle interrupts well, and to even
 * deadlock in certain situations.
 *
 * <p>Implementation Note: This code is written to be reusable in later versions of the KafkaConsumer.
 * Because Kafka is not maintaining binary compatibility, we use a "call bridge" as an indirection
 * to the KafkaConsumer calls that change signature.
 */
@Internal
public class KafkaConsumerThread extends Thread {

	/** Logger for this consumer. */
	private final Logger log;

	/** The handover of data and exceptions between the consumer thread and the task thread. */
	private final Handover handover;

	/** The next offsets that the main thread should commit and the commit callback. */
	private final AtomicReference<Tuple2<Map<TopicPartition, OffsetAndMetadata>, KafkaCommitCallback>> nextOffsetsToCommit;

	/** The configuration for the Kafka consumer. */
	private final Properties kafkaProperties;

	/** The queue of unassigned partitions that we need to assign to the Kafka consumer. */
	private final ClosableBlockingQueue<KafkaTopicPartitionState<TopicPartition>> unassignedPartitionsQueue;

	/** The indirections on KafkaConsumer methods, for cases where KafkaConsumer compatibility is broken. */
	private final KafkaConsumerCallBridge09 consumerCallBridge;

	/** The maximum number of milliseconds to wait for a fetch batch. */
	private final long pollTimeout;

	/** Flag whether to add Kafka's metrics to the Flink metrics. */
	private final boolean useMetrics;

	/**
	 * @deprecated We should only be publishing to the {{@link #consumerMetricGroup}}.
	 *             This is kept to retain compatibility for metrics.
	 **/
	@Deprecated
	private final MetricGroup subtaskMetricGroup;

	/** We get this from the outside to publish metrics. */
	private final MetricGroup consumerMetricGroup;

	/** Reference to the Kafka consumer, once it is created. */
	private volatile KafkaConsumer<byte[], byte[]> consumer;

	/** This lock is used to isolate the consumer for partition reassignment. */
	private final Object consumerReassignmentLock;

	/** Indication if this consumer has any assigned partition. */
	private boolean hasAssignedPartitions;

	/**
	 * Flag to indicate whether an external operation ({@link #setOffsetsToCommit(Map, KafkaCommitCallback)}
	 * or {@link #shutdown()}) had attempted to wakeup the consumer while it was isolated for partition reassignment.
	 */
	private volatile boolean hasBufferedWakeup;

	/** Flag to mark the main work loop as alive. */
	private volatile boolean running;

	/** Flag tracking whether the latest commit request has completed. */
	private volatile boolean commitInProgress;

	/** Ratelimiter. */
	private FlinkConnectorRateLimiter rateLimiter;

	/** Rate limiter unit, supported unit: BYTE(default), RECORD. */
	private RateLimitingUnit rateLimitingUnit = RateLimitingUnit.BYTE;

	/** Number of records between two sampling operations.*/
	private final long sampleInterval;

	/** Number of records that one sampling operation produces.*/
	private final long sampleNum;

	/** One sample segment is composed of one sampling interval and one sampling operation.
	 * The length of sample segment is sum of {{@link #sampleInterval}} and {{@link #sampleNum}}.*/
	private final long sampleSegmentLen;

	/**
	 * A list of partitions which are temporarily suspended because their latest offsets
	 * haven't reached the sample interval, from being consumed by the thread. They will
	 * be resumed before next poll.
	 */
	private final List<TopicPartition> suspendPartitions;

	/**
	 * The keys are partitions that need to be consumed next turn, because last poll
	 * didn't cover the requested sample amounts. The values are the amounts to be consumed.
	 */
	private final Map<TopicPartition, Long> toConsumeMap;

	/**
	 * The keys are all partitions subscribed to this consumer, the values are offsets that are
	 * processed last poll. Because the commit operations cannot be called immediately after poll,
	 * we cannot use last committed offsets.
	 */
	private final Map<TopicPartition, Long> lastProcessedMap;

	public KafkaConsumerThread(
			Logger log,
			Handover handover,
			Properties kafkaProperties,
			ClosableBlockingQueue<KafkaTopicPartitionState<TopicPartition>> unassignedPartitionsQueue,
			KafkaConsumerCallBridge09 consumerCallBridge,
			String threadName,
			long pollTimeout,
			boolean useMetrics,
			MetricGroup consumerMetricGroup,
			MetricGroup subtaskMetricGroup,
			FlinkConnectorRateLimiter rateLimiter,
			RateLimitingUnit rateLimitingUnit,
			long sampleInterval,
			long sampleNum) {

		super(threadName);
		setDaemon(true);

		this.log = checkNotNull(log);
		this.handover = checkNotNull(handover);
		this.kafkaProperties = checkNotNull(kafkaProperties);
		this.consumerMetricGroup = checkNotNull(consumerMetricGroup);
		this.subtaskMetricGroup = checkNotNull(subtaskMetricGroup);
		this.consumerCallBridge = checkNotNull(consumerCallBridge);

		this.unassignedPartitionsQueue = checkNotNull(unassignedPartitionsQueue);

		this.pollTimeout = pollTimeout;
		this.useMetrics = useMetrics;

		this.consumerReassignmentLock = new Object();
		this.nextOffsetsToCommit = new AtomicReference<>();
		this.running = true;

		if (rateLimiter != null) {
			this.rateLimiter = rateLimiter;
			log.info("limiting rate: {}", this.rateLimiter.getRate());
		}

		if (rateLimitingUnit != null) {
			this.rateLimitingUnit = rateLimitingUnit;
		}
		log.info("rateLimitingUnit: {}", this.rateLimitingUnit);

		this.sampleInterval = sampleInterval;
		this.sampleNum = sampleNum;
		this.sampleSegmentLen = sampleInterval + sampleNum;
		this.suspendPartitions = new ArrayList<>();
		this.toConsumeMap = new HashMap<>();
		this.lastProcessedMap = new HashMap<>();
	}

	public KafkaConsumerThread(
			Logger log,
			Handover handover,
			Properties kafkaProperties,
			ClosableBlockingQueue<KafkaTopicPartitionState<TopicPartition>> unassignedPartitionsQueue,
			KafkaConsumerCallBridge09 consumerCallBridge,
			String threadName,
			long pollTimeout,
			boolean useMetrics,
			MetricGroup consumerMetricGroup,
			MetricGroup subtaskMetricGroup,
			FlinkConnectorRateLimiter rateLimiter,
			RateLimitingUnit rateLimitingUnit) {
		this(
			log,
			handover,
			kafkaProperties,
			unassignedPartitionsQueue,
			consumerCallBridge,
			threadName,
			pollTimeout,
			useMetrics,
			consumerMetricGroup,
			subtaskMetricGroup,
			rateLimiter,
			rateLimitingUnit,
			0,
			1
		);
	}

	private void initProcessedMap() {
		Set<TopicPartition> toProcessPartitions = consumer.assignment();
		for (TopicPartition partition : toProcessPartitions) {
			lastProcessedMap.putIfAbsent(partition, consumer.position(partition));
		}
		if (toProcessPartitions.size() != lastProcessedMap.size()) {
			lastProcessedMap.keySet().removeIf(partition -> !toProcessPartitions.contains(partition));
		}
	}

	// ------------------------------------------------------------------------

	@Override
	public void run() {
		// early exit check
		if (!running) {
			return;
		}

		// this is the means to talk to FlinkKafkaConsumer's main thread
		final Handover handover = this.handover;

		// This method initializes the KafkaConsumer and guarantees it is torn down properly.
		// This is important, because the consumer has multi-threading issues,
		// including concurrent 'close()' calls.
		try {
			this.consumer = getConsumer(kafkaProperties);
		}
		catch (Throwable t) {
			handover.reportError(t);
			return;
		}

		// from here on, the consumer is guaranteed to be closed properly
		try {
			// register Kafka's very own metrics in Flink's metric reporters
			if (useMetrics) {
				// register Kafka metrics to Flink
				Map<MetricName, ? extends Metric> metrics = consumer.metrics();
				if (metrics == null) {
					// MapR's Kafka implementation returns null here.
					log.info("Consumer implementation does not support metrics");
				} else {
					// we have Kafka metrics, register them
					for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
						consumerMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));

						// TODO this metric is kept for compatibility purposes; should remove in the future
						subtaskMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
					}
				}
			}

			// early exit check
			if (!running) {
				return;
			}

			// the latest bulk of records. May carry across the loop if the thread is woken up
			// from blocking on the handover
			ConsumerRecords<byte[], byte[]> records = null;

			// reused variable to hold found unassigned new partitions.
			// found partitions are not carried across loops using this variable;
			// they are carried across via re-adding them to the unassigned partitions queue
			List<KafkaTopicPartitionState<TopicPartition>> newPartitions;

			// main fetch loop
			while (running) {

				// check if there is something to commit
				if (!commitInProgress) {
					// get and reset the work-to-be committed, so we don't repeatedly commit the same
					final Tuple2<Map<TopicPartition, OffsetAndMetadata>, KafkaCommitCallback> commitOffsetsAndCallback =
							nextOffsetsToCommit.getAndSet(null);

					if (commitOffsetsAndCallback != null) {
						log.debug("Sending async offset commit request to Kafka broker");

						// also record that a commit is already in progress
						// the order here matters! first set the flag, then send the commit command.
						commitInProgress = true;
						consumer.commitAsync(commitOffsetsAndCallback.f0, new CommitCallback(commitOffsetsAndCallback.f1));
					}
				}

				try {
					if (hasAssignedPartitions) {
						newPartitions = unassignedPartitionsQueue.pollBatch();
					} else {
						// if no assigned partitions block until we get at least one
						// instead of hot spinning this loop. We rely on a fact that
						// unassignedPartitionsQueue will be closed on a shutdown, so
						// we don't block indefinitely
						newPartitions = unassignedPartitionsQueue.getBatchBlocking();
					}
					if (newPartitions != null) {
						reassignPartitions(newPartitions);
						if (sampleInterval > 0){
							initProcessedMap();
						}
					}
				} catch (AbortedReassignmentException e) {
					continue;
				}

				if (!hasAssignedPartitions) {
					// Without assigned partitions KafkaConsumer.poll will throw an exception
					continue;
				}

				// get the next batch of records, unless we did not manage to hand the old batch over
				if (records == null) {
					try {
						records = (sampleInterval > 0) ? getSampledRecordsAfterInterval() : getRecordsFromKafka();
					}
					catch (WakeupException we) {
						continue;
					}
				}

				try {
					handover.produce(records);
					records = null;
				}
				catch (Handover.WakeupException e) {
					// fall through the loop
				}
			}
			// end main fetch loop
		}
		catch (Throwable t) {
			// let the main thread know and exit
			// it may be that this exception comes because the main thread closed the handover, in
			// which case the below reporting is irrelevant, but does not hurt either
			handover.reportError(t);
		}
		finally {
			// make sure the handover is closed if it is not already closed or has an error
			handover.close();

			// If a ratelimiter was created, make sure it's closed.
			if (rateLimiter != null) {
				rateLimiter.close();
			}

			// make sure the KafkaConsumer is closed
			try {
				consumer.close();
			}
			catch (Throwable t) {
				log.warn("Error while closing Kafka consumer", t);
			}
		}
	}

	/**
	 * Shuts this thread down, waking up the thread gracefully if blocked (without Thread.interrupt() calls).
	 */
	public void shutdown() {
		running = false;

		// wake up all blocking calls on the queue
		unassignedPartitionsQueue.close();

		// We cannot call close() on the KafkaConsumer, because it will actually throw
		// an exception if a concurrent call is in progress

		// this wakes up the consumer if it is blocked handing over records
		handover.wakeupProducer();

		// this wakes up the consumer if it is blocked in a kafka poll
		synchronized (consumerReassignmentLock) {
			if (consumer != null) {
				consumer.wakeup();
			} else {
				// the consumer is currently isolated for partition reassignment;
				// set this flag so that the wakeup state is restored once the reassignment is complete
				hasBufferedWakeup = true;
			}
		}

		// If a ratelimiter was created, make sure it's closed.
		if (rateLimiter != null) {
			rateLimiter.close();
		}
	}

	/**
	 * Tells this thread to commit a set of offsets. This method does not block, the committing
	 * operation will happen asynchronously.
	 *
	 * <p>Only one commit operation may be pending at any time. If the committing takes longer than
	 * the frequency with which this method is called, then some commits may be skipped due to being
	 * superseded by newer ones.
	 *
	 * @param offsetsToCommit The offsets to commit
	 * @param commitCallback callback when Kafka commit completes
	 */
	void setOffsetsToCommit(
			Map<TopicPartition, OffsetAndMetadata> offsetsToCommit,
			@Nonnull KafkaCommitCallback commitCallback) {

		// record the work to be committed by the main consumer thread and make sure the consumer notices that
		if (nextOffsetsToCommit.getAndSet(Tuple2.of(offsetsToCommit, commitCallback)) != null) {
			log.warn("Committing offsets to Kafka takes longer than the checkpoint interval. " +
					"Skipping commit of previous offsets because newer complete checkpoint offsets are available. " +
					"This does not compromise Flink's checkpoint integrity.");
		}

		// if the consumer is blocked in a poll() or handover operation, wake it up to commit soon
		handover.wakeupProducer();

		synchronized (consumerReassignmentLock) {
			if (consumer != null) {
				consumer.wakeup();
			} else {
				// the consumer is currently isolated for partition reassignment;
				// set this flag so that the wakeup state is restored once the reassignment is complete
				hasBufferedWakeup = true;
			}
		}
	}

	// ------------------------------------------------------------------------

	/**
	 * Reestablishes the assigned partitions for the consumer.
	 * The reassigned partitions consists of the provided new partitions and whatever partitions
	 * was already previously assigned to the consumer.
	 *
	 * <p>The reassignment process is protected against wakeup calls, so that after
	 * this method returns, the consumer is either untouched or completely reassigned
	 * with the correct offset positions.
	 *
	 * <p>If the consumer was already woken-up prior to a reassignment resulting in an
	 * interruption any time during the reassignment, the consumer is guaranteed
	 * to roll back as if it was untouched. On the other hand, if there was an attempt
	 * to wakeup the consumer during the reassignment, the wakeup call is "buffered"
	 * until the reassignment completes.
	 *
	 * <p>This method is exposed for testing purposes.
	 */
	@VisibleForTesting
	void reassignPartitions(List<KafkaTopicPartitionState<TopicPartition>> newPartitions) throws Exception {
		if (newPartitions.size() == 0) {
			return;
		}
		hasAssignedPartitions = true;
		boolean reassignmentStarted = false;

		// since the reassignment may introduce several Kafka blocking calls that cannot be interrupted,
		// the consumer needs to be isolated from external wakeup calls in setOffsetsToCommit() and shutdown()
		// until the reassignment is complete.
		final KafkaConsumer<byte[], byte[]> consumerTmp;
		synchronized (consumerReassignmentLock) {
			consumerTmp = this.consumer;
			this.consumer = null;
		}

		final Map<TopicPartition, Long> oldPartitionAssignmentsToPosition = new HashMap<>();
		try {
			for (TopicPartition oldPartition : consumerTmp.assignment()) {
				oldPartitionAssignmentsToPosition.put(oldPartition, consumerTmp.position(oldPartition));
			}

			final List<TopicPartition> newPartitionAssignments =
				new ArrayList<>(newPartitions.size() + oldPartitionAssignmentsToPosition.size());
			newPartitionAssignments.addAll(oldPartitionAssignmentsToPosition.keySet());
			newPartitionAssignments.addAll(convertKafkaPartitions(newPartitions));

			// reassign with the new partitions
			consumerCallBridge.assignPartitions(consumerTmp, newPartitionAssignments);
			reassignmentStarted = true;

			// old partitions should be seeked to their previous position
			for (Map.Entry<TopicPartition, Long> oldPartitionToPosition : oldPartitionAssignmentsToPosition.entrySet()) {
				consumerTmp.seek(oldPartitionToPosition.getKey(), oldPartitionToPosition.getValue());
			}

			// offsets in the state of new partitions may still be placeholder sentinel values if we are:
			//   (1) starting fresh,
			//   (2) checkpoint / savepoint state we were restored with had not completely
			//       been replaced with actual offset values yet, or
			//   (3) the partition was newly discovered after startup;
			// replace those with actual offsets, according to what the sentinel value represent.
			for (KafkaTopicPartitionState<TopicPartition> newPartitionState : newPartitions) {
				if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.EARLIEST_OFFSET) {
					consumerCallBridge.seekPartitionToBeginning(consumerTmp, newPartitionState.getKafkaPartitionHandle());
					newPartitionState.setOffset(consumerTmp.position(newPartitionState.getKafkaPartitionHandle()) - 1);
				} else if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.LATEST_OFFSET) {
					consumerCallBridge.seekPartitionToEnd(consumerTmp, newPartitionState.getKafkaPartitionHandle());
					newPartitionState.setOffset(consumerTmp.position(newPartitionState.getKafkaPartitionHandle()) - 1);
				} else if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.GROUP_OFFSET) {
					// the KafkaConsumer by default will automatically seek the consumer position
					// to the committed group offset, so we do not need to do it.

					newPartitionState.setOffset(consumerTmp.position(newPartitionState.getKafkaPartitionHandle()) - 1);
				} else if (newPartitionState.getOffset() == KafkaTopicPartitionStateSentinel.RESET_TO_EARLIEST_FOR_NEW_PARTITION) {
					OffsetAndMetadata offsetAndMetadata = consumerTmp.committed(newPartitionState.getKafkaPartitionHandle());
					long offset;
					if (offsetAndMetadata == null) {
						consumerCallBridge.seekPartitionToBeginning(consumerTmp, newPartitionState.getKafkaPartitionHandle());
						offset = consumerTmp.position(newPartitionState.getKafkaPartitionHandle());
					} else {
						offset = offsetAndMetadata.offset();
					}
					newPartitionState.setOffset(offset - 1);
				} else {
					consumerTmp.seek(newPartitionState.getKafkaPartitionHandle(), newPartitionState.getOffset() + 1);
				}
			}
		} catch (WakeupException e) {
			// a WakeupException may be thrown if the consumer was invoked wakeup()
			// before it was isolated for the reassignment. In this case, we abort the
			// reassignment and just re-expose the original consumer.

			synchronized (consumerReassignmentLock) {
				this.consumer = consumerTmp;

				// if reassignment had already started and affected the consumer,
				// we do a full roll back so that it is as if it was left untouched
				if (reassignmentStarted) {
					consumerCallBridge.assignPartitions(
							this.consumer, new ArrayList<>(oldPartitionAssignmentsToPosition.keySet()));

					for (Map.Entry<TopicPartition, Long> oldPartitionToPosition : oldPartitionAssignmentsToPosition.entrySet()) {
						this.consumer.seek(oldPartitionToPosition.getKey(), oldPartitionToPosition.getValue());
					}
				}

				// no need to restore the wakeup state in this case,
				// since only the last wakeup call is effective anyways
				hasBufferedWakeup = false;

				// re-add all new partitions back to the unassigned partitions queue to be picked up again
				for (KafkaTopicPartitionState<TopicPartition> newPartition : newPartitions) {
					unassignedPartitionsQueue.add(newPartition);
				}

				// this signals the main fetch loop to continue through the loop
				throw new AbortedReassignmentException();
			}
		}

		// reassignment complete; expose the reassigned consumer
		synchronized (consumerReassignmentLock) {
			this.consumer = consumerTmp;

			// restore wakeup state for the consumer if necessary
			if (hasBufferedWakeup) {
				this.consumer.wakeup();
				hasBufferedWakeup = false;
			}
		}
	}

	@VisibleForTesting
	KafkaConsumer<byte[], byte[]> getConsumer(Properties kafkaProperties) {
		return new KafkaConsumer<>(kafkaProperties);
	}

	@VisibleForTesting
	FlinkConnectorRateLimiter getRateLimiter() {
		return rateLimiter;
	}

	boolean isCommitInProgress() {
		return commitInProgress;
	}

	// -----------------------------------------------------------------------
	// Rate limiting methods
	// -----------------------------------------------------------------------

	/**
	 *
	 * @param records List of ConsumerRecords.
	 * @return Total batch size in bytes, including key and value.
	 */
	private int getRecordBatchSize(ConsumerRecords<byte[], byte[]> records) {
		int recordBatchSizeBytes = 0;
		for (ConsumerRecord<byte[], byte[]> record: records) {
			// Null is an allowed value for the key
			if (record.key() != null) {
				recordBatchSizeBytes += record.key().length;
			}
			recordBatchSizeBytes += record.value().length;

		}
		return recordBatchSizeBytes;
	}

	/**
	 * Get records from Kafka. If the rate-limiting feature is turned on, this method is called at
	 * a rate specified by the {@link #rateLimiter}.
	 * @return ConsumerRecords
	 */
	@VisibleForTesting
	protected ConsumerRecords<byte[], byte[]> getRecordsFromKafka() {
		ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);
		if (rateLimiter != null) {
			int requiredNum;
			switch (rateLimitingUnit) {
				case BYTE:
					// limit byte number
					requiredNum = getRecordBatchSize(records);
					break;
				case RECORD:
					// limit record number
					requiredNum = records.count();
					break;
				default:
					throw new FlinkRuntimeException(
						String.format("Unsupported RateLimitingUnit: %s.", rateLimitingUnit));

			}
			rateLimiter.acquire(requiredNum);
		}
		return records;
	}

	/** Wait for subscribed partitions ready to be fetched. If all partitions are not ready, sleep the thread.
	 * In the case that last call has got enough sampled records, ready state means unprocessed records amount
	 * to {{@link #sampleSegmentLen}}.
	 * In the case that last call hasn't got enough sampled records, ready state means unprocessed records amount
	 * to {{@link #sampleSegmentLen}} plus number of records left to be sampling last call.
	 */
	private void runWaitingLoop(Set<TopicPartition> toProcessPartitions) throws InterruptedException {
		Map<TopicPartition, Long> latestOffsetMap;
		long latestOffset, lastProcessed;
		boolean toWait = true;
		while (running && toWait) {
			consumerCallBridge.resume(consumer, suspendPartitions);
			suspendPartitions.clear();
			latestOffsetMap = consumerCallBridge.getEndOffsets(consumer, toProcessPartitions);
			//Check each partitions if they are ready to be sampled, if not, pause the partitions.
			for (TopicPartition partition : toProcessPartitions) {
				latestOffset = latestOffsetMap.getOrDefault(partition, 0L);
				lastProcessed = lastProcessedMap.get(partition);
				if ((latestOffset - lastProcessed) >=
						(toConsumeMap.getOrDefault(partition, 0L) + sampleSegmentLen)) {
					if (!toConsumeMap.containsKey(partition)) {
						consumer.seek(partition, lastProcessed + sampleInterval);
					}
				} else {
					suspendPartitions.add(partition);
				}
			}
			consumerCallBridge.pause(consumer, suspendPartitions);
			if (suspendPartitions.size() != toProcessPartitions.size()) {
				toWait = false;
			} else {
				sleep(100);
			}
		}
	}

	/**
	 * Sample data in a segment, when the segment is very long, sample can be done several times.
	 * Note that except the root call of this method, sampleLen is always equal to {{@link #sampleNum}}.
	 * In the root call, sampleLen can be either {{@link #sampleNum}} or toConsumeSize get from {{@link #toConsumeMap}}.
	 * Also, this method will return the toConsumeSize that next sampling call should cover.
	 * @return the toConsumeSize that next sampling call should cover.
	 */
	private long sampleInSegment(
			List<ConsumerRecord<byte[], byte[]>> candidates,
			List<ConsumerRecord<byte[], byte[]>> sampleData,
			long sampleLen) {
		if (candidates.size() == 0) {
			return 0;
		} else if (candidates.size() < sampleLen) {
			sampleData.addAll(candidates);
			return sampleLen - candidates.size();
		} else if (candidates.size() < sampleLen + sampleInterval) {
			sampleData.addAll(candidates.subList(0, Math.toIntExact(sampleLen)));
			return 0;
		} else {
			sampleData.addAll(candidates.subList(0, Math.toIntExact(sampleLen)));
			return sampleInSegment(
					candidates.subList(Math.toIntExact(sampleLen + sampleInterval), candidates.size()),
					sampleData,
					sampleNum);
		}
	}

	/**
	 * Get sampled records from Kafka. In one sampling segment, we skip {{@link #sampleInterval}} records,
	 * and then return {{@link #sampleNum}} records. Because we cannot control the number of records fetched
	 * from Kafka, we may not get enough sampled records in one call. Therefore, we should record the progress
	 * of sampling each time when this method is called.
	 * @return ConsumerRecords
	 * @throws InterruptedException
	 */
	private ConsumerRecords<byte[], byte[]> getSampledRecordsAfterInterval() throws InterruptedException {
		Set<TopicPartition> toProcessPartitions = consumer.assignment();

		runWaitingLoop(toProcessPartitions);

		ConsumerRecords<byte[], byte[]> records = consumer.poll(pollTimeout);

		Map<TopicPartition, List<ConsumerRecord<byte[], byte[]>>> sampledRecords = new HashMap<>();
		long sampleLen, leftToConsume;
		List<ConsumerRecord<byte[], byte[]>> tmpList;
		for (TopicPartition partition : toProcessPartitions) {
			List<ConsumerRecord<byte[], byte[]>> candidates = records.records(partition);
			if (candidates.size() == 0) {
				continue;
			}
			tmpList = new ArrayList<>();
			sampleLen = toConsumeMap.getOrDefault(partition, sampleNum);
			leftToConsume = sampleInSegment(candidates, tmpList, sampleLen);
			if (leftToConsume > 0){
				toConsumeMap.put(partition, leftToConsume);
			} else {
				toConsumeMap.remove(partition);
			}
			lastProcessedMap.put(partition, tmpList.get(tmpList.size() - 1).offset());
			sampledRecords.put(partition, tmpList);
		}

		return new ConsumerRecords<>(sampledRecords);
	}


	// ------------------------------------------------------------------------
	//  Utilities
	// ------------------------------------------------------------------------

	private static List<TopicPartition> convertKafkaPartitions(List<KafkaTopicPartitionState<TopicPartition>> partitions) {
		ArrayList<TopicPartition> result = new ArrayList<>(partitions.size());
		for (KafkaTopicPartitionState<TopicPartition> p : partitions) {
			result.add(p.getKafkaPartitionHandle());
		}
		return result;
	}

	private class CommitCallback implements OffsetCommitCallback {

		private final KafkaCommitCallback internalCommitCallback;

		CommitCallback(KafkaCommitCallback internalCommitCallback) {
			this.internalCommitCallback = checkNotNull(internalCommitCallback);
		}

		@Override
		public void onComplete(Map<TopicPartition, OffsetAndMetadata> offsets, Exception ex) {
			commitInProgress = false;

			if (ex != null) {
				log.warn("Committing offsets to Kafka failed. This does not compromise Flink's checkpoints.", ex);
				internalCommitCallback.onException(ex);
			} else {
				internalCommitCallback.onSuccess();
			}
		}
	}

	/**
	 * Utility exception that serves as a signal for the main loop to continue through the loop
	 * if a reassignment attempt was aborted due to an pre-reassignment wakeup call on the consumer.
	 */
	private static class AbortedReassignmentException extends Exception {
		private static final long serialVersionUID = 1L;
	}
}
