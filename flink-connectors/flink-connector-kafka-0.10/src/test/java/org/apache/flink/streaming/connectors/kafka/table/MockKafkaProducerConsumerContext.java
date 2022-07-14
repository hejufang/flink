/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.kafka.table;

import org.apache.flink.streaming.connectors.kafka.internals.KafkaConsumerFactory;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaProducerFactory;

import org.apache.flink.shaded.guava18.com.google.common.collect.ImmutableMap;
import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.record.TimestampType;
import org.mockito.invocation.InvocationOnMock;
import org.powermock.api.mockito.PowerMockito;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Future;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.function.Function;
import java.util.stream.Collectors;

import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_CHECKSUM;
import static org.apache.kafka.clients.consumer.ConsumerRecord.NULL_SIZE;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anyLong;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.mock;

/**
 * Context for mocked producer and consumer. Works with {@link KafkaTable010ITCase}.
 */
public class MockKafkaProducerConsumerContext {

	public static <R> R withContextApply(Function<MockKafkaProducerConsumerContext, R> function) {
		final MockKafkaProducerConsumerContext mockContext = new MockKafkaProducerConsumerContext();
		mockContext.setup();
		final R result = function.apply(mockContext);
		mockContext.clear();
		return result;
	}

	public static void withContextPresent(java.util.function.Consumer<MockKafkaProducerConsumerContext> consumer) {
		withContextApply(ctx -> {
			consumer.accept(ctx);
			return null;
		});
	}

	private final List<ProducerRecord<byte[], byte[]>> producerRecords = Collections.synchronizedList(new ArrayList<>());
	private final AtomicInteger consumedIndex = new AtomicInteger(-1);
	private final Object consumerLock = new Object();

	private KafkaConsumer<?, ?> kafkaConsumerMock;
	private KafkaProducer<?, ?> kafkaProducerMock;
	private long testTs;
	private long testOffset;
	private int testPartition;

	private MockKafkaProducerConsumerContext() {
	}

	private void setup() {
		kafkaConsumerMock = mock(KafkaConsumer.class);
		doAnswer(this::consumerCloseMockAction).when(kafkaConsumerMock).close();
		PowerMockito.when(kafkaConsumerMock.poll(anyLong()))
			.thenAnswer(this::consumerPollMockAction);
		PowerMockito.when(kafkaConsumerMock.partitionsFor(anyString()))
			.thenAnswer(this::consumerPartitionsForMockAction);
		kafkaProducerMock = mock(KafkaProducer.class);
		PowerMockito.when(kafkaProducerMock.send(any(), any()))
			.thenAnswer(this::producerMockAction);
		KafkaStaticConsumerFactoryForMock.setConsumerInstance(kafkaConsumerMock);
		KafkaStaticProducerFactoryForMock.setProducerInstance(kafkaProducerMock);
		testTs = System.currentTimeMillis();
		testOffset = (long) (5 * Math.random() + 1);
		testPartition = (int) (5 * Math.random());
	}

	private void clear() {
		KafkaStaticConsumerFactoryForMock.clearConsumerInstance();
		KafkaStaticProducerFactoryForMock.clearProducerInstance();
	}

	public long getTestTs() {
		return testTs;
	}

	public long getTestOffset() {
		return testOffset;
	}

	public long getTestPartition() {
		return testPartition;
	}

	public int getDataCount() {
		return this.producerRecords.size();
	}

	public List<String> getResults() {
		return producerRecords.stream()
			.map(record -> new String(record.value()))
			.collect(Collectors.toList());
	}

	public List<String> getSinkMsgKeyResults() {
		return producerRecords.stream()
			.map(record -> new String(record.key()))
			.collect(Collectors.toList());
	}

	private ConsumerRecords<byte[], byte[]> consumerPollMockAction(InvocationOnMock invocationOnMock) {
		synchronized (consumerLock) {
			int index = consumedIndex.incrementAndGet();
			if (index < producerRecords.size()) {
				final ProducerRecord<byte[], byte[]> producerRecord = producerRecords.get(index);
				final String topic = producerRecord.topic();
				final int partition = testPartition;
				final long offset = testOffset;
				final long ts = testTs;
				final byte[] key = producerRecord.key();
				final byte[] value = producerRecord.value();
				final TopicPartition tp = new TopicPartition(topic, partition);
				final ConsumerRecord<byte[], byte[]> consumerRecord = new ConsumerRecord<>(
					topic,
					partition,
					offset,
					ts,
					TimestampType.CREATE_TIME,
					NULL_CHECKSUM,
					NULL_SIZE,
					NULL_SIZE,
					key,
					value,
					null);
				return new ConsumerRecords(ImmutableMap.of(tp, Lists.newArrayList(consumerRecord)));
			} else {
				return ConsumerRecords.empty();
			}
		}
	}

	private List<PartitionInfo> consumerPartitionsForMockAction(InvocationOnMock invocationOnMock) {
		String topicName = invocationOnMock.getArgument(0);
		return Lists.newArrayList(new PartitionInfo(topicName, testPartition, null, null, null));
	}

	private Object consumerCloseMockAction(InvocationOnMock invocationOnMock) {
		//reset.
		this.consumedIndex.set(-1);
		return new Object();
	}

	private Future<RecordMetadata> producerMockAction(InvocationOnMock invocationOnMock) {
		final ProducerRecord<byte[], byte[]> argument = invocationOnMock.getArgument(0);
		producerRecords.add(argument);
		return null;
	}

	/**
	 * KafkaConsumerFactory for test.
	 */
	public static class KafkaStaticConsumerFactoryForMock implements KafkaConsumerFactory {

		private static volatile KafkaConsumer<?, ?> consumerInstance = null;

		static void setConsumerInstance(KafkaConsumer<?, ?> consumerInstance) {
			KafkaStaticConsumerFactoryForMock.consumerInstance = consumerInstance;
		}

		static void clearConsumerInstance() {
			consumerInstance = null;
		}

		@Override
		public Consumer<byte[], byte[]> getConsumer(Properties kafkaProperties) {
			final KafkaConsumer<?, ?> instance = KafkaStaticConsumerFactoryForMock.consumerInstance;
			if (instance == null) {
				throw new IllegalStateException("instance is null");
			}
			return (Consumer<byte[], byte[]>) instance;
		}
	}

	/**
	 * KafkaProducerFactory for test.
	 */
	public static class KafkaStaticProducerFactoryForMock implements KafkaProducerFactory {

		private static volatile KafkaProducer<?, ?> producerInstance = null;

		static void setProducerInstance(KafkaProducer<?, ?> producerInstance) {
			KafkaStaticProducerFactoryForMock.producerInstance = producerInstance;
		}

		static void clearProducerInstance() {
			producerInstance = null;
		}

		@Override
		public Producer<byte[], byte[]> getProducer(Properties kafkaProperties) {
			final KafkaProducer<?, ?> instance = KafkaStaticProducerFactoryForMock.producerInstance;
			if (instance == null) {
				throw new IllegalStateException("instance is null");
			}
			return (Producer<byte[], byte[]>) instance;
		}
	}

}
