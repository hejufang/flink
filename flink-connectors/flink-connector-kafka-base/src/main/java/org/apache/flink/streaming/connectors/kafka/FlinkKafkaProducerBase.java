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

package org.apache.flink.streaming.connectors.kafka;

import org.apache.flink.annotation.Internal;
import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.functions.RuntimeContext;
import org.apache.flink.api.common.io.ratelimiting.FlinkConnectorRateLimiter;
import org.apache.flink.api.java.ClosureCleaner;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.metrics.Counter;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.MetricsConstants;
import org.apache.flink.runtime.state.FunctionInitializationContext;
import org.apache.flink.runtime.state.FunctionSnapshotContext;
import org.apache.flink.streaming.api.checkpoint.CheckpointedFunction;
import org.apache.flink.streaming.api.functions.SpecificParallelism;
import org.apache.flink.streaming.api.functions.sink.RichSinkFunction;
import org.apache.flink.streaming.api.operators.StreamingRuntimeContext;
import org.apache.flink.streaming.connectors.kafka.internals.KafkaProducerFactory;
import org.apache.flink.streaming.connectors.kafka.internals.KeyedSerializationSchemaWrapper;
import org.apache.flink.streaming.connectors.kafka.internals.metrics.KafkaMetricWrapper;
import org.apache.flink.streaming.connectors.kafka.partitioner.FlinkKafkaPartitioner;
import org.apache.flink.streaming.util.serialization.KeyedSerializationSchema;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.functions.DeleteNormalizer;
import org.apache.flink.table.functions.RowKindSinkFilter;
import org.apache.flink.util.NetUtils;
import org.apache.flink.util.SerializableObject;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.Metric;
import org.apache.kafka.common.MetricName;
import org.apache.kafka.common.PartitionInfo;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;

/**
 * Flink Sink to produce data into a Kafka topic.
 *
 * <p>Please note that this producer provides at-least-once reliability guarantees when
 * checkpoints are enabled and setFlushOnCheckpoint(true) is set.
 * Otherwise, the producer doesn't provide any reliability guarantees.
 *
 * @param <IN> Type of the messages to write into Kafka.
 */
@Internal
public abstract class FlinkKafkaProducerBase<IN> extends RichSinkFunction<IN> implements
		CheckpointedFunction,
		SpecificParallelism {

	private static final Logger LOG = LoggerFactory.getLogger(FlinkKafkaProducerBase.class);

	private static final long serialVersionUID = 1L;

	/**
	 * Configuration key for disabling the metrics reporting.
	 */
	public static final String KEY_DISABLE_METRICS = "flink.disable-metrics";

	/**
	 * User defined properties for the Producer.
	 */
	protected final Properties producerConfig;

	/**
	 * The name of the default topic this producer is writing data to.
	 */
	protected final String defaultTopicId;

	/**
	 * (Serializable) SerializationSchema for turning objects used with Flink into.
	 * byte[] for Kafka.
	 */
	protected final KeyedSerializationSchema<IN> schema;

	/**
	 * User-provided partitioner for assigning an object to a Kafka partition for each topic.
	 */
	protected final FlinkKafkaPartitioner<IN> flinkKafkaPartitioner;

	/**
	 * Partitions of each topic.
	 */
	protected final Map<String, int[]> topicPartitionsMap;

	/**
	 * Flag indicating whether to accept failures (and log them), or to fail on failures.
	 */
	protected boolean logFailuresOnly;

	/**
	 * If true, the producer will wait until all outstanding records have been send to the broker.
	 */
	protected boolean flushOnCheckpoint = true;

	// -------------------------------- Runtime fields ------------------------------------------

	/** KafkaProducer instance. */
	protected transient KafkaProducer<byte[], byte[]> producer;

	/** The callback than handles error propagation or logging callbacks. */
	protected transient Callback callback;

	/**
	 * Metrics to save the number of write failed.
	 */
	protected transient Counter writeFailedCounter;

	/** Errors encountered in the async producer are stored here. */
	protected transient volatile Exception asyncException;

	/** Lock for accessing the pending records. */
	protected final SerializableObject pendingRecordsLock = new SerializableObject();

	/** Number of unacknowledged records. */
	protected long pendingRecords;

	/** Lock for accessing the in-flight parameters. */
	protected final SerializableObject inFlightLock = new SerializableObject();

	/** In-flight records' size. */
	protected long inFlightRecordsSize;

	/** Number of in-flight records. */
	protected long inFlightRecordsNum;

	/** Single max batch size in kafka. */
	protected final long kafkaMaxBatchSize;

	/** Total max in-flight size of in-flight data. Set default value*/
	protected long maxInFlightSize = Long.MAX_VALUE;

	protected KafkaProducerFactory producerFactory = KafkaProducerFactory.DefaultKafkaProducerFactory.getInstance();

	/** The in-flight dataSize factor. */
	protected int inFlightFactor;

	/** The max number of in-flight records. */
	protected int maxInFlightNum = 0;

	protected RowKindSinkFilter<IN> rowKindSinkFilter;

	protected DeleteNormalizer<IN> deleteNormalizer;

	private int parallelism = FactoryUtil.PARALLELISM.defaultValue();

	private FlinkConnectorRateLimiter rateLimiter;

	/**
	 * All history partitions of each topic.
	 */
	protected final Map<String, Set<Integer>> historyTopicPartitionMap = new HashMap<>();

	/**
	 * The main constructor for creating a FlinkKafkaProducer.
	 *
	 * @param defaultTopicId The default topic to write data to
	 * @param serializationSchema A serializable serialization schema for turning user objects into a kafka-consumable byte[] supporting key/value messages
	 * @param producerConfig Configuration properties for the KafkaProducer. 'bootstrap.servers.' is the only required argument.
	 * @param customPartitioner A serializable partitioner for assigning messages to Kafka partitions. Passing null will use Kafka's partitioner.
	 */
	public FlinkKafkaProducerBase(String defaultTopicId, KeyedSerializationSchema<IN> serializationSchema, Properties producerConfig, FlinkKafkaPartitioner<IN> customPartitioner) {
		requireNonNull(defaultTopicId, "TopicID not set");
		requireNonNull(serializationSchema, "serializationSchema not set");
		requireNonNull(producerConfig, "producerConfig not set");
		ClosureCleaner.clean(customPartitioner, ExecutionConfig.ClosureCleanerLevel.RECURSIVE, true);
		ClosureCleaner.ensureSerializable(serializationSchema);

		this.defaultTopicId = defaultTopicId;
		this.schema = serializationSchema;
		this.producerConfig = producerConfig;
		this.flinkKafkaPartitioner = customPartitioner;

		// set the producer configuration properties for kafka record key value serializers.
		if (!producerConfig.containsKey(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG);
		}

		if (!producerConfig.containsKey(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG)) {
			this.producerConfig.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, ByteArraySerializer.class.getName());
		} else {
			LOG.warn("Overwriting the '{}' is not recommended", ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG);
		}

		this.topicPartitionsMap = new HashMap<>();
		getProducerDefaultConfig().forEach((key, value) -> {
			if (!producerConfig.containsKey(key)) {
				LOG.info("Add default configuration: {} = {}", key, value);
				producerConfig.put(key, value);
			}
		});
		this.kafkaMaxBatchSize = Long.valueOf(producerConfig.get(ProducerConfig.BATCH_SIZE_CONFIG).toString());
	}

	// ---------------------------------- Properties --------------------------

	/**
	 * Defines whether the producer should fail on errors, or only log them.
	 * If this is set to true, then exceptions will be only logged, if set to false,
	 * exceptions will be eventually thrown and cause the streaming program to
	 * fail (and enter recovery).
	 *
	 * @param logFailuresOnly The flag to indicate logging-only on exceptions.
	 */
	public void setLogFailuresOnly(boolean logFailuresOnly) {
		this.logFailuresOnly = logFailuresOnly;
	}

	public void setInFlightFactor(int inFlightFactor) {
		this.inFlightFactor = inFlightFactor;
	}

	public void setMaxInFlightNum(int maxInFlightNum) {
		this.maxInFlightNum = maxInFlightNum;
	}

	/**
	 * If set to true, the Flink producer will wait for all outstanding messages in the Kafka buffers
	 * to be acknowledged by the Kafka producer on a checkpoint.
	 * This way, the producer can guarantee that messages in the Kafka buffers are part of the checkpoint.
	 *
	 * @param flush Flag indicating the flushing mode (true = flush on checkpoint)
	 */
	public void setFlushOnCheckpoint(boolean flush) {
		this.flushOnCheckpoint = flush;
	}

	/**
	 * Used for testing only.
	 */
	@VisibleForTesting
	protected <K, V> KafkaProducer<K, V> getKafkaProducer(Properties props) {
		return (KafkaProducer<K, V>) producerFactory.getProducer(props);
	}

	// ----------------------------------- Utilities --------------------------

	/**
	 * Initializes the connection to Kafka.
	 */
	@Override
	public void open(Configuration configuration) throws Exception {
		if (schema instanceof KeyedSerializationSchemaWrapper) {
			((KeyedSerializationSchemaWrapper<IN>) schema).getSerializationSchema()
				.open(() -> getRuntimeContext().getMetricGroup().addGroup("user"));
		}
		producer = getKafkaProducer(this.producerConfig);

		RuntimeContext ctx = getRuntimeContext();

		if (rateLimiter != null) {
			rateLimiter.open(ctx);
		}

		if (null != flinkKafkaPartitioner) {
			flinkKafkaPartitioner.open(ctx.getIndexOfThisSubtask(), ctx.getNumberOfParallelSubtasks());
		}

		LOG.info("Starting FlinkKafkaProducer ({}/{}) to produce into default topic {}",
				ctx.getIndexOfThisSubtask() + 1, ctx.getNumberOfParallelSubtasks(), defaultTopicId);

		// register Kafka metrics to Flink accumulators
		if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
			Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

			if (metrics == null) {
				// MapR's Kafka implementation returns null here.
				LOG.info("Producer implementation does not support metrics");
			} else {
				final MetricGroup kafkaMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
				for (Map.Entry<MetricName, ? extends Metric> metric : metrics.entrySet()) {
					kafkaMetricGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
				}
			}
		}

		if (flushOnCheckpoint && !((StreamingRuntimeContext) this.getRuntimeContext()).isCheckpointingEnabled()) {
			LOG.warn("Flushing on checkpoint is enabled, but checkpointing is not enabled. Disabling flushing.");
			flushOnCheckpoint = false;
		}
		// Calculate the max in-flight data size.
		if (inFlightFactor > 0) {
			maxInFlightSize = inFlightFactor * kafkaMaxBatchSize;
		}

		if (logFailuresOnly) {
			this.writeFailedCounter =
				getRuntimeContext().getMetricGroup().counter(ConfigConstants.WRITE_FAILED_COUNTER);
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception e) {
					if (e != null) {
						writeFailedCounter.inc();
						LOG.error("Error while sending record to Kafka: " + e.getMessage(), e);
					}
					acknowledgeMessage(metadata);
				}
			};
		}
		else {
			callback = new Callback() {
				@Override
				public void onCompletion(RecordMetadata metadata, Exception exception) {
					if (exception != null && asyncException == null) {
						asyncException = exception;
					}
					acknowledgeMessage(metadata);
				}
			};
		}
	}

	/**
	 * Called when new data arrives to the sink, and forwards it to Kafka.
	 *
	 * @param next
	 * 		The incoming data
	 */
	@Override
	public void invoke(IN next, Context context) throws Exception {
		// propagate asynchronous errors
		checkErroneous();

		if (!filter(next)) {
			return;
		}

		acquireRateLimit();
		byte[] serializedKey = schema.serializeKey(next);
		byte[] serializedValue = schema.serializeValue(next);
		String targetTopic = schema.getTargetTopic(next);
		if (targetTopic == null) {
			targetTopic = defaultTopicId;
		}

		int[] partitions = this.topicPartitionsMap.get(targetTopic);
		if (null == partitions) {
			partitions = getPartitionsByTopic(targetTopic, producer);
			// register Kafka metrics to Flink accumulators
			if (!Boolean.parseBoolean(producerConfig.getProperty(KEY_DISABLE_METRICS, "false"))) {
				registerKafkaMetricForNewPartition(
					targetTopic,
					Arrays
						.stream(partitions)
						.boxed()
						.collect(Collectors.toSet()));
			}
			this.topicPartitionsMap.put(targetTopic, partitions);
		}

		ProducerRecord<byte[], byte[]> record;
		if (flinkKafkaPartitioner == null) {
			record = new ProducerRecord<>(targetTopic, serializedKey, serializedValue);
		} else {
			record = new ProducerRecord<>(
					targetTopic,
					flinkKafkaPartitioner.partition(next, serializedKey, serializedValue, targetTopic, partitions),
					serializedKey,
					serializedValue);
		}
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords++;
			}
		}
		producer.send(record, callback);
	}

	@Override
	public void close() throws Exception {
		if (producer != null) {
			producer.close();
		}

		// make sure we propagate pending errors
		checkErroneous();
	}

	// ------------------- Logic for handling checkpoint flushing -------------------------- //

	private void acknowledgeMessage(RecordMetadata recordMetadata) {
		if (flushOnCheckpoint) {
			synchronized (pendingRecordsLock) {
				pendingRecords--;
				if (pendingRecords == 0) {
					pendingRecordsLock.notifyAll();
				}
			}
		}
		if (inFlightFactor > 0 || maxInFlightNum > 0) {
			synchronized (inFlightLock) {
				int recordSize = 0;
				if (recordMetadata.serializedKeySize() > 0) {
					recordSize += recordMetadata.serializedKeySize();
				}
				if (recordMetadata.serializedValueSize() > 0) {
					recordSize += recordMetadata.serializedValueSize();
				}
				inFlightRecordsSize -= recordSize;
				inFlightRecordsNum--;
				if (inFlightRecordsSize == 0 || inFlightRecordsNum == 0) {
					inFlightLock.notifyAll();
				}
			}
		}
	}

	/**
	 * Flush pending records.
	 */
	protected abstract void flush();

	@Override
	public void initializeState(FunctionInitializationContext context) throws Exception {
		// nothing to do
	}

	@Override
	public void snapshotState(FunctionSnapshotContext ctx) throws Exception {
		// check for asynchronous errors and fail the checkpoint if necessary
		checkErroneous();

		if (flushOnCheckpoint) {
			// flushing is activated: We need to wait until pendingRecords is 0
			flush();
			synchronized (pendingRecordsLock) {
				if (pendingRecords != 0) {
					throw new IllegalStateException("Pending record count must be zero at this point: " + pendingRecords);
				}

				// if the flushed requests has errors, we should propagate it also and fail the checkpoint
				checkErroneous();
			}
		}
	}

	// ----------------------------------- Utilities --------------------------

	protected void checkErroneous() throws Exception {
		Exception e = asyncException;
		if (e != null) {
			// prevent double throwing
			asyncException = null;
			throw new Exception("Failed to send data to Kafka: " + e.getMessage(), e);
		}
	}

	public static Properties getPropertiesFromBrokerList(String brokerList) {
		String[] elements = brokerList.split(",");

		// validate the broker addresses
		for (String broker: elements) {
			NetUtils.getCorrectHostnamePort(broker);
		}

		Properties props = new Properties();
		props.setProperty(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, brokerList);
		return props;
	}

	protected static int[] getPartitionsByTopic(String topic, KafkaProducer<byte[], byte[]> producer) {
		// the fetched list is immutable, so we're creating a mutable copy in order to sort it
		List<PartitionInfo> partitionsList = new ArrayList<>(producer.partitionsFor(topic));

		// sort the partitions by partition id to make sure the fetched partition list is the same across subtasks
		Collections.sort(partitionsList, new Comparator<PartitionInfo>() {
			@Override
			public int compare(PartitionInfo o1, PartitionInfo o2) {
				return Integer.compare(o1.partition(), o2.partition());
			}
		});

		int[] partitions = new int[partitionsList.size()];
		for (int i = 0; i < partitions.length; i++) {
			partitions[i] = partitionsList.get(i).partition();
		}

		return partitions;
	}

	@VisibleForTesting
	protected long numPendingRecords() {
		synchronized (pendingRecordsLock) {
			return pendingRecords;
		}
	}

	protected Map<String, Object> getProducerDefaultConfig() {
		return new HashMap<>();
	}

	public FlinkKafkaProducerBase<IN> setRowKindSinkFilter(RowKindSinkFilter<IN> rowKindSinkFilter) {
		this.rowKindSinkFilter = rowKindSinkFilter;
		return this;
	}

	public FlinkKafkaProducerBase<IN> setDeleteNormalizer(DeleteNormalizer<IN> normalizer) {
		this.deleteNormalizer = normalizer;
		return this;
	}

	boolean filter(IN in) {
		return rowKindSinkFilter == null || rowKindSinkFilter.filter(in);
	}

	public void setProducerFactory(KafkaProducerFactory producerFactory) {
		this.producerFactory = producerFactory;
	}

	public void setParallelism(int parallelism) {
		this.parallelism = parallelism;
	}

	public void setRateLimiter(FlinkConnectorRateLimiter rateLimiter) {
		this.rateLimiter = rateLimiter;
	}

	@Override
	public int getParallelism() {
		return parallelism;
	}

	protected void acquireRateLimit() {
		if (rateLimiter != null) {
			rateLimiter.acquire(1);
		}
	}

	/**
	 * When inFlightRecordsSize is larger than maxInFlightSize, block invoking and wait for all in-flight
	 * records to be sent.
	 * @throws InterruptedException
	 */
	protected void inFlightWaitFinished() throws InterruptedException {
		if (inFlightFactor > 0 || maxInFlightNum > 0) {
			// Choose the min one to check.
			if (inFlightRecordsSize > maxInFlightSize || inFlightRecordsNum > maxInFlightNum) {
				synchronized (inFlightLock) {
					if (inFlightRecordsSize > 0 || inFlightRecordsNum > 0) {
						LOG.info("Exceed the max in-flight value. Wait for in-flight records onCompletion.");
						inFlightLock.wait();
					}
				}
				if (inFlightRecordsSize != 0 || inFlightRecordsNum != 0) {
					throw new IllegalArgumentException(String.format("In-flight Records must be zero after waiting!" +
						"Record size: %d, Record num: ", inFlightRecordsSize, inFlightRecordsNum));
				}
			}
		}
	}

	protected void registerKafkaMetricForNewPartition(String targetTopic, Set<Integer> addPartition) {
		historyTopicPartitionMap.putIfAbsent(targetTopic, new HashSet<>());
		addPartition.removeAll(historyTopicPartitionMap.get(targetTopic));

		registerKafkaMetricForPartition(targetTopic, addPartition);

		historyTopicPartitionMap.get(targetTopic).addAll(addPartition);
	}

	private void registerKafkaMetricForPartition(String targetTopic, Set<Integer> partitionIds) {
		Map<MetricName, ? extends Metric> metrics = this.producer.metrics();

		if (metrics == null) {
			// MapR's Kafka implementation returns null here.
			LOG.info("Producer implementation does not support metrics");
		} else {
			final MetricGroup producerMetricGroup = getRuntimeContext().getMetricGroup().addGroup("KafkaProducer");
			for (Map.Entry<MetricName, ? extends Metric> metric: metrics.entrySet()) {
				Map<String, String> tags = metric.getKey().tags();
				if (tags.containsKey("partition") && tags.containsKey("topic")) {
					String topic = tags.get("topic");
					String partition = tags.get("partition");

					if (topic.equalsIgnoreCase(targetTopic) && partitionIds.contains(Integer.parseInt(partition))) {
						MetricGroup topicPartitionGroup = producerMetricGroup
							.addGroup("topic", topic)
							.addGroup("partition", partition)
							.addGroup(MetricsConstants.METRICS_CONNECTOR_TYPE, "kafka")
							.addGroup(MetricsConstants.METRICS_FLINK_VERSION, MetricsConstants.FLINK_VERSION_VALUE);

						topicPartitionGroup.gauge(metric.getKey().name(), new KafkaMetricWrapper(metric.getValue()));
					}
				}
			}
		}
	}
}
