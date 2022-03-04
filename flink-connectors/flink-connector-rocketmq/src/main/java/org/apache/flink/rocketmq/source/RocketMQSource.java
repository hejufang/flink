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

package org.apache.flink.rocketmq.source;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.connector.source.Boundedness;
import org.apache.flink.api.connector.source.Source;
import org.apache.flink.api.connector.source.SourceReader;
import org.apache.flink.api.connector.source.SourceReaderContext;
import org.apache.flink.api.connector.source.SplitEnumerator;
import org.apache.flink.api.connector.source.SplitEnumeratorContext;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.api.java.typeutils.ResultTypeQueryable;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.connector.base.source.reader.RecordsWithSplitIds;
import org.apache.flink.connector.base.source.reader.synchronization.FutureCompletingBlockingQueue;
import org.apache.flink.connector.base.source.reader.synchronization.FutureNotifier;
import org.apache.flink.connector.rocketmq.RocketMQConfig;
import org.apache.flink.connector.rocketmq.RocketMQUtils;
import org.apache.flink.connector.rocketmq.serialization.RocketMQDeserializationSchema;
import org.apache.flink.core.io.SimpleVersionedSerializer;
import org.apache.flink.rocketmq.source.enumerator.RocketMQEnumState;
import org.apache.flink.rocketmq.source.enumerator.RocketMQEnumStateSerializer;
import org.apache.flink.rocketmq.source.enumerator.RocketMQSplitEnumerator;
import org.apache.flink.rocketmq.source.reader.RocketMQRecordEmitter;
import org.apache.flink.rocketmq.source.reader.RocketMQSourceReader;
import org.apache.flink.rocketmq.source.reader.RocketMQSplitReader;
import org.apache.flink.rocketmq.source.split.RocketMQSplit;
import org.apache.flink.rocketmq.source.split.RocketMQSplitSerializer;

import java.io.IOException;
import java.util.Map;
import java.util.function.Supplier;

/**
 * The Source implementation of RocketMQ.
 * @param <OUT>
 */
public class RocketMQSource<OUT> implements
		Source<OUT, RocketMQSplit, RocketMQEnumState>,
		ResultTypeQueryable<OUT> {

	private static final long serialVersionUID = 1L;

	private final Boundedness boundedness;

	private final RocketMQDeserializationSchema<OUT> deserializationSchema;

	private final Map<String, String> props;

	private final RocketMQConfig<OUT> config;

	private final String jobName;

	public RocketMQSource(
			Boundedness boundedness,
			RocketMQDeserializationSchema<OUT> deserializationSchema,
			Map<String, String> props,
			RocketMQConfig<OUT> config) {
		this.boundedness = boundedness;
		this.deserializationSchema = deserializationSchema;
		this.props = props;
		this.config = config;
		this.jobName = System.getProperty(
			ConfigConstants.JOB_NAME_KEY, ConfigConstants.JOB_NAME_DEFAULT);
		RocketMQUtils.saveConfigurationToSystemProperties(config);
		RocketMQUtils.validateBrokerQueueList(config);
	}

	@Override
	public Boundedness getBoundedness() {
		return this.boundedness;
	}

	@Override
	public SourceReader<OUT, RocketMQSplit> createReader(SourceReaderContext readerContext) {
		FutureNotifier futureNotifier = new FutureNotifier();
		FutureCompletingBlockingQueue<RecordsWithSplitIds<Tuple3<OUT, Long, Long>>> elementsQueue =
				new FutureCompletingBlockingQueue<>(futureNotifier);
		Supplier<RocketMQSplitReader<OUT>> splitReaderSupplier =
				() ->
						new RocketMQSplitReader<>(
								deserializationSchema,
								props,
								config,
								jobName,
								readerContext);
		RocketMQRecordEmitter<OUT> recordEmitter = new RocketMQRecordEmitter<>();
		return new RocketMQSourceReader(
			futureNotifier,
			elementsQueue,
			splitReaderSupplier,
			recordEmitter,
			config,
			readerContext);
	}

	@Override
	public SplitEnumerator<RocketMQSplit, RocketMQEnumState> createEnumerator(SplitEnumeratorContext<RocketMQSplit> enumContext) {
		return new RocketMQSplitEnumerator(enumContext, props, config, jobName, boundedness);
	}

	@Override
	public SplitEnumerator<RocketMQSplit, RocketMQEnumState> restoreEnumerator(
			SplitEnumeratorContext<RocketMQSplit> enumContext,
			RocketMQEnumState checkpoint) throws IOException {
		// We do not need restore the assignment in enumerator. Everytime the job restarts, the enumerator
		// should discover new partitions and reassign them. So here, just create a new enumerator.
		return new RocketMQSplitEnumerator(enumContext, props, config, jobName, boundedness);
	}

	@Override
	public SimpleVersionedSerializer<RocketMQSplit> getSplitSerializer() {
		return new RocketMQSplitSerializer();
	}

	@Override
	public SimpleVersionedSerializer<RocketMQEnumState> getEnumeratorCheckpointSerializer() {
		return new RocketMQEnumStateSerializer();
	}

	@Override
	public TypeInformation<OUT> getProducedType() {
		return deserializationSchema.getProducedType();
	}

	@Override
	public int getParallelism() {
		return config.getParallelism();
	}
}
