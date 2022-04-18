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

package org.apache.flink.streaming.api.operators.sink;

import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.datastream.DataStreamSink;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.transformations.SinkTransformation;

/**
 * A {@link DataStreamSink} which is used to push results from task to netty server.
 */
public class TaskPushStreamSink<T> extends DataStreamSink<T> {

	private final SinkTransformation<T> transformation;

	public TaskPushStreamSink(DataStream<T> inputStream, TaskPushSinkOperatorFactory<T> factory) {
		super(inputStream, (TaskPushSinkOperator<T>) factory.getOperator());
		this.transformation = new SinkTransformation<>(
			inputStream.getTransformation(),
			"Socket Stream Sink",
			factory,
			inputStream.getParallelism());
	}

	@Override
	public SinkTransformation<T> getTransformation() {
		return transformation;
	}

	@Override
	public DataStreamSink<T> name(String name) {
		transformation.setName(name);
		return this;
	}

	@Override
	public DataStreamSink<T> uid(String uid) {
		transformation.setUid(uid);
		return this;
	}

	@Override
	public DataStreamSink<T> setUidHash(String uidHash) {
		transformation.setUidHash(uidHash);
		return this;
	}

	@Override
	public DataStreamSink<T> setParallelism(int parallelism) {
		transformation.setParallelism(parallelism);
		return this;
	}

	@Override
	public DataStreamSink<T> disableChaining() {
		this.transformation.setChainingStrategy(ChainingStrategy.NEVER);
		return this;
	}

	@Override
	public DataStreamSink<T> slotSharingGroup(String slotSharingGroup) {
		transformation.setSlotSharingGroup(slotSharingGroup);
		return this;
	}
}
