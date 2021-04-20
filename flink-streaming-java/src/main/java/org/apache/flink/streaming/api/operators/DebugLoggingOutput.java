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

package org.apache.flink.streaming.api.operators;

import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.LatencyMarker;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.OutputTag;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.LocalDateTime;

/**
 * Wrapping {@link Output} that updates metrics on the number of emitted elements.
 */
public class DebugLoggingOutput<OUT> implements Output<StreamRecord<OUT>> {
	protected static final Logger LOG = LoggerFactory.getLogger(DebugLoggingOutput.class);

	private final Output<StreamRecord<OUT>> output;
	private final String operatorName;
	private final DebugLoggingConverter converter;
	private final DebugLoggingLocation debugLoggingLocation;
	private final int subtaskId;

	public DebugLoggingOutput(
			String operatorName,
			Output<StreamRecord<OUT>> output,
			DebugLoggingConverter converter,
			DebugLoggingLocation location,
			int subtaskId) {
		this.operatorName = operatorName;
		this.output = output;
		this.converter = converter;
		this.debugLoggingLocation = location;
		this.subtaskId = subtaskId;
	}

	@Override
	public void emitWatermark(Watermark mark) {
		output.emitWatermark(mark);
	}

	@Override
	public void emitLatencyMarker(LatencyMarker latencyMarker) {
		output.emitLatencyMarker(latencyMarker);
	}

	@Override
	public void collect(StreamRecord<OUT> record) {
		doLogging(record);
		output.collect(record);
	}

	@Override
	public <X> void collect(OutputTag<X> outputTag, StreamRecord<X> record) {
		doLogging(record);
		output.collect(outputTag, record);
	}

	private void doLogging(StreamRecord<?> record) {
		if (converter == null) {
			return;
		}

		final String content = converter.convert(record);

		switch (debugLoggingLocation) {
			case STDOUT:
				System.out.println(String.format("%s %s(%d) %s",
					LocalDateTime.now(),
					operatorName,
					subtaskId,
					content));
				break;
			case LOG_FILE:
				LOG.info(String.format("%s(%d) %s",
					operatorName,
					subtaskId,
					content));
				break;
			default:
				throw new FlinkRuntimeException("Not supported debugging location: '" +
					debugLoggingLocation + "'.");
		}
	}

	@Override
	public void close() {
		output.close();
	}
}
