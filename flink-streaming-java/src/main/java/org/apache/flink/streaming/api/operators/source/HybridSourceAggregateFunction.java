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

package org.apache.flink.streaming.api.operators.source;

import org.apache.flink.api.common.functions.AggregateFunction;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;
import java.util.HashSet;
import java.util.Set;

/**
 * There are three stages:
 * Stage 1: Streaming Source recover from state, report state to agg function.
 * Stage 2: Batch Source read data, Streaming Source wait.
 * Stage 3: Batch Finished, Streaming Source reading.
 */
public class HybridSourceAggregateFunction implements AggregateFunction<
		HybridSourceAggregateFunction.Input,
		HybridSourceAggregateFunction.Aggregator,
		HybridSourceAggregateFunction.Output>, Serializable {
	private static final long serialVersionUID = 1L;

	private static final Logger LOG = LoggerFactory.getLogger(HybridSourceAggregateFunction.class);

	@Override
	public Aggregator createAccumulator() {
		return new Aggregator();
	}

	@Override
	public Aggregator add(Input value, Aggregator accumulator) {
		if (value.inputType == InputType.STREAM_WAITING || value.inputType == InputType.BATCH_WAITING) {
			return accumulator;
		}

		if (value.inputType == InputType.STREAM_REPORT) {
			accumulator.batchFinished = value.batchFinished;
			accumulator.streamReported = true;
		} else { // batch report
			accumulator.totalBatchTasks = value.totalBatchTasks;
			if (value.currentBatchTaskFinished) {
				accumulator.finishedBatchTasks.add(value.currentBatchTaskId);
				if (accumulator.finishedBatchTasks.size() == accumulator.totalBatchTasks) {
					accumulator.batchFinished = true;
				}
			}
		}
		LOG.debug("Got message: {}, aggregator after added is: {}", value, accumulator);

		return accumulator;
	}

	@Override
	public Output getResult(Aggregator accumulator) {
		Output output = new Output();
		output.batchFinished = accumulator.batchFinished;
		output.streamReported = accumulator.streamReported;

		LOG.debug("getResult: {}", output);
		return output;
	}

	@Override
	public Aggregator merge(Aggregator a, Aggregator b) {
		if (a.batchFinished || b.batchFinished) {
			a.batchFinished = true;
			return a;
		}
		a.finishedBatchTasks.addAll(b.finishedBatchTasks);
		return a;
	}

	/**
	 * The aggregator.
	 */
	public static class Aggregator implements Serializable {
		private static final long serialVersionUID = 1L;

		boolean batchFinished;
		boolean streamReported;

		int totalBatchTasks;
		Set<Integer> finishedBatchTasks = new HashSet<>();

		@Override
		public String toString() {
			return String.format("Aggregator{batchFinished=%s, streamReported=%s, totalBatchTasks=%s" +
				", finishedBatchTasks=%s}",
				batchFinished, streamReported, totalBatchTasks, finishedBatchTasks);
		}
	}

	/**
	 * The input type for aggregate function.
	 */
	public enum InputType {
		STREAM_REPORT,
		STREAM_WAITING,
		BATCH_REPORT,
		BATCH_WAITING
	}

	/**
	 * The input for aggregate function.
	 */
	public static class Input implements Serializable {
		private static final long serialVersionUID = 1L;

		public InputType inputType;

		public boolean batchFinished = false;

		public int totalBatchTasks;
		public int currentBatchTaskId;
		public boolean currentBatchTaskFinished;

		@Override
		public String toString() {
			return String.format("Input{inputType=%s, batchFinished=%s, totalBatchTasks=%s, " +
				"currentBatchTaskId=%s, currentBatchTaskFinished=%s}",
				inputType, batchFinished, totalBatchTasks, currentBatchTaskId, currentBatchTaskFinished);
		}
	}

	/**
	 * The output for aggregate function.
	 */
	public static class Output implements Serializable {
		private static final long serialVersionUID = 1L;

		public boolean streamReported;
		public boolean batchFinished;

		@Override
		public String toString() {
			return String.format("Output{streamReported=%s, batchFinished=%s}", streamReported, batchFinished);
		}
	}
}
