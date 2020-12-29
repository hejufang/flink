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

package org.apache.flink.table.runtime.operators.bundle;

import org.apache.flink.api.common.functions.util.FunctionUtils;
import org.apache.flink.metrics.Gauge;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.ChainingStrategy;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.runtime.context.ExecutionContextImpl;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTrigger;
import org.apache.flink.table.runtime.operators.bundle.trigger.BundleTriggerCallback;
import org.apache.flink.table.runtime.util.StreamRecordCollector;
import org.apache.flink.util.Collector;

import java.util.ArrayList;
import java.util.List;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * The {@link ListBundleOperator} simply used a java List to store the input elements.
 * and register itself as {@link BundleTriggerCallback} to receive callback by {@link BundleTrigger}
 * @param <IN> The input data type
 * @param <OUT> The output data type
 */
public class ListBundleOperator<IN, OUT> extends AbstractStreamOperator<OUT>
		implements OneInputStreamOperator<IN, OUT>, BundleTriggerCallback {

	private static final long serialVersionUID = 1L;

	/** The list in heap to store elements. */
	private transient List<IN> bundle;

	/** The trigger that determines how many elements should be put into a bundle. */
	private final BundleTrigger<IN> bundleTrigger;

	/** The function used to process when receiving element. */
	private final ListBundleFunction<IN, OUT> function;

	/** Output for stream records. */
	private transient Collector<OUT> collector;

	private transient int numOfElements = 0;

	public ListBundleOperator(
			ListBundleFunction<IN, OUT> function,
			BundleTrigger<IN> bundleTrigger) {
		chainingStrategy = ChainingStrategy.ALWAYS;
		this.function = checkNotNull(function, "function is null");
		this.bundleTrigger = checkNotNull(bundleTrigger, "bundleTrigger is null");
	}

	@Override
	public void open() throws Exception {
		super.open();
		function.open(new ExecutionContextImpl(this, getRuntimeContext()));

		this.numOfElements = 0;
		this.collector = new StreamRecordCollector<>(output);
		this.bundle = new ArrayList<>();

		bundleTrigger.registerCallback(this);
		// reset trigger
		bundleTrigger.reset();
		LOG.info("BundleOperator's trigger info: " + bundleTrigger.explain());

		// counter metric to get the size of bundle
		getRuntimeContext().getMetricGroup().gauge("bundleSize", (Gauge<Integer>) () -> numOfElements);
	}

	@Override
	public void processElement(StreamRecord<IN> element) throws Exception {
		long startTimestamp = System.nanoTime();
		// get the key and value for the map bundle
		final IN input = element.getValue();

		IN newInput = function.addInput(input);
		bundle.add(newInput);

		numOfElements++;
		bundleTrigger.onElement(input);
		getOperatorLatency().update((System.nanoTime() - startTimestamp) / 1000);
	}

	@Override
	public void finishBundle() throws Exception {
		if (!bundle.isEmpty()) {
			numOfElements = 0;
			function.finishBundle(bundle, collector);
			bundle.clear();
		}
		bundleTrigger.reset();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		finishBundle();
		super.processWatermark(mark);
	}

	@Override
	public void prepareSnapshotPreBarrier(long checkpointId) throws Exception {
		finishBundle();
	}

	@Override
	public void close() throws Exception {
		try {
			finishBundle();
		} finally {
			Exception exception = null;

			try {
				super.close();
				if (function != null) {
					FunctionUtils.closeFunction(function);
				}
			} catch (InterruptedException interrupted) {
				exception = interrupted;

				Thread.currentThread().interrupt();
			} catch (Exception e) {
				exception = e;
			}

			if (exception != null) {
				LOG.warn("Errors occurred while closing the BundleOperator.", exception);
			}
		}
	}
}
