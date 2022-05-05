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

package org.apache.flink.streaming.api.functions.source;

import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.watermark.Watermark;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.flink.api.common.io.LoopInputSplitAssigner.TimeInputSplit;

/**
 * PeriodInputFormatSourceFunction.
 * @param <OUT>
 */
public class PeriodInputFormatSourceFunction<OUT> extends InputFormatSourceFunction<OUT> {
	private static final Logger LOG = LoggerFactory.getLogger(PeriodInputFormatSourceFunction.class);
	private static final long serialVersionUID = 1L;

	private final int countOfScanTimes;
	private transient Configuration configuration;
	private transient long loopStartTimestampMs;
	private volatile boolean isCanceled = false;

	public PeriodInputFormatSourceFunction(
			InputFormat<OUT, ?> format,
			TypeInformation<OUT> typeInfo,
			int countOfScanTimes) {
		super(format, typeInfo);
		this.countOfScanTimes = countOfScanTimes;
	}

	@Override
	public void open(Configuration parameters) throws Exception {
		super.open(parameters);
		this.configuration = parameters;
	}

	@Override
	public void run(SourceContext<OUT> ctx) throws Exception {
		int curCnt = 0;
		while ((countOfScanTimes < 0 || curCnt < countOfScanTimes) && !isCanceled) {
			while (!splitIterator.hasNext() && !isCanceled) {
				splitIterator = getInputSplits();
				LOG.warn("Other task still not finished, we will wait for it.");
				Thread.sleep(500);
			}

			TimeInputSplit timeInputSplit = (TimeInputSplit) splitIterator.next();
			isRunning = splitIterator.hasNext();
			loopStartTimestampMs = timeInputSplit.getTimestamp();
			ctx.emitWatermark(new Watermark(loopStartTimestampMs));
			long leftMs = 0;
			if (isRunning) {
				leftMs = runOnce(timeInputSplit, ctx);
				LOG.info("Task {} after read all data, left time is {} ms",
					getRuntimeContext().getIndexOfThisSubtask(), leftMs);
			} else {
				LOG.warn("All splits is finished, task {} not read any split",
					getRuntimeContext().getIndexOfThisSubtask());
				leftMs = timeInputSplit.getNextStartTimestamp() - System.currentTimeMillis();
			}

			// After end of this task, we will emit watermark.
			ctx.emitWatermark(new Watermark(loopStartTimestampMs + 1));

			initFormat(configuration, true);
			curCnt++;
			if (leftMs > 0) {
				Thread.sleep(leftMs);
			}
		}
	}

	private long runOnce(TimeInputSplit timeInputSplit, SourceContext<OUT> ctx) throws Exception {
		long loopNextTimestampMs = timeInputSplit.getNextStartTimestamp();
		long start = System.currentTimeMillis();
		LOG.info("Start run PeriodInputFormatSourceFunction with loopTime {}, system time {}",
			loopStartTimestampMs, start);

		super.run(ctx);
		LOG.info("End of run once, cost is {} ms", System.currentTimeMillis() - start);
		return loopNextTimestampMs - System.currentTimeMillis();
	}

	@Override
	protected void collect(OUT out, SourceContext<OUT> ctx) {
		ctx.collectWithTimestamp(out, loopStartTimestampMs);
	}

	@Override
	public boolean withSameWatermarkPerBatch() {
		return true;
	}

	@Override
	public void cancel() {
		isCanceled = true;
		super.cancel();
	}
}
