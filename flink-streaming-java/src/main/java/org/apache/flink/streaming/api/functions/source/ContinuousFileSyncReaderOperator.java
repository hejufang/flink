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

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.io.InputFormat;
import org.apache.flink.api.common.io.RichInputFormat;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.streaming.api.operators.AbstractStreamOperator;
import org.apache.flink.streaming.api.operators.OneInputStreamOperator;
import org.apache.flink.streaming.api.operators.OutputTypeConfigurable;
import org.apache.flink.streaming.api.operators.StreamSourceContexts;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.streaming.runtime.tasks.ProcessingTimeService;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.function.RunnableWithException;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.flink.util.Preconditions.checkState;

/**
 * This operator that reads the {@link TimestampedFileInputSplit splits} received from the preceding
 * {@link ContinuousFileMonitoringFunction}. Contrary to the {@link ContinuousFileMonitoringFunction}
 * which has a parallelism of 1, this operator can have DOP > 1. Note that this operator reads data
 * synchronously rather than asynchronously. This is because in the broadcast join scenario, the dimension
 * table cannot be constructed until all the data is present.
 */

public class ContinuousFileSyncReaderOperator<OUT, T extends TimestampedInputSplit> extends AbstractStreamOperator<OUT>
	implements OneInputStreamOperator<T, OUT>, OutputTypeConfigurable<OUT> {

	private transient InputFormat<OUT, ? super T> format;

	private transient SourceFunction.SourceContext<OUT> sourceContext;

	private TypeSerializer<OUT> serializer;

	private transient OUT reusedRecord;

	private long readRecordsCounter;

	private long readSplitsCounter;

	private long totalReadSplitsMs;

	private long curWatermarkTs;

	ContinuousFileSyncReaderOperator(
		InputFormat<OUT, ? super T> format,
		ProcessingTimeService processingTimeService) {

		this.format = checkNotNull(format);
		this.processingTimeService = checkNotNull(processingTimeService);

	}

	@Override
	public void open() throws Exception {
		super.open();

		checkState(this.serializer != null, "The serializer has not been set. " +
			"Probably the setOutputType() was not called. Please report it.");

		if (this.format instanceof RichInputFormat) {
			((RichInputFormat) this.format).setRuntimeContext(getRuntimeContext());
		}
		this.format.configure(new Configuration());

		this.sourceContext = StreamSourceContexts.getSourceContext(
			getOperatorConfig().getTimeCharacteristic(),
			getProcessingTimeService(),
			new Object(),
			getContainingTask().getStreamStatusMaintainer(),
			output,
			getRuntimeContext().getExecutionConfig().getAutoWatermarkInterval(),
			-1);

		this.reusedRecord = serializer.createInstance();
	}

	@Override
	public void processElement(StreamRecord<T> element) throws Exception {

		if (this.format instanceof RichInputFormat) {
			((RichInputFormat) this.format).openInputFormat();
		}
		format.open(element.getValue());

		long readSplitsMS = System.currentTimeMillis();
		while (!format.reachedEnd()){
			OUT out = format.nextRecord(this.reusedRecord);
			if (out != null) {
				sourceContext.collect(out);
				this.readRecordsCounter++;
			} else {
				if (format.takeNullAsEndOfStream()) {
					break;
				}
			}
		}
		this.totalReadSplitsMs += System.currentTimeMillis() - readSplitsMS;
		this.readSplitsCounter++;

		format.close();
	}

	@Override
	public void processWatermark(Watermark mark) throws Exception {
		sourceContext.emitWatermark(mark);
		if (this.curWatermarkTs == 0L) {
			this.curWatermarkTs = mark.getTimestamp();
		} else if (this.curWatermarkTs == mark.getTimestamp() - 1L) {
			LOG.info("Read {} splits and {} records from hive to flink costs {} ms.", this.readSplitsCounter, this.readRecordsCounter, this.totalReadSplitsMs);
			this.totalReadSplitsMs = 0L;
			this.curWatermarkTs = 0L;
			this.readRecordsCounter = 0L;
			this.readSplitsCounter = 0L;
		}
	}

	@Override
	public void setOutputType(TypeInformation<OUT> outTypeInfo, ExecutionConfig executionConfig) {
		this.serializer = outTypeInfo.createSerializer(executionConfig);
	}

	@Override
	public void close() throws Exception {
		LOG.debug("closing");
		super.close();

		try {
			sourceContext.emitWatermark(Watermark.MAX_WATERMARK);
		} catch (Exception e) {
			LOG.warn("unable to emit watermark while closing", e);
		}

		cleanUp();
	}

	private void cleanUp() throws Exception {
		LOG.debug("cleanup");

		RunnableWithException[] runClose = {
			() -> sourceContext.close(),
			() -> output.close(),
			() -> format.close(),
			() -> {
				if (this.format instanceof RichInputFormat) {
					((RichInputFormat) this.format).closeInputFormat();
				}
			}};
		Exception firstException = null;

		for (RunnableWithException r : runClose) {
			try {
				r.run();
			} catch (Exception e) {
				firstException = ExceptionUtils.firstOrSuppressed(e, firstException);
			}
		}

		if (firstException != null) {
			throw firstException;
		}
	}

	@Override
	public void dispose() throws Exception {
		Exception e = null;
		{
			format = null;
			sourceContext = null;
			reusedRecord = null;
			serializer = null;
		}
		try {
			super.dispose();
		} catch (Exception ex) {
			e = ExceptionUtils.firstOrSuppressed(ex, e);
		}
		if (e != null) {
			throw e;
		}
	}

}
