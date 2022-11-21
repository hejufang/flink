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

package org.apache.flink.runtime.io.network.api.writer;

import org.apache.flink.api.common.JobID;
import org.apache.flink.core.io.IOReadableWritable;
import org.apache.flink.runtime.taskmanager.TaskThreadPoolExecutor;

import static org.apache.flink.runtime.io.network.api.writer.RecordWriter.DEFAULT_OUTPUT_FLUSH_THREAD_NAME;

/**
 * Utility class to encapsulate the logic of building a {@link RecordWriter} instance.
 */
public class RecordWriterBuilder<T extends IOReadableWritable> {

	private ChannelSelector<T> selector = new RoundRobinChannelSelector<>();

	private long timeout = -1;

	private String taskName = "test";

	private String taskMetricNameWithSubtask = "test";

	private JobID jobId = null;

	private boolean cloudShuffleMode = false;

	private boolean isRecoverable = false;

	private TaskThreadPoolExecutor taskDaemonExecutor = null;

	public RecordWriterBuilder<T> setChannelSelector(ChannelSelector<T> selector) {
		this.selector = selector;
		return this;
	}

	public RecordWriterBuilder<T> setTimeout(long timeout) {
		this.timeout = timeout;
		return this;
	}

	public RecordWriterBuilder<T> setTaskName(String taskName) {
		this.taskName = taskName;
		return this;
	}

	public RecordWriterBuilder<T> setTaskMetricNameWithSubtask(String taskMetricNameWithSubtask) {
		this.taskMetricNameWithSubtask = taskMetricNameWithSubtask;
		return this;
	}

	public RecordWriterBuilder<T> setJobId(JobID jobId) {
		this.jobId = jobId;
		return this;
	}

	public RecordWriterBuilder<T> setCloudShuffleMode(boolean cloudShuffleMode) {
		this.cloudShuffleMode = cloudShuffleMode;
		return this;
	}

	public RecordWriterBuilder<T> setRecoverable(boolean recoverable) {
		isRecoverable = recoverable;
		return this;
	}

	public RecordWriterBuilder<T> setTaskDaemonExecutor(TaskThreadPoolExecutor taskDaemonExecutor) {
		this.taskDaemonExecutor = taskDaemonExecutor;
		return this;
	}

	public RecordWriter<T> build(ResultPartitionWriter writer) {
		String threadName = taskDaemonExecutor == null ? null : DEFAULT_OUTPUT_FLUSH_THREAD_NAME + " for " + jobId + "-" + taskMetricNameWithSubtask;
		if (selector.isBroadcast()) {
			if (cloudShuffleMode) {
				return new CloudShuffleBroadcastRecordWriter<>(writer, timeout, taskName);
			} else if (isRecoverable) {
				return new RecoverableBroadcastRecordWriter<>(writer, selector, timeout, taskName);
			} else {
				return new BroadcastRecordWriter<>(writer, timeout, taskName, threadName, taskDaemonExecutor);
			}
		} else {
			if (cloudShuffleMode) {
				return new CloudShuffleChannelSelectorRecordWriter<>(writer, selector, timeout, taskName);
			} else {
				return new ChannelSelectorRecordWriter<>(writer, selector, timeout, taskName, threadName, taskDaemonExecutor);
			}
		}
	}
}
