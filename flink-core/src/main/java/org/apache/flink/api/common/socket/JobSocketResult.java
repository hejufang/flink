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

package org.apache.flink.api.common.socket;

import org.apache.flink.api.common.JobID;
import org.apache.flink.util.SerializedThrowable;

import javax.annotation.Nullable;

import java.io.Serializable;

/**
 * Job socket result is similar to {@link org.apache.flink.api.common.JobExecutionResult}. Dispatcher return
 * it to client with result status, data and exception information.
 */
public class JobSocketResult implements Serializable {
	private final JobID jobId;
	private final ResultStatus resultStatus;
	@Nullable
	private final Object result;
	@Nullable
	private final SerializedThrowable serializedThrowable;

	private JobSocketResult(
			JobID jobId,
			ResultStatus resultStatus,
			Object result,
			SerializedThrowable serializedThrowable) {
		this.jobId = jobId;
		this.resultStatus = resultStatus;
		this.result = result;
		this.serializedThrowable = serializedThrowable;
	}

	public boolean isFinish() {
		return resultStatus == ResultStatus.COMPLETE || resultStatus == ResultStatus.FAIL;
	}

	public boolean isFailed() {
		return resultStatus == ResultStatus.FAIL;
	}

	public JobID getJobId() {
		return jobId;
	}

	@Nullable
	public Object getResult() {
		return result;
	}

	@Nullable
	public SerializedThrowable getSerializedThrowable() {
		return serializedThrowable;
	}

	/**
	 * Factory of {@link JobSocketResult}.
	 */
	public static class Builder {
		private JobID jobId;
		private ResultStatus resultStatus;
		private Object result;
		private SerializedThrowable serializedThrowable;

		public Builder setJobId(JobID jobId) {
			this.jobId = jobId;
			return this;
		}

		public Builder setResultStatus(ResultStatus resultStatus) {
			this.resultStatus = resultStatus;
			return this;
		}

		public Builder setResult(Object result) {
			this.result = result;
			return this;
		}

		public Builder setSerializedThrowable(SerializedThrowable serializedThrowable) {
			this.serializedThrowable = serializedThrowable;
			return this;
		}

		public JobSocketResult build() {
			return new JobSocketResult(jobId, resultStatus, result, serializedThrowable);
		}
	}
}
