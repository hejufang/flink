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
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.SerializedThrowable;

import java.util.concurrent.BlockingQueue;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Iterator of {@link JobSocketResult}.
 * @param <T> The result data.
 */
public class SocketResultIterator<T> implements CloseableIterator<T> {
	private final JobID jobId;
	private final BlockingQueue<JobSocketResult> resultList;
	private boolean hasMore;
	private JobSocketResult result;

	public SocketResultIterator(JobID jobId, BlockingQueue<JobSocketResult> resultList) {
		this.jobId = jobId;
		this.resultList = resultList;
		this.hasMore = true;
		this.result = null;
	}

	@Override
	public void close() throws Exception {
		resultList.clear();
		hasMore = false;
		result = null;
	}

	@Override
	public boolean hasNext() {
		if (result == null) {
			if (hasMore) {
				fetchNextResult();
			} else {
				return false;
			}
		}
		return true;
	}

	@Override
	public T next() {
		if (result == null) {
			if (hasMore) {
				fetchNextResult();
			}
		}
		if (result != null) {
			if (result.isFailed()) {
				SerializedThrowable serializedThrowable = result.getSerializedThrowable();
				result = null;
				checkNotNull(serializedThrowable);
				throw new RuntimeException(serializedThrowable.getFullStringifiedStackTrace());
			} else {
				JobSocketResult current = result;
				result = null;
				return (T) current.getResult();
			}
		}
		return null;
	}

	private void fetchNextResult() {
		try {
			result = resultList.take();
			checkArgument(result.getJobId().equals(jobId), "Fetch job(" + result.getJobId() + ") results while current job is " + jobId);
			if (result.isFinish()) {
				hasMore = false;
			}
		} catch (InterruptedException e) {
			throw new RuntimeException(e);
		}
	}
}
