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
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.base.ListSerializer;
import org.apache.flink.core.memory.DataInputViewStreamWrapper;
import org.apache.flink.util.CloseableIterator;
import org.apache.flink.util.SerializedThrowable;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.LinkedList;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Iterator of {@link JobSocketResult}.
 * @param <T> The result data.
 */
public class SocketResultIterator<T> implements CloseableIterator<T> {
	private final JobID jobId;
	private final BlockingQueue<JobSocketResult> resultList;
	private final Queue<T> resultValueQueue;
	private boolean hasMore;
	private ListSerializer<T> resultSerializer;
	private SerializedThrowable serializedThrowable;

	public SocketResultIterator(JobID jobId, BlockingQueue<JobSocketResult> resultList) {
		this.jobId = jobId;
		this.resultList = resultList;
		this.resultValueQueue = new LinkedList<>();
		this.hasMore = true;
		this.resultSerializer = null;
		this.serializedThrowable = null;
	}

	@Override
	public void close() throws Exception {
		resultList.clear();
		resultValueQueue.clear();
	}

	@Override
	public boolean hasNext() {
		if (serializedThrowable != null) {
			throw new RuntimeException(serializedThrowable.getFullStringifiedStackTrace());
		}

		if (resultValueQueue.isEmpty()) {
			fetchNextResult();
			return !resultValueQueue.isEmpty();
		}
		return true;
	}

	@Override
	public T next() {
		if (serializedThrowable != null) {
			throw new RuntimeException(serializedThrowable.getFullStringifiedStackTrace());
		}

		if (resultValueQueue.isEmpty()) {
			throw new RuntimeException("All the data has been taken away");
		}

		return resultValueQueue.poll();
	}

	public void registerResultSerializer(TypeSerializer<T> serializer) {
		checkNotNull(serializer);
		resultSerializer = new ListSerializer<>(serializer);
	}

	private void fetchNextResult() {
		try {
			while (hasMore) {
				JobSocketResult jobSocketResult = resultList.take();
				if (!jobSocketResult.getJobId().equals(jobId)) {
					hasMore = false;
					serializedThrowable = new SerializedThrowable(
						new Exception("Receive result for " + jobSocketResult.getJobId() + " while current job is " + jobId));
					return;
				}
				hasMore = !jobSocketResult.isFinish();
				if (jobSocketResult.isFailed()) {
					serializedThrowable = checkNotNull(jobSocketResult.getSerializedThrowable());
					throw new RuntimeException(serializedThrowable.getFullStringifiedStackTrace());
				}
				if (jobSocketResult.getResult() != null) {
					if (resultSerializer != null) {
						try {
							resultValueQueue.addAll(
								resultSerializer.deserialize(
									new DataInputViewStreamWrapper(
										new ByteArrayInputStream((byte[]) jobSocketResult.getResult()))));
						} catch (IOException e) {
							serializedThrowable = new SerializedThrowable(e);
							hasMore = false;
							return;
						}
						if (resultValueQueue.isEmpty()) {
							continue;
						}
					} else {
						resultValueQueue.add((T) jobSocketResult.getResult());
					}
					return;
				}
			}
		} catch (InterruptedException e) {
			serializedThrowable = new SerializedThrowable(e);
			hasMore = false;
		}
	}
}
