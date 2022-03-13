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

package org.apache.flink.client.deployment;

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.ClusterClient;
import org.apache.flink.client.program.ClusterClientProvider;
import org.apache.flink.core.execution.JobClient;
import org.apache.flink.util.CloseableIterator;

/**
 * An implementation of the {@link JobClient} interface that uses a socket {@link ClusterClient} underneath..
 */
public class SocketClusterClientJobClientAdapter<ClusterID> extends ClusterClientJobClientAdapter<ClusterID> {
	private final CloseableIterator<?> resultIterator;

	public SocketClusterClientJobClientAdapter(
			CloseableIterator<?> resultIterator,
			ClusterClientProvider<ClusterID> clusterClientProvider,
			JobID jobID) {
		super(clusterClientProvider, jobID);
		this.resultIterator = resultIterator;
	}

	@Override
	public <T> CloseableIterator<T> getJobResultIterator(ClassLoader userClassLoader) {
		return (CloseableIterator<T>) resultIterator;
	}
}
