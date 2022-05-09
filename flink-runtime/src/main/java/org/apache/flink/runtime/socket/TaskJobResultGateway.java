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

package org.apache.flink.runtime.socket;

import org.apache.flink.api.common.JobID;
import org.apache.flink.api.common.socket.ResultStatus;

import javax.annotation.Nullable;

import java.io.Closeable;

/**
 * Interface used to return job results by task.
 */
public interface TaskJobResultGateway extends Closeable {
	/**
	 * Connect to given address and port.
	 *
	 * @param address the server address
	 * @param port the server port
	 * @throws Exception the thrown exception
	 */
	void connect(String address, int port) throws Exception;

	/**
	 * Send the data of given job to job manager.
	 *
	 * @param jobId the given job id
	 * @param data the data of given job
	 * @param resultStatus status of the result
	 * @param listener the job result listener
	 */
	void sendResult(JobID jobId, @Nullable byte[] data, ResultStatus resultStatus, SocketJobResultListener listener);

	/**
	 * Close the result gateway.
	 */
	@Override
	void close();
}
