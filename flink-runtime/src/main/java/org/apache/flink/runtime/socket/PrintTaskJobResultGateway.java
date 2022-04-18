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

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nonnull;

/**
 * Just print and ignore the job results.
 */
public class PrintTaskJobResultGateway implements TaskJobResultGateway {
	private static final Logger LOG = LoggerFactory.getLogger(PrintTaskJobResultGateway.class);

	@Override
	public void connect(String address, int port) throws Exception { }

	@Override
	public void sendResult(JobID jobId, @Nonnull byte[] data, ResultStatus resultStatus) {
		LOG.info("Print result for job {} with status {}", jobId, resultStatus);
	}

	@Override
	public void close() { }
}
