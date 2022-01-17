/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.state;

/**
 * Configuration of local state management to limit the size of local state.
 */
public class LocalStateManageConfig {
	private final long expectedMaxLocalStateSize;
	private final long actualMaxLocalStateSize;
	private final boolean failExceedQuotaTask;

	public LocalStateManageConfig(long expectedMaxLocalStateSize, long actualMaxLocalStateSize, boolean failExceedQuotaTask) {
		this.expectedMaxLocalStateSize = expectedMaxLocalStateSize;
		this.actualMaxLocalStateSize = actualMaxLocalStateSize;
		this.failExceedQuotaTask = failExceedQuotaTask;
	}

	public long getExpectedMaxLocalStateSize() {
		return expectedMaxLocalStateSize;
	}

	public long getActualMaxLocalStateSize() {
		return actualMaxLocalStateSize;
	}

	public boolean isFailExceedQuotaTask() {
		return failExceedQuotaTask;
	}

	@Override
	public String toString() {
		return "LocalStateManageConfig{" +
			"expectedMaxLocalStateSize=" + expectedMaxLocalStateSize +
			", actualMaxLocalStateSize=" + actualMaxLocalStateSize +
			", failExceedQuotaTask=" + failExceedQuotaTask +
			'}';
	}
}
