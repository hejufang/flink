/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p/>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p/>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state.watchdog;

import static org.apache.flink.util.Preconditions.checkArgument;
import static org.apache.flink.util.Preconditions.checkNotNull;

/** Helper class used to construct watchdogs with the params. */
public class RocksDBWatchdogProvider {

	private final long timeoutMillis;
	private final RocksDBWatchdog.TimeoutHandler timeoutHandler;

	public RocksDBWatchdogProvider(long timeoutMillis, RocksDBWatchdog.TimeoutHandler timeoutHandler) {
		checkArgument(timeoutMillis >= 0, "Timeout needs to be >= 0.");
		this.timeoutMillis = timeoutMillis;
		this.timeoutHandler = checkNotNull(timeoutHandler);
	}

	public RocksDBWatchdog provide() {
		return new RocksDBWatchdog(timeoutMillis, timeoutHandler);
	}
}
