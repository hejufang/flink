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

package org.apache.flink.runtime.blacklist;

import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutor;
import org.apache.flink.runtime.concurrent.ComponentMainThreadExecutorServiceAdapter;
import org.apache.flink.runtime.concurrent.ManuallyTriggeredScheduledExecutor;

/**
 * Utility class of blacklist tests.
 */
public class BlacklistTestUtils {

	public static void triggerPeriodicScheduledTasksInMainThreadExecutor(ManuallyTriggeredScheduledExecutor executor) {
		// 1. trigger periodic tasks, then the executor might call execute() of MainThreadExecutor.
		executor.triggerPeriodicScheduledTasks();
		// 2. trigger all execute() of MainThreadExecutor.
		executor.triggerAll();
	}

	public static ComponentMainThreadExecutor createManuallyTriggeredMainThreadExecutor(ManuallyTriggeredScheduledExecutor executor) {
		final Thread main = Thread.currentThread();
		return new ComponentMainThreadExecutorServiceAdapter(
				executor,
				main);
	}

}
