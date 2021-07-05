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

package org.apache.flink.runtime.state.cache;

import org.apache.flink.api.common.TaskInfo;
import org.apache.flink.configuration.MemorySize;

/**
 * Responsible for managing all registered {@link Cache} in {@link org.apache.flink.runtime.taskexecutor.TaskExecutor}.
 */
public interface CacheManager {

	/** Register a cache and return the corresponding {@link PolicyStats} object. */
	PolicyStats registerCache(TaskInfo taskInfo, String name, Cache cache, MemorySize initialSize) throws Exception;

	/** Unregister a cache and release the corresponding resources. */
	void unregisterCache(TaskInfo taskInfo, String name, Cache cache);

	/** Stop all cache-related services. */
	void shutdown() throws Exception;
}
