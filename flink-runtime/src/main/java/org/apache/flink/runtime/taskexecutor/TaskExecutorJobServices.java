/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.taskexecutor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.runtime.execution.librarycache.LibraryCacheManager;

/**
 * JobService used in TaskExecutor.
 */
public class TaskExecutorJobServices implements JobTable.JobServices{
	private final LibraryCacheManager.ClassLoaderLease classLoaderLease;

	private final Runnable closeHook;

	private TaskExecutorJobServices(
		LibraryCacheManager.ClassLoaderLease classLoaderLease,
		Runnable closeHook) {
		this.classLoaderLease = classLoaderLease;
		this.closeHook = closeHook;
	}

	@Override
	public LibraryCacheManager.ClassLoaderHandle getClassLoaderHandle() {
		return classLoaderLease;
	}

	@Override
	public void close() {
		classLoaderLease.release();
		closeHook.run();
	}

	@VisibleForTesting
	static TaskExecutorJobServices create(
		LibraryCacheManager.ClassLoaderLease classLoaderLease,
		Runnable closeHook) {
		return new TaskExecutorJobServices(
			classLoaderLease,
			closeHook);
	}
}
