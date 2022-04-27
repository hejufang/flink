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

package org.apache.flink.runtime.util;

import org.apache.flink.runtime.rest.messages.taskmanager.ThreadDumpInfo;

import java.lang.management.ManagementFactory;
import java.lang.management.ThreadInfo;
import java.lang.management.ThreadMXBean;
import java.util.Arrays;
import java.util.Collection;
import java.util.stream.Collectors;

/**
 * Utilities for {@link java.lang.management.ManagementFactory}.
 */
public final class JvmUtils {

	/**
	 * Creates a thread dump of the current JVM.
	 *
	 * @return the thread dump of current JVM
	 */
	private static Collection<ThreadInfo> createThreadDump() {
		ThreadMXBean threadMxBean = ManagementFactory.getThreadMXBean();

		return Arrays.asList(threadMxBean.dumpAllThreads(true, true));
	}

	public static Collection<ThreadDumpInfo.ThreadInfo> createThreadDumpInfos() {
		final Collection<ThreadInfo> threadDump = JvmUtils.createThreadDump();

		return threadDump.stream()
			.map(threadInfo -> ThreadDumpInfo.ThreadInfo.create(threadInfo.getThreadName(), threadInfo.toString()))
			.collect(Collectors.toList());
	}

	/**
	 * Private default constructor to avoid instantiation.
	 */
	private JvmUtils() {}

}
