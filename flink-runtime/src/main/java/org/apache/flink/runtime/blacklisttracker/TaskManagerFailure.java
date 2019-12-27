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

package org.apache.flink.runtime.blacklisttracker;

import org.apache.flink.runtime.taskmanager.TaskManagerLocation;

/**
 * TaskManager Failure, only the last Exception.
 */
public class TaskManagerFailure {
	private TaskManagerLocation taskManagerLocation;
	private String exception;
	private long timestamp;

	public TaskManagerFailure(TaskManagerLocation taskManagerLocation, String exception, Long timestamp) {
		this.taskManagerLocation = taskManagerLocation;
		this.exception = exception;
		this.timestamp = timestamp;
	}

	public TaskManagerLocation getTaskManagerLocation() {
		return taskManagerLocation;
	}

	public String getException() {
		return exception;
	}

	public long getTimestamp() {
		return timestamp;
	}

	@Override
	public String toString() {
		return "TaskManagerFailure{" +
				"taskManagerLocation='" + taskManagerLocation + '\'' +
				", exception=" + exception +
				", timestamp=" + timestamp +
				'}';
	}
}
