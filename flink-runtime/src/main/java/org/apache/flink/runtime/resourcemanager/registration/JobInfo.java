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

package org.apache.flink.runtime.resourcemanager.registration;

import org.apache.flink.annotation.VisibleForTesting;

import java.util.Objects;

/**
 * Job information, such as minimum required slot num of job.
 */
public class JobInfo implements java.io.Serializable {
	private final int minSlotsNum;
	private final int initialTaskManagers;
	private final int initialExtraTaskManagers;

	@VisibleForTesting
	public JobInfo(int minSlotsNum) {
		this(minSlotsNum, 1, 0);
	}

	public JobInfo(int minSlotsNum, int initialTaskManagers, int extraTaskManagers) {
		this.minSlotsNum = minSlotsNum;
		this.initialTaskManagers = initialTaskManagers;
		this.initialExtraTaskManagers = extraTaskManagers;
	}

	public int getMinSlotsNum() {
		return minSlotsNum;
	}

	public int getInitialTaskManagers() {
		return initialTaskManagers;
	}

	public int getInitialExtraTaskManagers() {
		return initialExtraTaskManagers;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobInfo jobInfo = (JobInfo) o;
		return minSlotsNum == jobInfo.minSlotsNum &&
			initialTaskManagers == jobInfo.initialTaskManagers &&
			initialExtraTaskManagers == jobInfo.initialExtraTaskManagers;
	}

	@Override
	public int hashCode() {
		return Objects.hash(minSlotsNum, initialTaskManagers, initialExtraTaskManagers);
	}

	@Override
	public String toString() {
		return "JobInfo{" +
			"minSlotsNum=" + minSlotsNum +
			", initialTaskManagers=" + initialTaskManagers +
			", initialExtraTaskManagers=" + initialExtraTaskManagers +
			'}';
	}
}
