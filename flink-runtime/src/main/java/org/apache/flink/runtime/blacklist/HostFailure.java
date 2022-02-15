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

import org.apache.flink.runtime.clusterframework.types.ResourceID;
import org.apache.flink.runtime.throwable.ThrowableClassifier;
import org.apache.flink.runtime.throwable.ThrowableType;
import org.apache.flink.util.ExceptionUtils;
import org.apache.flink.util.FlinkException;

import java.util.Arrays;
import java.util.List;

/**
 * TaskManager Failure, only the last Exception.
 */
public class HostFailure {
	public static final List<Class<? extends Throwable> > STRIP_EXCEPTION_LIST = Arrays.asList(
			FlinkException.class);

	private final BlacklistUtil.FailureType failureType;
	private final String hostname;
	private final ResourceID resourceID;
	private final Throwable exception;
	private final long timestamp;
	private final boolean isCrtialError;

	public HostFailure(
			BlacklistUtil.FailureType failureType,
			String hostname,
			ResourceID resourceID,
			Throwable exception,
			long timestamp) {
		this.failureType = failureType;
		this.hostname = hostname;
		this.resourceID = resourceID;
		Throwable t = exception;
		for (Class<? extends Throwable> c : STRIP_EXCEPTION_LIST) {
			t = ExceptionUtils.stripException(t, c);
		}
		this.exception = t;
		this.timestamp = timestamp;
		if (ThrowableClassifier.findThrowableOfThrowableType(
			exception, ThrowableType.CriticalError).isPresent()) {
			isCrtialError = true;
		} else {
			isCrtialError = false;
		}
	}

	public BlacklistUtil.FailureType getFailureType() {
		return failureType;
	}

	public String getHostname() {
		return hostname;
	}

	public ResourceID getResourceID() {
		return resourceID;
	}

	public Throwable getException() {
		return exception;
	}

	public long getTimestamp() {
		return timestamp;
	}

	public boolean isCrtialError() {
		return isCrtialError;
	}

	@Override
	public String toString() {
		return "HostFailure{" +
				"failureType='" + failureType + '\'' +
				", hostname='" + hostname + '\'' +
				", resourceID='" + resourceID + '\'' +
				", exception=" + exception +
				", timestamp=" + timestamp +
				'}';
	}
}
