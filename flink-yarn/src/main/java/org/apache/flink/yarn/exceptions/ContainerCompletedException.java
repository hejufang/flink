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
 *
 */

package org.apache.flink.yarn.exceptions;

/**
 * Container Completed Exception.
 */
public class ContainerCompletedException extends Exception {
	static final long serialVersionUID = 1L;
	private final int exitCode;

	public ContainerCompletedException(int exitCode, String errorMessage) {
		super(errorMessage);
		this.exitCode = exitCode;
	}

	public int getExitCode() {
		return exitCode;
	}

	public static ContainerCompletedException fromExitCode(int exitCode, String errorMessage) {
		switch (exitCode) {
			case -103: // virtual memory over limit.
			case -104: // physical memory over limit.
			case -10001: // disk usage over limit.
			case -10002: // load over limit.
			case -10003: // log dir usage over limit.
			case -10004: // work dir usage over limit.
			case -10005: // shuffle disk usage over limit.
			case -10006: // total disk usage over limit.
				return new ExpectedContainerCompletedException(exitCode, errorMessage);
			default:
				return new ContainerCompletedException(exitCode, errorMessage);
		}
	}
}
