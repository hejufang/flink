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

package org.apache.flink.runtime.io.network.partition;

import java.io.IOException;

/**
 * Currently this exception can only be seen with the feature of single task failover and should not
 * happen very often. Here is the case: assuming that there exists TM-A and TM-B, TM-B goes down before sending its
 * credits to TM-A, which causes that TM-A cannot send more data because of the lack of the credits. And TM-A
 * doesn't know the tcp connection is dead until tcp's keep-alive time is passed, which is 7,200 seconds by default.
 */
public class TcpConnectionLostException extends IOException {

	private static final long serialVersionUID = 1L;

	public TcpConnectionLostException() {
		super("The downstream task manager is dead but this task manager knows nothing about it.");
	}
}
