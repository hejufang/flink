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

package org.apache.flink.runtime.socket;

/**
 * Listener for job task push results from task manager to netty server.
 */
public interface SocketJobResultListener {
	/**
	 * Notifier for task push results failed.
	 *
	 * @param exception the thrown exception
	 */
	void fail(Exception exception);

	/**
	 * Notifier for task push results finished.
	 */
	void success();

	/**
	 * Wait for the operation of push results to be finished.
	 *
	 * @throws Exception the thrown exception
	 */
	void await() throws Exception;

	/**
	 * Check SocketJobResult is still running.
	 *
	 * @return If SocketJobResultListener is running.
	 */
	boolean checkRunning();
}
