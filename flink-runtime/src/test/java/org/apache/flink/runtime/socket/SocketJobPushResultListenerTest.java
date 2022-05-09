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

package org.apache.flink.runtime.socket;

import org.junit.Test;

import static org.apache.flink.core.testutils.CommonTestUtils.assertThrows;

/**
 * Test case for {@link SocketJobResultListener}.
 */
public class SocketJobPushResultListenerTest {
	@Test
	public void testMultipleSuccess() throws Exception {
		SocketJobResultListener listener = new SocketPushJobResultListener();
		listener.success();
		listener.success();
		listener.success();
		listener.await();
	}

	@Test
	public void testMultipleFailed() throws Exception {
		SocketJobResultListener listener = new SocketPushJobResultListener();
		listener.fail(new Exception("first fail"));
		listener.fail(new Exception("second fail"));
		listener.success();
		listener.success();
		listener.fail(new Exception("third fail"));
		assertThrows("first fail", Exception.class, () -> {
			listener.await();
			return null;
		});
	}
}
