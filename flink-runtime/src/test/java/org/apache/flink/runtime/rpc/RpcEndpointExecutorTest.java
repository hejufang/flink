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

package org.apache.flink.runtime.rpc;

import org.apache.flink.core.fs.CloseableRegistry;
import org.apache.flink.runtime.concurrent.MainScheduledExecutorService;

import org.apache.flink.util.TestLogger;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Base test for {@link RpcEndpoint} with {@link MainScheduledExecutorService}.
 */
public class RpcEndpointExecutorTest extends TestLogger {

	/**
	 * Assert the resource count in the {@link CloseableRegistry}.
	 *
	 * @param resourceCount the resource count
	 * @param closeableRegistry the given closeable registry
	 */
	protected void assertResourceCount(int resourceCount, CloseableRegistry closeableRegistry) {
		assertFalse(closeableRegistry.isClosed());
		assertEquals(resourceCount, closeableRegistry.getNumberOfRegisteredCloseables());
	}

	/**
	 * Assert the given {@link CloseableRegistry} is closed.
	 *
	 * @param closeableRegistry the given closeable registry
	 */
	protected void assertResourceClosed(CloseableRegistry closeableRegistry) {
		assertTrue(closeableRegistry.isClosed());
		assertEquals(0, closeableRegistry.getNumberOfRegisteredCloseables());
	}
}
