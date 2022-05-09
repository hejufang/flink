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

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.atomic.AtomicReference;

/**
 * The listener for socket push job results.
 */
public class SocketPushJobResultListener implements SocketJobResultListener {
	private final CountDownLatch latch;
	private final AtomicReference<Exception> exceptionHolder;

	public SocketPushJobResultListener() {
		this.latch = new CountDownLatch(1);
		this.exceptionHolder = new AtomicReference<>();
	}

	@Override
	public void fail(Exception exception) {
		if (exceptionHolder.compareAndSet(null, exception)) {
			latch.countDown();
		}
	}

	@Override
	public void success() {
		if (exceptionHolder.get() == null) {
			latch.countDown();
		}
	}

	@Override
	public void await() throws Exception {
		latch.await();
		if (exceptionHolder.get() != null) {
			throw exceptionHolder.get();
		}
	}
}
