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

import java.util.function.Supplier;

/**
 * Testing BlacklistActions.
 */
public class TestingBlacklistActions implements BlacklistActions{
	Runnable notifyBlacklistUpdatedConsumer;
	Supplier<Integer> registeredWorkerNumberSupplier;

	public TestingBlacklistActions(Runnable notifyBlacklistUpdatedConsumer, Supplier<Integer> registeredWorkerNumberSupplier) {
		this.notifyBlacklistUpdatedConsumer = notifyBlacklistUpdatedConsumer;
		this.registeredWorkerNumberSupplier = registeredWorkerNumberSupplier;
	}

	@Override
	public void notifyBlacklistUpdated() {
		this.notifyBlacklistUpdatedConsumer.run();
	}

	@Override
	public int getRegisteredWorkerNumber() {
		return registeredWorkerNumberSupplier.get();
	}

	/**
	 * Builder for {@link TestingBlacklistActions}.
	 */
	public static class TestingBlacklistActionsBuilder {
		Runnable notifyBlacklistUpdatedConsumer = () -> {};
		Supplier<Integer> registeredWorkerNumberSupplier = () -> 0;

		public TestingBlacklistActionsBuilder setNotifyBlacklistUpdatedConsumer(Runnable notifyBlacklistUpdatedConsumer) {
			this.notifyBlacklistUpdatedConsumer = notifyBlacklistUpdatedConsumer;
			return this;
		}

		public TestingBlacklistActionsBuilder setRegisteredWorkerNumberSupplier(Supplier<Integer> registeredWorkerNumberSupplier) {
			this.registeredWorkerNumberSupplier = registeredWorkerNumberSupplier;
			return this;
		}

		public TestingBlacklistActions build() {
			return new TestingBlacklistActions(notifyBlacklistUpdatedConsumer, registeredWorkerNumberSupplier);
		}
	}
}
