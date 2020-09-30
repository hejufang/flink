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

package org.apache.flink.runtime.scheduler;

/**
 * Deployment option which indicates whether the task should send scheduleOrUpdateConsumer message to master.
 */
public class DeploymentOption {

	private final boolean sendScheduleOrUpdateConsumerMessage;

	private final boolean deployCopy;

	private final boolean notifyConsumer;

	public DeploymentOption(boolean sendScheduleOrUpdateConsumerMessage) {
		this(sendScheduleOrUpdateConsumerMessage, false, false);
	}

	public DeploymentOption(boolean sendScheduleOrUpdateConsumerMessage, boolean deployCopy) {
		this(sendScheduleOrUpdateConsumerMessage, deployCopy, false);
	}

	public DeploymentOption(
			boolean sendScheduleOrUpdateConsumerMessage,
			boolean deployCopy,
			boolean notifyConsumer) {
		this.sendScheduleOrUpdateConsumerMessage = sendScheduleOrUpdateConsumerMessage;
		this.deployCopy = deployCopy;
		this.notifyConsumer = notifyConsumer;
	}

	public boolean sendScheduleOrUpdateConsumerMessage() {
		return sendScheduleOrUpdateConsumerMessage;
	}

	public boolean isDeployCopy() {
		return deployCopy;
	}

	public boolean isNotifyConsumer() {
		return notifyConsumer;
	}

	/**
	 * Builder for {@link DeploymentOption}.
	 */
	public static class Builder {

		boolean sendScheduleOrUpdateConsumerMessage;
		boolean deployCopy;
		boolean notifyConsumer;

		public Builder() {}

		public Builder sendScheduleOrUpdateConsumerMessage(boolean sendScheduleOrUpdateConsumerMessage) {
			this.sendScheduleOrUpdateConsumerMessage = sendScheduleOrUpdateConsumerMessage;
			return this;
		}

		public Builder deployCopy(boolean deployCopy) {
			this.deployCopy = deployCopy;
			return this;
		}

		public Builder notifyConsumer(boolean notifyConsumer) {
			this.notifyConsumer = notifyConsumer;
			return this;
		}

		public DeploymentOption build() {
			return new DeploymentOption(sendScheduleOrUpdateConsumerMessage, deployCopy, notifyConsumer);
		}
	}
}
