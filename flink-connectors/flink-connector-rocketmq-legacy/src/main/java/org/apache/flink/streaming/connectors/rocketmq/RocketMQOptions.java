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

package org.apache.flink.streaming.connectors.rocketmq;

import static org.apache.flink.streaming.connectors.rocketmq.RocketMQConfig.BATCH_SIZE_DEFAULT;

/**
 * RocketMQ options.
 */
public class RocketMQOptions {
	private boolean async;
	private boolean batchFlushOnCheckpoint;
	private int batchSize;

	public RocketMQOptions(boolean async, boolean batchFlushOnCheckpoint, int batchSize) {
		this.async = async;
		this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
		this.batchSize = batchSize;
	}

	public boolean isAsync() {
		return async;
	}

	public boolean isBatchFlushOnCheckpoint() {
		return batchFlushOnCheckpoint;
	}

	public int getBatchSize() {
		return batchSize;
	}

	/**
	 * RocketMQ options builder.
	 * */
	public static class RocketMQOptionsBuilder {
		private boolean async;
		private boolean batchFlushOnCheckpoint;
		private int batchSize = BATCH_SIZE_DEFAULT;

		public RocketMQOptionsBuilder setAsync(boolean async) {
			this.async = async;
			return this;
		}

		public RocketMQOptionsBuilder setBatchFlushOnCheckpoint(boolean batchFlushOnCheckpoint) {
			this.batchFlushOnCheckpoint = batchFlushOnCheckpoint;
			return this;
		}

		public RocketMQOptionsBuilder setBatchSize(int batchSize) {
			this.batchSize = batchSize;
			return this;
		}

		public RocketMQOptions build() {
			return new RocketMQOptions(async, batchFlushOnCheckpoint, batchSize);
		}
	}
}
