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

package org.apache.flink.rocketmq.source.split;

import org.apache.flink.api.connector.source.SourceSplit;

import java.util.Objects;
import java.util.Optional;

/** A {@link SourceSplit} for a RocketMQ partition. */
public class RocketMQSplit extends RocketMQSplitBase {

	public static final long NO_STOPPING_OFFSET = Long.MAX_VALUE;
	public static final long INIT_STARTING_OFFSET = -1;

	private final long startingOffset;
	private final long stoppingOffset;

	public RocketMQSplit(RocketMQSplitBase splitBase, long startingOffset) {
		this(splitBase, startingOffset, NO_STOPPING_OFFSET);
	}

	public RocketMQSplit(RocketMQSplitBase splitBase, long startingOffset, long stoppingOffset) {
		super(splitBase.getTopic(), splitBase.getBrokerName(), splitBase.getQueueId());
		this.startingOffset = startingOffset;
		this.stoppingOffset = stoppingOffset;
	}

	public long getStartingOffset() {
		return startingOffset;
	}

	public Optional<Long> getStoppingOffset() {
		return stoppingOffset > 0 ? Optional.of(stoppingOffset) : Optional.empty();
	}

	@Override
	public String toString() {
		return "RocketMQSplit{" +
			"RocketMQBaseSplit=" + super.toString() +
			", startingOffset=" + startingOffset +
			", stoppingOffset=" + stoppingOffset +
			'}';
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RocketMQSplit)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		RocketMQSplit that = (RocketMQSplit) o;
		return startingOffset == that.startingOffset && stoppingOffset == that.stoppingOffset;
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), startingOffset, stoppingOffset);
	}
}
