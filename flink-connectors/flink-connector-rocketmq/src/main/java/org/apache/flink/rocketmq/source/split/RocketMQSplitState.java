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

import java.util.Objects;

/** The split state to track a mutable current offset. */
public class RocketMQSplitState extends RocketMQSplit {

	private Long currentOffset;

	public RocketMQSplitState(RocketMQSplit rocketMQSplit, Long currentOffset) {
		super(
				rocketMQSplit.getRocketMQBaseSplit(),
				rocketMQSplit.getStartingOffset(),
				rocketMQSplit.getStoppingOffset().orElse(NO_STOPPING_OFFSET));
		this.currentOffset = currentOffset;
	}

	public RocketMQSplitState(RocketMQSplit rocketMQSplit) {
		this(rocketMQSplit, null);
	}

	public Long getCurrentOffset() {
		return currentOffset;
	}

	public void setCurrentOffset(long currentOffset) {
		this.currentOffset = currentOffset;
	}

	/**
	 * Convert SplitState to Split.
	 * @return
	 */
	public RocketMQSplit toRocketMQSplit() {
		return new RocketMQSplit(
				getRocketMQBaseSplit(),
				getCurrentOffset(),
				getStoppingOffset().orElse(NO_STOPPING_OFFSET));
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof RocketMQSplitState)) {
			return false;
		}
		if (!super.equals(o)) {
			return false;
		}
		RocketMQSplitState that = (RocketMQSplitState) o;
		return Objects.equals(currentOffset, that.currentOffset);
	}

	@Override
	public int hashCode() {
		return Objects.hash(super.hashCode(), currentOffset);
	}

	@Override
	public String toString() {
		return "RocketMQSplitState{" +
			"RocketMQSplit=" + super.toString() +
			", currentOffset=" + currentOffset +
			'}';
	}
}
