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

package com.bytedance.flink.topology;

import com.bytedance.flink.configuration.Constants;

/**
 * Group type enum.
 */
public enum GroupType {

	/**
	 * The output elements are shuffled uniformly randomly to the next operation.
	 */
	SHUFFLE,

	/**
	 * The output elements are forwarded to the local subtask of the next operation.
	 * This require the parallelism of down stream is the same with up stream.
	 */
	FORWARD,

	/**
	 * The output elements are distributed evenly to a subset of instances of the next operation
	 * in a round-robin fashion.
	 */
	RESCALE,

	/**
	 * The output elements are broadcast to every parallel instance of the next operation.
	 */
	BROADCAST,

	/**
	 * The output elements are distributed evenly to instances of the next operation in a round-robin
	 * fashion.
	 */
	REBALANCE,


	/**
	 * It creates a new KeyedStream that uses the provided key for partitioning its operator states.
	 */
	KEY_BY;

	public static GroupType parse(String name) {
		if (name == null) {
			throw new RuntimeException("Group type cannot be null");
		}
		switch (name) {
			case Constants.SHUFFLE:
				return SHUFFLE;
			case Constants.FORWARD:
				return FORWARD;
			case Constants.LOCAL_FIRST:
				return RESCALE;
			case Constants.RESCALE:
				return RESCALE;
			case Constants.BROADCAST:
				return BROADCAST;
			case Constants.ALL:
				return BROADCAST;
			case Constants.REBALANCE:
				return REBALANCE;
			case Constants.KEY_BY:
				return KEY_BY;
			case Constants.FIELDS:
				return KEY_BY;
		}
		throw new RuntimeException(String.format("Unsupported group type: %s", name));
	}
}
