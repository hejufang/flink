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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import java.util.concurrent.ThreadLocalRandom;

/**
 * A round robin partitioner.
 */
public class RoundRobinPartitioner<T> extends FlinkKafkaPartitioner<T> {
	private int current = -1;

	@Override
	public int partition(T record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		if (current < 0) {
			current = ThreadLocalRandom.current().nextInt(0, partitions.length);
		}
		current += 1;
		if (current >= partitions.length) {
			current = 0;
		}
		return partitions[current];
	}
}
