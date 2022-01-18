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

package org.apache.flink.rocketmq.source.enumerator;

import org.apache.flink.rocketmq.source.split.RocketMQSplitBase;

import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * EnumSplitManager.
 */
public class EnumSplitIdManager {
	private final Map<RocketMQSplitBase, Integer> split2idMap = new HashMap<>();
	private final AtomicInteger numAssignedSplit = new AtomicInteger();

	public void addAll(Collection<RocketMQSplitBase> splitBases) {
		List<RocketMQSplitBase> splitBaseList = new ArrayList<>(splitBases);
		splitBaseList.stream().sorted().forEach(
			split -> {
				if (!split2idMap.containsKey(split)) {
					split2idMap.put(split, numAssignedSplit.getAndIncrement());
				}
			}
		);
	}

	public int getSplitId(RocketMQSplitBase splitBase) {
		return split2idMap.get(splitBase);
	}
}
