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

package com.bytedance.flink.utils;

import com.bytedance.flink.configuration.Constants;
import com.bytedance.flink.pojo.BoltInfo;
import com.bytedance.flink.pojo.SpoutInfo;

import java.util.List;

/**
 * Validation utils.
 * */
public class Validation {
	public static void validateBoltInfo(BoltInfo boltInfo) {
		String prefix = String.format("Bolt (%s):", boltInfo.getName());
		validateString(boltInfo.getName(), prefix, Constants.BOLT_NAME);
		validateString(boltInfo.getInterpreter(), prefix, Constants.INTERPRETER);
		validateString(boltInfo.getScript(), prefix, Constants.SCRIPT);
		notNull(boltInfo.getGroupList(), prefix, Constants.GROUP_LIST);
		positive(boltInfo.getParallelism(), prefix, Constants.PARALLELISM);
	}

	public static void validateSpoutInfo(SpoutInfo spoutInfo) {
		String prefix = String.format("Spout (%s):", spoutInfo.getName());
		validateString(spoutInfo.getName(), prefix, Constants.SPOUT_NAME);
		validateString(spoutInfo.getInterpreter(), prefix, Constants.INTERPRETER);
		validateString(spoutInfo.getScript(), prefix, Constants.SCRIPT);
		if (!spoutInfo.isKafkaMultiSource()) {
			validatePositiveInteger(spoutInfo.getTotalPartition(), prefix, Constants.TOTAL_PARTITION);
			validatePositiveInteger(spoutInfo.getParallelism(), prefix, Constants.PARALLELISM);
		} else {
			validateNotEmptyList((List) spoutInfo.getArgs().get(Constants.KAFKA_SOURCE), prefix,
				Constants.KAFKA_SOURCE);
		}
	}

	public static void validateString(String value, String prefix, String name) {
		if (value == null || value.isEmpty()) {
			throw new RuntimeException(String.format("%s %s cannot be null or empty, current value is %s",
				prefix, name, value));
		}
	}

	public static void validateNotEmptyList(List value, String name) {
		validateNotEmptyList(value, "", name);
	}

	public static void validateNotEmptyList(List value, String prefix, String name) {
		if (value == null || value.isEmpty()) {
			throw new RuntimeException(String.format("%s %s cannot be null or empty, current value is %s",
				prefix, name, value));
		}
	}

	public static void validatePositiveInteger(Integer value, String prefix, String name) {
		if (value == null || value <= 0) {
			throw new RuntimeException(String.format("%s %s must be positive, current value is %s",
				prefix, name, value));
		}
	}

	public static void notNull(Object value, String prefix, String name) {
		if (value == null) {
			throw new RuntimeException(String.format("%s %s cannot be null, current value is %s",
				prefix, name, value));
		}
	}

	public static void positive(Integer value, String prefix, String name) {
		if (value == null || value <= 0) {
			throw new RuntimeException(String.format("%s %s must be greater than 0, current value is %s",
				prefix, name, value));
		}
	}
}
