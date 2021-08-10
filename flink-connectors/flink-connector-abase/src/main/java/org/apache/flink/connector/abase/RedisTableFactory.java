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

package org.apache.flink.connector.abase;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.connector.abase.utils.Constants;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.connector.abase.descriptors.AbaseConfigs.CLUSTER;

/**
 * Factory for creating configured instances of {@link AbaseTableSource} and {@link AbaseTableSink}.
 */
public class RedisTableFactory extends AbaseTableFactory {

	@Override
	public String factoryIdentifier() {
		return Constants.REDIS_IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> requiredOptions = new HashSet<>();
		requiredOptions.add(CLUSTER);
		return requiredOptions;
	}

}
