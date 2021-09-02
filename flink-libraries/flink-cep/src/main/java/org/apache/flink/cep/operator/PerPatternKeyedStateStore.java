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

package org.apache.flink.cep.operator;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.state.MapState;
import org.apache.flink.api.common.state.MapStateDescriptor;
import org.apache.flink.api.common.typeutils.base.StringSerializer;
import org.apache.flink.runtime.state.DefaultKeyedStateStore;
import org.apache.flink.runtime.state.KeyedStateBackend;

import static java.util.Objects.requireNonNull;

/**
 * PerPatternKeyedStateStore.
 */
public class PerPatternKeyedStateStore extends DefaultKeyedStateStore {

	protected String pattern;

	public PerPatternKeyedStateStore(String pattern, KeyedStateBackend<?> keyedStateBackend, ExecutionConfig executionConfig) {
		super(keyedStateBackend, executionConfig);
		this.pattern = pattern;
	}

	@Override
	public <UK, UV> MapState<UK, UV> getMapState(MapStateDescriptor<UK, UV> stateProperties) {
		requireNonNull(stateProperties, "The state properties must not be null");
		try {
			stateProperties.initializeSerializerUnlessSet(executionConfig);
			return keyedStateBackend.getPartitionedState(
				pattern,
				StringSerializer.INSTANCE,
				stateProperties);
		} catch (Exception e) {
			throw new RuntimeException("Error while getting state", e);
		}
	}

}
