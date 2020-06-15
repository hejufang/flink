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
package org.apache.flink.runtime.executiongraph.speculation;

import org.apache.flink.configuration.Configuration;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Loader for speculation strategy.
 */
public class SpeculationStrategyLoader {
	private static final Logger LOG = LoggerFactory.getLogger(SpeculationStrategyLoader.class);

	public static SpeculationStrategy.Factory loadSpeculationStrategy(Configuration config) {
		final boolean enabled = config.getBoolean(SpeculationOptions.SPECULATION_ENABLED);
		if (enabled) {
			LOG.info("Using {} as speculation strategy.", DefaultSpeculationStrategy.class.getName());
			return new DefaultSpeculationStrategy.Factory(config);
		} else {
			LOG.info("Using {} as speculation strategy.", NoOpSpeculationStrategy.class.getName());
			return new NoOpSpeculationStrategy.Factory();
		}
	}
}
