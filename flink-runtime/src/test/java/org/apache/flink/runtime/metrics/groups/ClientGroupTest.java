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

package org.apache.flink.runtime.metrics.groups;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.MetricOptions;
import org.apache.flink.runtime.metrics.MetricRegistryConfiguration;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.metrics.util.DummyCharacterFilter;
import org.apache.flink.util.TestLogger;

import org.junit.Test;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * Tests for the {@link ClientMetricGroup}.
 */
public class ClientGroupTest extends TestLogger {

	// ------------------------------------------------------------------------
	//  scope name tests
	// ------------------------------------------------------------------------

	@Test
	public void testGenerateScopeDefault() throws Exception {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		ClientMetricGroup group = new ClientMetricGroup(registry, "localhost");

		assertArrayEquals(new String[]{"localhost", "client"}, group.getScopeComponents());
		assertEquals("localhost.client.name", group.getMetricIdentifier("name"));

		registry.shutdown().get();
	}

	@Test
	public void testGenerateScopeCustom() throws Exception {
		Configuration cfg = new Configuration();
		cfg.setString(MetricOptions.SCOPE_NAMING_CLI, "constant.<host>.foo.<host>");
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.fromConfiguration(cfg));

		ClientMetricGroup group = new ClientMetricGroup(registry, "host");

		assertArrayEquals(new String[]{"constant", "host", "foo", "host"}, group.getScopeComponents());
		assertEquals("constant.host.foo.host.name", group.getMetricIdentifier("name"));

		registry.shutdown().get();
	}

	@Test
	public void testCreateQueryServiceMetricInfo() {
		MetricRegistryImpl registry = new MetricRegistryImpl(MetricRegistryConfiguration.defaultMetricRegistryConfiguration());
		ClientMetricGroup group = new ClientMetricGroup(registry, "host");

		Exception ex = null;
		try {
			group.createQueryServiceMetricInfo(new DummyCharacterFilter());
		} catch (UnsupportedOperationException e) {
			ex = e;
		}
		assertNotNull(ex);
	}
}
