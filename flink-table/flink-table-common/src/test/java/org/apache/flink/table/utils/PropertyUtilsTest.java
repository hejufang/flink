/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License");; you may not use this file except in compliance
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

package org.apache.flink.table.utils;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.utils.PropertyUtils.reorderTheSchemaIndex;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link org.apache.flink.table.utils.PropertyUtils}.
 */
public class PropertyUtilsTest {
	@Test
	public void testReorderTheSchemaIndex() {
		Map<String, String> properties = new HashMap<>();
		//Assuming that the 2nd ~ 6th and 8th ~ 9th fields are removed.
		properties.put("schema.0.name", "name");
		properties.put("schema.0.type", "INT");
		properties.put("schema.1.name", "app_id");
		properties.put("schema.1.type", "INT");

		properties.put("schema.7.name", "enter_from");
		properties.put("schema.7.type", "VARCHAR");

		properties.put("schema.10.name", "entrance_show_ec_cnt");
		properties.put("schema.10.type", "INT");

		reorderTheSchemaIndex(properties);

		Map<String, String> expectedProperties = new HashMap<>();
		expectedProperties.put("schema.0.name", "name");
		expectedProperties.put("schema.0.type", "INT");
		expectedProperties.put("schema.1.name", "app_id");
		expectedProperties.put("schema.1.type", "INT");
		expectedProperties.put("schema.2.name", "enter_from");
		expectedProperties.put("schema.2.type", "VARCHAR");
		expectedProperties.put("schema.3.name", "entrance_show_ec_cnt");
		expectedProperties.put("schema.3.type", "INT");

		assertEquals(properties, expectedProperties);
	}
}
