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

package org.apache.flink.table.planner;

import org.apache.flink.configuration.Configuration;
import org.apache.flink.table.api.TableConfig;
import org.apache.flink.table.plan.PlanCacheManager;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

/**
 * Test case for planner cache manager.
 */
public class PlanCacheManagerTest {
	@Test
	public void testPutAndGet() {
		Map<String, Long> catalogLsn10 = new HashMap<>();
		catalogLsn10.put("t1", 0L);

		Map<String, Long> catalogLsn11 = new HashMap<>();
		catalogLsn11.put("t1", 1L);

		Map<String, Long> catalogLsn20 = new HashMap<>();
		catalogLsn20.put("t2", 0L);

		Map<String, Long> catalogLsn21 = new HashMap<>();
		catalogLsn20.put("t2", 1L);

		TableConfig tableConfig = TableConfig.getDefault();
		PlanCacheManager<String> cache = new PlanCacheManager<>(10);
		cache.putPlan("select1", catalogLsn10, tableConfig, "value1");
		cache.putPlan("select2", catalogLsn21, tableConfig, "value2");

		assertTrue(cache.getPlan("select1", catalogLsn10, tableConfig).isPresent());
		assertEquals("value1", cache.getPlan("select1", catalogLsn10, tableConfig).get());
		assertFalse(cache.getPlan("select1", catalogLsn11, tableConfig).isPresent());

		assertFalse(cache.getPlan("select2", catalogLsn20, tableConfig).isPresent());
		assertTrue(cache.getPlan("select2", catalogLsn21, tableConfig).isPresent());
		assertEquals("value2", cache.getPlan("select2", catalogLsn21, tableConfig).get());
	}

	@Test
	public void testCacheCapacity() {
		Map<String, Long> catalogLsn0 = new HashMap<>();
		catalogLsn0.put("t1", 0L);

		Map<String, Long> catalogLsn1 = new HashMap<>();
		catalogLsn1.put("t1", 1L);

		TableConfig tableConfig = TableConfig.getDefault();
		PlanCacheManager<String> cache = new PlanCacheManager<>(3);
		cache.putPlan("select1", catalogLsn0, tableConfig, "value10");
		cache.putPlan("select2", catalogLsn0, tableConfig, "value20");
		cache.putPlan("select3", catalogLsn0, tableConfig, "value30");
		cache.putPlan("select1", catalogLsn1, tableConfig, "value11");
		cache.putPlan("select2", catalogLsn1, tableConfig, "value21");

		assertFalse(cache.getPlan("select1", catalogLsn0, tableConfig).isPresent());
		assertFalse(cache.getPlan("select2", catalogLsn0, tableConfig).isPresent());
		assertTrue(cache.getPlan("select3", catalogLsn0, tableConfig).isPresent());
		assertTrue(cache.getPlan("select1", catalogLsn1, tableConfig).isPresent());
		assertTrue(cache.getPlan("select2", catalogLsn1, tableConfig).isPresent());
	}

	@Test
	public void testUpdateTableConfig() {
		Map<String, Long> catalogLsn0 = new HashMap<>();
		catalogLsn0.put("t1", 0L);

		TableConfig tableConfig = TableConfig.getDefault();
		PlanCacheManager<String> cache = new PlanCacheManager<>(3);
		cache.putPlan("select1", catalogLsn0, tableConfig, "value10");
		cache.putPlan("select2", catalogLsn0, tableConfig, "value20");
		cache.putPlan("select3", catalogLsn0, tableConfig, "value30");

		assertTrue(cache.getPlan("select1", catalogLsn0, tableConfig).isPresent());
		assertTrue(cache.getPlan("select2", catalogLsn0, tableConfig).isPresent());
		assertTrue(cache.getPlan("select3", catalogLsn0, tableConfig).isPresent());

		Configuration conf = new Configuration();
		conf.setString("testKey", "testValue");
		tableConfig.addConfiguration(conf);
		assertFalse(cache.getPlan("select1", catalogLsn0, tableConfig).isPresent());
		assertFalse(cache.getPlan("select2", catalogLsn0, tableConfig).isPresent());
		assertFalse(cache.getPlan("select3", catalogLsn0, tableConfig).isPresent());
	}
}
