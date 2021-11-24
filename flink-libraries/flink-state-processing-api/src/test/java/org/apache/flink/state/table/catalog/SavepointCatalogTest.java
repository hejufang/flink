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

package org.apache.flink.state.table.catalog;

import org.apache.flink.state.table.catalog.resolver.LocationResolver;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.TableEnvironment;
import org.apache.flink.table.api.TableResult;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Test case for SavepointCatalog.
 */

public class SavepointCatalogTest {

	private TableEnvironment tEnv;

	private SavepointCatalog savepointCatalog;

	private LocationResolver resolver;

	@Before
	public void setup() {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().build();
		this.tEnv = TableEnvironment.create(settings);
		resolver = new LocalTestResolver();
		savepointCatalog = new SavepointCatalog("savepoint", "test_sp_1", resolver);
		// use savepoint catalog
		tEnv.registerCatalog("savepoint", savepointCatalog);
		tEnv.useCatalog("savepoint");
	}

	@Test
	public void testShowTables(){
		final TableResult result = tEnv.executeSql("show tables");

		List expect = new ArrayList();
		expect.add(Row.of("6fe83e977ea8ea576938e21ab20d3024#all_keyed_states"));
		expect.add(Row.of("6fe83e977ea8ea576938e21ab20d3024#all_operator_states"));
		expect.add(Row.of("6fe83e977ea8ea576938e21ab20d3024#average"));
		expect.add(Row.of("bc764cd8ddf7a0cff126f51c16239658#all_keyed_states"));
		expect.add(Row.of("bc764cd8ddf7a0cff126f51c16239658#all_operator_states"));
		expect.add(Row.of("bc764cd8ddf7a0cff126f51c16239658#topic-partition-offset-states"));
		expect.add(Row.of("state_meta"));

		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));
	}

	@Test
	public void testUseJarClassNotFound(){
		try {
			tEnv.executeSql("use test_sp_2");
			tEnv.executeSql("show tables");
		} catch (Exception e){
			Assert.assertEquals(true, e.getMessage().contains("failed with ClassNotFoundException"));
		}
	}

	@Test
	public void testQueryStateMeta(){

		final TableResult result = tEnv.executeSql("select * from state_meta");
		List expect = new ArrayList();
		expect.add(Row.of("bc764cd8ddf7a0cff126f51c16239658", "Source: mySource", null, false, null, "topic-partition-offset-states", "LIST", "OPERATOR_STATE_BACKEND"));
		expect.add(Row.of("6fe83e977ea8ea576938e21ab20d3024", "Flat Map", null, true, "Tuple1", "average", "VALUE", "INCREMENTAL_ROCKSDB_STATE_BACKEND"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));

	}

	@Test
	public void testQueryStateMetaWithCondition(){
		tEnv.executeSql("use test_sp_3");

		final TableResult result = tEnv.executeSql("select * from state_meta where operator_id = '000000000000000a0000000000000000'");
		List expect = new ArrayList();
		expect.add(Row.of("000000000000000a0000000000000000", "testName", "testUid", true, "String", "test-state2", "MAP", "MOCK_STATE_BACKEND"));
		expect.add(Row.of("000000000000000a0000000000000000", "testName", "testUid", false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND"));
		Assert.assertEquals(expect, Lists.newArrayList(result.collect()));

		final TableResult result1 = tEnv.executeSql("select * from state_meta where is_keyed_state = false");
		List expect1 = new ArrayList();
		expect1.add(Row.of("000000000000000a0000000000000000", "testName", "testUid", false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND"));
		expect1.add(Row.of("000000000000001e0000000000000000", "testName2", "testUid2", false, null, "test-state", "LIST", "OPERATOR_STATE_BACKEND"));
		Assert.assertEquals(expect1, Lists.newArrayList(result1.collect()));

	}
}
