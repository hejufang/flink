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

package org.apache.flink.connector.jdbc.table;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.bridge.java.StreamTableEnvironment;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.collect.Lists;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.flink.table.api.Expressions.$;
import static org.junit.Assert.assertEquals;

/**
 * IT case for RowData lookup source of JDBC connector.
 */
public class RowDataLookupTableITCase extends JdbcLookupTestBase {

	@Rule
	public ExpectedException exception = ExpectedException.none();

	@Test
	public void testWidenTypes() throws Exception {
		Iterator<Row> collected;
		collected = collectResult(true);
		List<String> result = Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());

		List<String> expected = new ArrayList<>();
		expected.add("1,1,11-c1-v1,11-c2-v1,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,aaaaa,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("1,1,11-c1-v1,11-c2-v1,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,aaaaa,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("1,1,11-c1-v2,11-c2-v2,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,bbbbb,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("1,1,11-c1-v2,11-c2-v2,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,bbbbb,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("2,3,null,23-c2,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,ccccc,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("2,5,25-c1,25-c2,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,ddddd,2021-12-06,20:12:35,2021-12-06T20:12:35");
		expected.add("3,8,38-c1,38-c2,true,12,12345,12345678,123.456,123.456789,123456789.9876543210,654.3209838867188,135.0000000000,eeeee,2021-12-06,20:12:35,2021-12-06T20:12:35");
		Collections.sort(expected);

		assertEquals(expected, result);
	}

	@Test
	public void testCastingException() {
		exception.expect(RuntimeException.class);
		Iterator<Row> collected;
		collected = collectResult(false);
		Lists.newArrayList(collected).stream()
			.map(Row::toString)
			.sorted()
			.collect(Collectors.toList());
	}

	private Iterator<Row> collectResult(boolean compatibleMode) {
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(env);
		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1, "1"),
			new Tuple2<>(1, "1"),
			new Tuple2<>(2, "3"),
			new Tuple2<>(2, "5"),
			new Tuple2<>(3, "5"),
			new Tuple2<>(3, "8")
		)), $("id1"), $("id2"), $("proctime").proctime());

		tEnv.createTemporaryView("T", t);

		tEnv.executeSql(
			"create table lookup (" +
				"  id1 INT," +
				"  id2 VARCHAR," +
				"  comment1 VARCHAR," +
				"  comment2 VARCHAR," +
				"  boolean_field boolean," +
				"  smallint_field bigint," +
				"  int_field bigint," +
				"  bigint_field bigint," +
				"  float_field double," +
				"  double_field double," +
				"  decimal_field decimal(30,10)," +
				"  real_field double," +
				"  numeric_field decimal(38,10)," +
				"  char_field char(5)," +
				"  date_field date," +
				"  time_field time," +
				"  timestamp_field timestamp(3)" +
				") with(" +
				"  'connector'='jdbc'," +
				"  'url'='" + DB_URL + "'," +
				"  'use-bytedance-mysql'='false'," +
				"  'compatible-mode'='" + compatibleMode + "'," +
				"  'table-name'='" + LOOKUP_TABLE + "'" +
				"  )");

		String sqlQuery = "SELECT source.id1, source.id2, L.comment1, L.comment2, L.boolean_field, L.smallint_field," +
			" L.int_field, L.bigint_field, L.float_field, L.double_field, L.decimal_field, L.real_field, L.numeric_field," +
			" L.char_field, L.date_field, L.time_field, L.timestamp_field" +
			" FROM T AS source " +
			" JOIN lookup for system_time as of source.proctime AS L " +
			" ON source.id1 = L.id1 and source.id2 = L.id2";
		return tEnv.executeSql(sqlQuery).collect();
	}
}
