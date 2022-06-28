/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

package org.apache.flink.connector.bytesql.table;

import org.apache.flink.api.java.tuple.Tuple4;
import org.apache.flink.api.java.tuple.Tuple6;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;

import org.junit.Rule;
import org.junit.rules.TestName;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Base class of test of ByteSQL.
 */
public class ByteSQLTableTestBase {
	private static final String RESOURCE_PATH = "./src/test/resources/statements-validation/";
	private static final String FILE_NAME_PATTERN = "%s.out";
	@Rule
	public TestName testName = new TestName();

	public static DataStream<Tuple4<Integer, Long, String, Timestamp>> get4TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple4<Integer, Long, String, Timestamp>> data = new ArrayList<>();
		data.add(new Tuple4<>(1, 1L, "Hi", Timestamp.valueOf("1970-01-01 00:00:00.001")));
		data.add(new Tuple4<>(2, 2L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.002")));
		data.add(new Tuple4<>(3, 2L, "Hello world", Timestamp.valueOf("1970-01-01 00:00:00.003")));
		data.add(new Tuple4<>(4, 3L, "Hello world, how are you?", Timestamp.valueOf("1970-01-01 00:00:00.004")));
		data.add(new Tuple4<>(5, 3L, "I am fine.", Timestamp.valueOf("1970-01-01 00:00:00.005")));
		data.add(new Tuple4<>(6, 3L, "Luke Skywalker", Timestamp.valueOf("1970-01-01 00:00:00.006")));
		data.add(new Tuple4<>(7, 4L, null, Timestamp.valueOf("1970-01-01 00:00:00.007")));
		data.add(new Tuple4<>(8, 4L, "Hello", Timestamp.valueOf("1970-01-01 00:00:00.008")));
		return env.fromCollection(data);
	}

	public static DataStream<Tuple6<String, Long, Integer, Integer, Timestamp, Long>> get6TupleDataStream(StreamExecutionEnvironment env) {
		List<Tuple6<String, Long, Integer, Integer, Timestamp, Long>> data = new ArrayList<>();
		data.add(new Tuple6<>("Bob", 10L, 5, 7, Timestamp.valueOf("2022-01-10 00:01:00.000"), 1641744060000L));
		data.add(new Tuple6<>("Tom", 15L, 10, 4, Timestamp.valueOf("2022-01-10 00:02:00.000"), 1641744120000L));
		data.add(new Tuple6<>("Bob", 20L, 5, 6, Timestamp.valueOf("2022-01-10 00:10:00.000"), 1641744600000L));
		data.add(new Tuple6<>("Bob", 35L, 10, 3, Timestamp.valueOf("2022-01-10 00:12:00.000"), 1641744720000L));
		data.add(new Tuple6<>("Tom", 45L, 10, 2, Timestamp.valueOf("2022-01-10 00:13:00.000"), 1641744780000L));
		return env.fromCollection(data);
	}

	public void verify(List<String> actualSQL) {
		assertEquals(readExpectedSQLFromFile(), String.join("\n", actualSQL));
	}

	private String readExpectedSQLFromFile() {
		File file = new File(RESOURCE_PATH, String.format(FILE_NAME_PATTERN, testName.getMethodName()));
		StringBuilder stringBuilder = new StringBuilder();
		try (
			InputStreamReader read = new InputStreamReader(new FileInputStream(file));
			BufferedReader bufferedReader = new BufferedReader(read)) {
			String line;
			String ls = System.getProperty("line.separator");
			while ((line = bufferedReader.readLine()) != null) {
				stringBuilder.append(line);
				stringBuilder.append(ls);
			}
			stringBuilder.deleteCharAt(stringBuilder.length() - 1);
			return stringBuilder.toString();
		} catch (IOException ignored) {
		}
		return stringBuilder.toString();
	}
}
