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

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.streaming.api.TimeCharacteristic;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.java.StreamTableEnvironment;

import org.apache.commons.io.FileUtils;
import org.junit.Assert;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;

/**
 * NewLineDelimitedTableITCase.
 */
public class NewLineDelimitedTableITCase {
	private static File createTempFile(String filePrefix, String fileSuffix) throws IOException {
		File tempFile = File.createTempFile(filePrefix, fileSuffix);
		tempFile.deleteOnExit();
		return tempFile;
	}

	private static final String[] JSON_RECORDS = new String[]{
		"{\"score\": \"13.4\", \"last\": \"Jim\", \"id\": \"1\", \"first\": \"Jim\"}",
		"{\"score\": \"12.3\", \"last\": \"Smith\", \"id\": \"1\", \"first\": \"Mike\"}",
		"{\"score\": \"45.6\", \"last\": \"Taylor\", \"id\": \"2\", \"first\": \"Bob\"}"
	};

	public static void main(String[] args) throws Exception {
		EnvironmentSettings settings = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build();
		StreamExecutionEnvironment execEnv;
		execEnv = StreamExecutionEnvironment.getExecutionEnvironment().setParallelism(4);
		execEnv.setStreamTimeCharacteristic(TimeCharacteristic.EventTime);
		StreamTableEnvironment tEnv = StreamTableEnvironment.create(execEnv, settings);

		execEnv.setParallelism(1);

		File tempSourceFile = createTempFile("tmp", ".json");
		FileUtils.writeStringToFile(tempSourceFile, Stream.of(JSON_RECORDS).collect(joining("\n", "", "")));
		String sourcePath = tempSourceFile.getAbsolutePath();
		String sinkPath = sourcePath + "-sink.csv";

		String sql = "CREATE TABLE `source` (\n" +
			"  `first`  VARCHAR,\n" +
			"  `id`  INTEGER,\n" +
			"  `score`  DOUBLE,\n" +
			"  `last`  VARCHAR\n" +
			") WITH (\n" +
			"  'connector.type' = 'filesystem',\n" +
			"  'connector.path' = '%s',\n" +
			"  'format.type' = 'json'\n" +
			");\n" +
			"CREATE TABLE `sink` (\n" +
			"  `id`  INTEGER,\n" +
			"  `cnt`  BIGINT\n" +
			") WITH (\n" +
			"  'connector.type' = 'filesystem',\n" +
			"  'connector.path' = '%s',\n" +
			"  'format.type' = 'json'\n" +
			");\n";

		String insert = "insert into sink select id, count(1) as cnt from source group by id";

		tEnv.sql(String.format(sql, sourcePath, sinkPath) + insert);

		tEnv.execute("testJob");

		List<String> results = Files.lines(Paths.get(sinkPath)).collect(Collectors.toList());
		Assert.assertEquals("{\"id\":1,\"cnt\":1}", results.get(0));
		Assert.assertEquals("{\"id\":1,\"cnt\":2}", results.get(1));
		Assert.assertEquals("{\"id\":2,\"cnt\":1}", results.get(2));

		File tempFile = new File(sinkPath);
		if (tempFile.exists()) {
			tempFile.delete();
		}

	}
}
