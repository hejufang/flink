/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 * <p>
 * http://www.apache.org/licenses/LICENSE-2.0
 * <p>
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.EnvironmentSettings;
import org.apache.flink.table.api.Table;
import org.apache.flink.table.api.java.StreamTableEnvironment;
import org.apache.flink.util.FileUtils;

import org.junit.Ignore;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import static java.util.stream.Collectors.joining;
import static org.junit.Assert.assertEquals;

/**
 * NewLineDelimitedLookupFunctionITCase.
 */
@Ignore
public class NewLineDelimitedLookupFunctionITCase {

	private static final String[] JSON_RECORDS = new String[]{
		"{\"id\": \"1\", \"title\": \"hi\"}",
		"{\"id\": \"2\", \"title\": \"hello\"}"
	};

	private static File createTempFile(String filePrefix, String fileSuffix) throws IOException {
		File tempFile = File.createTempFile(filePrefix, fileSuffix);
		tempFile.deleteOnExit();
		return tempFile;
	}

	private static void deleteFile(String filePath) {
		File tempFile = new File(filePath);
		if (tempFile.exists()) {
			tempFile.delete();
		}
	}

	@Test
	public void test() throws Exception {
		final EnvironmentSettings settings = EnvironmentSettings.newInstance()
			.useBlinkPlanner()
			.build();
		StreamExecutionEnvironment env = StreamExecutionEnvironment.getExecutionEnvironment();
		final StreamTableEnvironment tEnv = StreamTableEnvironment.create(env, settings);

		Table t = tEnv.fromDataStream(env.fromCollection(Arrays.asList(
			new Tuple2<>(1L, "Hi"),
			new Tuple2<>(2L, "Hello"),
			new Tuple2<>(3L, "Hello world"),
			new Tuple2<>(5L, "I am fine.")
		)), "id, text");

		tEnv.registerTable("source", t);

		String content = Stream.of(JSON_RECORDS).collect(joining("\n", "", ""));
		File tempDimensionFile = createTempFile("dim", ".json");
		String dimFilePath = tempDimensionFile.getAbsolutePath();
		FileUtils.writeFileUtf8(tempDimensionFile, content);
		String dimDdl = String.format("create table file_dim(" +
			" id bigint," +
			" title varchar" +
			") with (" +
			"  'connector.type' = 'filesystem'," +
			"  'connector.path' = '%s', " +
			"  'format.type' = 'json' " +
			")", dimFilePath);

		String sinkFilePath = dimFilePath + "-sink.csv";
		String sinkDdl = String.format("create table sink(" +
			" id bigint," +
			" text varchar," +
			" title varchar" +
			") with (" +
			"  'connector.type' = 'filesystem'," +
			"  'connector.path' = '%s', " +
			"  'format.type' = 'csv', " +
			"  'format.derive-schema' = 'true', " +
			"  'format.field-delimiter' = ','" +
			")", sinkFilePath);

		String insertSql = "insert into sink" +
			" select T.id, T.text, D.title " +
			" from (select source.id, source.text, PROCTIME() as proc from source) T" +
			" left join file_dim FOR SYSTEM_TIME AS OF T.proc AS D " +
			" on T.id=D.id ";

		tEnv.sqlUpdate(dimDdl);
		tEnv.sqlUpdate(sinkDdl);
		tEnv.sqlUpdate(insertSql);

		env.execute();

		List<String> results = Files.lines(Paths.get(sinkFilePath)).collect(Collectors.toList());
		assertEquals("id,text,title", results.get(0));
		assertEquals("1,Hi,hi", results.get(1));
		assertEquals("2,Hello,hello", results.get(2));
		assertEquals("3,\"Hello world\",", results.get(3));

		deleteFile(sinkFilePath);
	}
}
