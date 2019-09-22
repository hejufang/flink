/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.connectors.fs;

import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.formats.csv.CsvRowSerializationSchema;
import org.apache.flink.formats.json.JsonRowSerializationSchema;
import org.apache.flink.streaming.api.datastream.DataStream;
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.junit.Assert;
import org.junit.Test;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.stream.Collectors;

/**
 * newline delimited table sink test.
 */
public class NewLineDelimitedTableSinkITCase {

	private static final String[] FIELD_NAMES = new String[]{"header1", "header2", "header3"};
	private static final TypeInformation<?>[] FIELD_TYPES = new TypeInformation[]{
		Types.STRING(), Types.INT(), Types.DOUBLE()
	};

	private static final TypeInformation<Row> SCHEMA = Types.ROW(
		FIELD_NAMES,
		FIELD_TYPES
	);

	private static final TableSchema TABLE_SCHEMA = new TableSchema(FIELD_NAMES, FIELD_TYPES);

	private void execute(String filePath, SerializationSchema serSchema, String codec) throws Exception{
		final StreamExecutionEnvironment env = StreamExecutionEnvironment.createLocalEnvironment();
		Row row1 = Row.of("this is", 1, 2.0);
		Row row2 = Row.of("a test", 3, 4.0);
		List<Row> list = new ArrayList<>();
		list.add(row1);
		list.add(row2);
		DataStream<Row> rows = env.fromCollection(list);

		NewLineDelimitedTableSink newLineDelimitedTableSink = new NewLineDelimitedTableSink(filePath, TABLE_SCHEMA, 1, null, serSchema, codec);
		newLineDelimitedTableSink.emitDataStream(rows);
		env.execute();
	}

	private void deleteFile(String path) {
		File tempFile = new File(path);
		if (tempFile.exists()) {
			tempFile.delete();
		}
	}

	@Test
	public void testCsvSer() throws Exception {
		final CsvRowSerializationSchema serSchema = new CsvRowSerializationSchema.Builder(SCHEMA).build();
		UUID uuid = UUID.randomUUID();
		String tempFilePath = "/tmp/" + uuid.toString() + ".csv";
		execute(tempFilePath, serSchema, null);

		List<String> results = Files.lines(Paths.get(tempFilePath)).collect(Collectors.toList());
		Assert.assertEquals("\"this is\",1,2.0", results.get(0));

		deleteFile(tempFilePath);
	}

	@Test
	public void testJsonSer() throws Exception {
		final JsonRowSerializationSchema serSchema = new JsonRowSerializationSchema.Builder(SCHEMA).build();
		UUID uuid = UUID.randomUUID();
		String tempFilePath = "/tmp/" + uuid.toString() + ".json";
		execute(tempFilePath, serSchema, null);

		List<String> results = Files.lines(Paths.get(tempFilePath)).collect(Collectors.toList());
		Assert.assertEquals("{\"header1\":\"this is\",\"header2\":1,\"header3\":2.0}", results.get(0));

		deleteFile(tempFilePath);
	}

}
