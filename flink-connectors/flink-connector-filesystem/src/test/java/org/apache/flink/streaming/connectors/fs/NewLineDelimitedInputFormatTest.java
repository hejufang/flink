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

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;
import org.apache.flink.formats.csv.CsvRowDeserializationSchema;
import org.apache.flink.formats.json.JsonRowDeserializationSchema;
import org.apache.flink.table.api.Types;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import static junit.framework.TestCase.assertEquals;
import static org.junit.Assert.assertNotNull;

/**
 * test for newLine delimited input format.
 */
public class NewLineDelimitedInputFormatTest {

	private static FileInputSplit createTempFile(String content) throws IOException {
		File tempFile = File.createTempFile("test_contents", "tmp");
		tempFile.deleteOnExit();
		OutputStreamWriter wrt = new OutputStreamWriter(new FileOutputStream(tempFile), StandardCharsets.UTF_8);
		wrt.write(content);
		wrt.close();
		return new FileInputSplit(0, new Path(tempFile.toURI().toString()), 0, tempFile.length(), new String[]{"localhost"});
	}


	private static final TypeInformation<Row> SCHEMA = Types.ROW(
		new String[]{"header1", "header2", "header3"},
		new TypeInformation[]{
			Types.STRING(), Types.INT(), Types.DOUBLE()
		}
	);

	private void validate(String fileContent, DeserializationSchema<Row> deser) throws IOException {

		FileInputSplit split = createTempFile(fileContent);
		Configuration configuration = new Configuration();

		NewLineDelimitedInputFormat format = new NewLineDelimitedInputFormat(new Path("test"),
			configuration, deser);
		format.configure(configuration);
		format.open(split);

		Row result = new Row(3);

		result = (Row) format.nextRecord(result);
		assertNotNull(result);
		assertEquals("this is", result.getField(0));
		assertEquals(1, result.getField(1));
		assertEquals(2.0, result.getField(2));

		result = (Row) format.nextRecord(result);
		assertNotNull(result);
		assertEquals("a test", result.getField(0));
		assertEquals(3, result.getField(1));
		assertEquals(4.0, result.getField(2));
	}

	@Test
	public void testCsvDeser() throws IOException {
		String fileContent =
			"this is|1|2.0|\n" +
			"a test|3|4.0|\n";

		final CsvRowDeserializationSchema deser = new CsvRowDeserializationSchema.Builder(SCHEMA)
			.setFieldDelimiter('|')
			.setQuoteCharacter('\'')
			.setAllowComments(true)
			.setIgnoreParseErrors(true)
			.setEscapeCharacter('\\')
			.setNullLiteral("n/a")
			.build();

		validate(fileContent, deser);
	}

	@Test
	public void testJsonDeser() throws IOException {
		String fileContent = "{\"header2\": 1, \"header3\": 2.0, \"header1\": \"this is\"}\n" +
			"{\"header2\": 3, \"header3\": 4.0, \"header1\": \"a test\"}";

		final JsonRowDeserializationSchema deser = new JsonRowDeserializationSchema.Builder(SCHEMA).build();
		validate(fileContent, deser);
	}

}
