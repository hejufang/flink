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

package org.apache.flink.formats.pb;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.formats.pb.PbOptions.PB_CLASS;
import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link PbFormatFactory}.
 */
public class PbFormatFactoryTest {
	@Test
	public void testGetTableSchema() {
		PbFormatFactory pbFormatFactory = new PbFormatFactory();
		Map<String, String> formatOptions = new HashMap<>();
		formatOptions.put(PbFormatFactory.fullKey(PB_CLASS.key()), "org.apache.flink.formats.pb.TestPb$Container");

		TableSchema expectedTableSchema = TableSchema.builder()
			.field("intTest", DataTypes.INT())
			.field("longTest", DataTypes.BIGINT())
			.field("stringTest", DataTypes.STRING())
			.field("boolTest", DataTypes.BOOLEAN())
			.field("doubleTest", DataTypes.DOUBLE())
			.field("floatTest", DataTypes.FLOAT())
			.field("enumTest", DataTypes.STRING())
			.field("arrayTest",
				DataTypes.ARRAY(
					DataTypes.ROW(
						DataTypes.FIELD("stringTest", DataTypes.STRING()),
						DataTypes.FIELD("mapTest", DataTypes.MAP(DataTypes.STRING(), DataTypes.INT()))
					)))
			.field("bytesTest", DataTypes.BYTES())
			.field("oneofTestStr", DataTypes.STRING())
			.field("oneofTestInt", DataTypes.INT())
			.field("innerMessage", DataTypes.ROW(
				DataTypes.FIELD("longTest", DataTypes.BIGINT()),
				DataTypes.FIELD("boolTest", DataTypes.BOOLEAN())
			))
			.field("intArrayTest", DataTypes.ARRAY(DataTypes.INT()))
			.field("underline_name_test", DataTypes.STRING())
			.field("stringArrayTest", DataTypes.ARRAY(DataTypes.STRING()))
			.field("enumTests", DataTypes.ARRAY(DataTypes.STRING()))
			.build();

		TableSchema tableSchema = pbFormatFactory.getTableSchema(formatOptions);
		assertEquals(expectedTableSchema, tableSchema);
	}
}
