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

package org.apache.flink.connectors.rpc.thrift;

import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.VarCharType;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;

import static org.junit.Assert.assertEquals;

/**
 * Unit Tests for generating a object.
 */
public class ThriftSerializationTest {
	@Test
	public void testCreateRowConverter() throws Exception {
		RowType rowType = new RowType(
			Arrays.asList(
				new RowType.RowField("strVal", new VarCharType()),
				new RowType.RowField("innerTestStruct", new RowType(
					Arrays.asList(
						new RowType.RowField("boolVal", new BooleanType()),
						new RowType.RowField("intVal", new IntType())
					)
				)),
				new RowType.RowField("mapVal", new MapType(new VarCharType(), new BigIntType()))
				));
		SerializationRuntimeConverter converter = SerializationRuntimeConverterFactory
			.createRowConverter(org.apache.flink.connectors.rpc.thrift.TestStruct.class, rowType);
		HashMap<String, Long> mapVal = new HashMap<String, Long>(){{
			put("key", 20L);
		}};
		Row base = Row.of("str", Row.of(true, 23), mapVal);
		Object actualObj = converter.convert(base);
		Object expectObj = new TestStruct()
			.setStrVal("str")
			.setInnerTestStruct(new InnerTestStruct()
				.setBoolVal(true)
				.setIntVal(23))
			.setMapVal(mapVal);
		assertEquals(expectObj, actualObj);
	}
}
