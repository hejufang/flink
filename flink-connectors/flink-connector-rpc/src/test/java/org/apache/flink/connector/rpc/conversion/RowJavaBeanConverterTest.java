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

package org.apache.flink.connector.rpc.conversion;

import org.apache.flink.connector.rpc.thrift.conversion.RowJavaBeanConverter;
import org.apache.flink.connector.rpc.thrift.generated.InnerTestStruct;
import org.apache.flink.connector.rpc.thrift.generated.SimpleStruct;
import org.apache.flink.connector.rpc.thrift.generated.TestStruct;
import org.apache.flink.connector.rpc.thrift.generated.TestType;
import org.apache.flink.connector.rpc.util.DataTypeUtil;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;
import org.apache.flink.table.types.DataType;

import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link RowJavaBeanConverter}.
 */
public class RowJavaBeanConverterTest {
	private TestStruct getTestStructInstance() {
		// Construct a InnerTestStruct.
		InnerTestStruct innerTestStruct = new InnerTestStruct();
		innerTestStruct.setBoolVal(true);
		innerTestStruct.setIntVal(12);
		Map<String, Long> mapVal = new HashMap<>();
		mapVal.put("mapKey", 1014L);
		innerTestStruct.setMapVal(mapVal);
		List<Long> listVal = new ArrayList<>();
		listVal.add(1024L);
		innerTestStruct.setListVal(listVal);
		// Construct a SimpleStruct.
		SimpleStruct simpleStruct = new SimpleStruct();
		simpleStruct.setLongVal(23);
		simpleStruct.setBiVal(ByteBuffer.wrap(new byte[]{1}));
		// Construct a TestStruct.
		TestStruct testStruct = new TestStruct();
		testStruct.setStrVal("string");
		testStruct.setInnerTestStruct(innerTestStruct);
		Map<String, SimpleStruct> mapWithStruct = new HashMap<>();
		mapWithStruct.put("mapKey", simpleStruct.deepCopy());
		testStruct.setMapWithStruct(mapWithStruct);
		List<SimpleStruct> listWithStruct = new ArrayList<>();
		listWithStruct.add(simpleStruct.deepCopy());
		testStruct.setListWithStruct(listWithStruct);
		Map<String, List<Long>> mapWithList = new HashMap<>();
		mapWithList.put("mapKey", Collections.singletonList(829L));
		testStruct.setMapWithList(mapWithList);
		List<Map<String, Integer>> listWithMap = new ArrayList<>();
		listWithMap.add(new HashMap<String, Integer>(){{
			put("mapKey", 2);
		}});
		testStruct.setListWithMap(listWithMap);
		Map<String, List<SimpleStruct>> nested = new HashMap<>();
		nested.put("mapKey", Collections.singletonList(simpleStruct.deepCopy()));
		testStruct.setNested(nested);
		List<TestType> enumList = new ArrayList<>();
		enumList.add(TestType.TYPE1);
		testStruct.setEnumList(enumList);
		return testStruct;
	}

	@Test
	public void testConversions() {
		DataType dataType = DataTypeUtil.generateFieldsDataType(TestStruct.class, new HashSet<>());
		DataStructureConverter<RowData, Object> converter = RowJavaBeanConverter.create(TestStruct.class, dataType);
		converter.open(RowJavaBeanConverterTest.class.getClassLoader());
		TestStruct test = getTestStructInstance();
		RowData row = converter.toInternal(test);
		Object real = converter.toExternal(row);
		assertEquals(test, real);
	}
}
