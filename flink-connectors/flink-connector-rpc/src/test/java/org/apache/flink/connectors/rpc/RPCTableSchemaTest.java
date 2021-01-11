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

package org.apache.flink.connectors.rpc;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;
import org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosRequest;
import org.apache.flink.connectors.rpc.thriftexample.SimpleDimMiddlewareService;
import org.apache.flink.types.Row;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_BATCH_CLASS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_BATCH_SIZE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_CONSUL;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_IS_DIMENSION_TABLE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_REQUEST_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_RESPONSE_LIST_NAME;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_THRIFT_METHOD;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_THRIFT_SERVICE_CLASS;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.RPCValidator.CONNECTOR_USE_BATCH_LOOKUP;
import static org.junit.Assert.assertEquals;

/**
 * Test {@link RPCTableFactory}.
 */
public class RPCTableSchemaTest {
	@Test
	public void testSimpleSinkSchema() {
		Map<String, String> map = new HashMap();
		map.put(CONNECTOR_TYPE, "rpc");
		map.put(CONNECTOR_CONSUL, "xx");
		map.put(CONNECTOR_IS_DIMENSION_TABLE, "false");
		map.put(CONNECTOR_THRIFT_SERVICE_CLASS, SimpleDimMiddlewareService.class.getName());
		map.put(CONNECTOR_THRIFT_METHOD, "GetDimInfos");
		map.put(CONNECTOR_BATCH_SIZE, "1");
		TypeInformation<Row> rowTypeInfo = RPCTableFactory.getRowTypeInformation(map);
		assertEquals(Types.ROW_NAMED(new String[]{"adId", "creativeId", "advertiserId", "dimensionList", "code"},
			Types.LONG, Types.LONG, Types.LONG, Types.OBJECT_ARRAY(Types.STRING), Types.INT)
			, rowTypeInfo);
	}

	@Test
	public void testBatchSinkSchema() {
		Map<String, String> map = new HashMap();
		map.put(CONNECTOR_TYPE, "rpc");
		map.put(CONNECTOR_CONSUL, "xx");
		map.put(CONNECTOR_IS_DIMENSION_TABLE, "false");
		map.put(CONNECTOR_THRIFT_SERVICE_CLASS, BatchDimMiddlewareService.class.getName());
		map.put(CONNECTOR_THRIFT_METHOD, "GetDimInfosBatch");
		map.put(CONNECTOR_BATCH_CLASS, GetDimInfosRequest.class.getName());
		map.put(CONNECTOR_BATCH_SIZE, "2");
		TypeInformation<Row> rowTypeInfo = RPCTableFactory.getRowTypeInformation(map);
		assertEquals(Types.ROW_NAMED(new String[]{"adId", "creativeId", "advertiserId", "dimensionList", "code"},
			Types.LONG, Types.LONG, Types.LONG, Types.OBJECT_ARRAY(Types.STRING), Types.INT)
			, rowTypeInfo);
	}

	@Test
	public void testSimpleDimensionSchema() {
		Map<String, String> map = new HashMap();
		map.put(CONNECTOR_TYPE, "rpc");
		map.put(CONNECTOR_CONSUL, "xx");
		map.put(CONNECTOR_IS_DIMENSION_TABLE, "true");
		map.put(CONNECTOR_THRIFT_SERVICE_CLASS, SimpleDimMiddlewareService.class.getName());
		map.put(CONNECTOR_THRIFT_METHOD, "GetDimInfos");
		TypeInformation<Row> typeInfo = RPCTableFactory.getRowTypeInformation(map);
		assertEquals(Types.ROW_NAMED(new String[]{"adId", "creativeId", "advertiserId",
				"dimensionList", "code", "response"},
			Types.LONG, Types.LONG, Types.LONG, Types.OBJECT_ARRAY(Types.STRING), Types.INT,
			Types.ROW_NAMED(new String[]{"dimensions", "code"}, Types.MAP(Types.STRING, Types.STRING), Types.INT))
			, typeInfo);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
		assertEquals("GetDimInfosResponse", rowTypeInfo.getFieldNames()[rowTypeInfo.getArity() - 1]);
	}

	@Test
	public void testBatchDimensionSchema() {
		Map<String, String> map = new HashMap();
		map.put(CONNECTOR_TYPE, "rpc");
		map.put(CONNECTOR_CONSUL, "xx");
		map.put(CONNECTOR_IS_DIMENSION_TABLE, "true");
		map.put(CONNECTOR_USE_BATCH_LOOKUP, "true");
		map.put(CONNECTOR_THRIFT_SERVICE_CLASS, BatchDimMiddlewareService.class.getName());
		map.put(CONNECTOR_THRIFT_METHOD, "GetDimInfosBatch");
		map.put(CONNECTOR_REQUEST_LIST_NAME, "requests");
		map.put(CONNECTOR_RESPONSE_LIST_NAME, "responses");
		TypeInformation<Row> typeInfo = RPCTableFactory.getRowTypeInformation(map);
		assertEquals(Types.ROW_NAMED(new String[]{"adId", "creativeId", "advertiserId",
				"dimensionList", "code", "GetDimInfosResponse"},
			Types.LONG, Types.LONG, Types.LONG, Types.OBJECT_ARRAY(Types.STRING), Types.INT,
			Types.ROW_NAMED(new String[]{"dimensions", "code"}, Types.MAP(Types.STRING, Types.STRING), Types.INT))
			, typeInfo);
		RowTypeInfo rowTypeInfo = (RowTypeInfo) typeInfo;
		assertEquals("GetDimInfosResponse", rowTypeInfo.getFieldNames()[rowTypeInfo.getArity() - 1]);
	}
}
