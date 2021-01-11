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

import org.apache.flink.connectors.rpc.thrift.ThriftRPCClient;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosRequest;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosResponse;
import org.apache.flink.connectors.rpc.thriftexample.SimpleDimMiddlewareService;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.runtime.collector.TableFunctionCollector;
import org.apache.flink.types.Row;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Collections;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNotNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test simple rpc batch lookup join.
 */
public class SimpleRPCLookupTest {
	public static final TableSchema LOOKUP_TABLE_SCHEMA = TableSchema.builder()
		.field("adId", DataTypes.BIGINT())
		.field("dimensionList", DataTypes.ARRAY(DataTypes.STRING()))
		.field("response", ROW(
			FIELD("dimensions", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
			FIELD("code", DataTypes.INT())))
		.build();

	private RPCOptions rpcOptions;
	private RPCLookupOptions rpcLookupOptions;
	private RPCLookupFunction rpcLookupFunction;
	private ThriftRPCClient thriftRPCClient;

	@Before
	public void before() {
		rpcOptions = RPCOptions.builder()
			.setConsul("")
			.setThriftServiceClass(SimpleDimMiddlewareService.class.getName())
			.setThriftMethod("GetDimInfos")
			.setPsm("xxx")
			.setTestHostPort("127.0.0.1:80")
			.build();
		rpcLookupOptions = RPCLookupOptions.builder()
			.setCacheMaxSize(100)
			.setCacheExpireMs(3600000L)
			.build();
		rpcLookupFunction = new RPCLookupFunction(
			LOOKUP_TABLE_SCHEMA.toRowType(),
			LOOKUP_TABLE_SCHEMA.getFieldNames(),
			new String[]{"adId", "dimensionList"},
			rpcOptions,
			rpcLookupOptions,
			LOOKUP_TABLE_SCHEMA.toRowDataType()
		);

		thriftRPCClient = spy(rpcLookupFunction.getThriftRPCClient());
		rpcLookupFunction.setThriftRPCClient(thriftRPCClient);

		FunctionContext spyFunctionContext = spy(new FunctionContext(null));
		doReturn(new UnregisteredMetricsGroup()).when(spyFunctionContext).getMetricGroup();
		rpcLookupFunction.open(spyFunctionContext);
		rpcLookupFunction.setCollector(new TableFunctionCollector<Row>() {
			@Override
			public void collect(Row record) {
				setInput(record);
			}
		});
	}

	@Test
	public void testSimpleLookupJoin(){
		callThrift();
		verify(thriftRPCClient).sendRequest(Collections.singletonList(getThriftRequestObj()));
		TableFunctionCollector<Row> collector = (TableFunctionCollector<Row>) rpcLookupFunction.getCollector();
		Row resultRow = (Row) collector.getInput();
		assertEquals(getResponseRow(), resultRow);
	}

	@Test
	public void testCacheStored() {
		callThrift();
		//check cache
		Cache<Row, Row> cache = rpcLookupFunction.getCache();
		Row cachedRow = cache.getIfPresent(Row.of(0L, new String[]{"dim0"}));
		assertNotNull(cachedRow);
		assertEquals(getResponseRow(), cachedRow);
	}

	@Test
	public void testCacheWorks(){
		rpcLookupFunction.getCache().put(Row.of(0L, new String[]{"dim0"}), getResponseRow());
		rpcLookupFunction.eval(0L, new String[]{"dim0"});
		verify(thriftRPCClient, never()).sendRequest(any());
	}

	private void callThrift() {
		doAnswer(x -> {
			GetDimInfosResponse response0 = new GetDimInfosResponse();
			response0.setDimensions(Collections.singletonMap("dim0", "value0"));
			response0.setCode(100);

			return response0;
		}).when(thriftRPCClient).sendRequest(any());

		rpcLookupFunction.eval(0L, new String[]{"dim0"});
	}

	private GetDimInfosRequest getThriftRequestObj(){
		GetDimInfosRequest request = new GetDimInfosRequest();
		request.setAdId(0L);
		request.setDimensionList(Collections.singletonList("dim0"));
		return request;
	}

	private Row getResponseRow(){
		return Row.of(0L, new String[]{"dim0"},
			Row.of(Collections.singletonMap("dim0", "value0"), 100));
	}

	@After
	public void after() {
		if (rpcLookupFunction != null) {
			rpcLookupFunction.close();
		}
	}
}
