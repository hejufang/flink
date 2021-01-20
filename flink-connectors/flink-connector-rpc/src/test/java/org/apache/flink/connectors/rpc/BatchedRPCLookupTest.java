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
import org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchRequest;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchResponse;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosRequest;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosResponse;
import org.apache.flink.metrics.groups.UnregisteredMetricsGroup;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.types.Row;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.List;

import static org.apache.flink.table.api.DataTypes.FIELD;
import static org.apache.flink.table.api.DataTypes.ROW;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertNull;
import static org.mockito.Mockito.any;
import static org.mockito.Mockito.doAnswer;
import static org.mockito.Mockito.doReturn;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.spy;
import static org.mockito.Mockito.verify;

/**
 * Test rpc batch lookup join.
 */
public class BatchedRPCLookupTest {
	public static final TableSchema LOOKUP_TABLE_SCHEMA = TableSchema.builder()
		.field("adId", DataTypes.BIGINT())
		.field("dimensionList", DataTypes.ARRAY(DataTypes.STRING()))
		.field("response", ROW(
			FIELD("dimensions", DataTypes.MAP(DataTypes.STRING(), DataTypes.STRING())),
			FIELD("code", DataTypes.INT())))
		.build();

	private RPCOptions rpcOptions;
	private RPCLookupOptions rpcLookupOptions;
	private RPCBatchedLookupFunction rpcLookupFunction;
	private ThriftRPCClient thriftRPCClient;

	@Before
	public void before() {
		rpcOptions = RPCOptions.builder()
			.setConsul("")
			.setThriftServiceClass(BatchDimMiddlewareService.class.getName())
			.setThriftMethod("GetDimInfosBatch")
			.setPsm("xxx")
			.setTestHostPort("127.0.0.1:80")
			.build();
		rpcLookupOptions = RPCLookupOptions.builder()
			.setCacheMaxSize(100)
			.setCacheExpireMs(3600000L)
			.setBatchLookup(true)
			.setRequestListFieldName("requests")
			.setResponseListFieldName("responses")
			.build();
		rpcLookupFunction = new RPCBatchedLookupFunction(
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
	}

	private List<Collection<Row>> testCallThrift() {
		doAnswer(x -> {
			GetDimInfosResponse response0 = new GetDimInfosResponse();
			response0.setDimensions(Collections.singletonMap("dim0", "value0"));
			response0.setCode(100);

			GetDimInfosResponse response2 = new GetDimInfosResponse();
			response2.setDimensions(Collections.singletonMap("dim2", "value2"));
			response2.setCode(300);

			GetDimInfosBatchResponse batchResponse = new GetDimInfosBatchResponse();
			batchResponse.setResponses(Arrays.asList(response0, null, response2));
			return batchResponse;
		}).when(thriftRPCClient).sendLookupBatchRequest(any());

		List<Object[]> keysList = new ArrayList<>();
		keysList.add(new Object[]{0L, new String[]{"dim0"}});
		keysList.add(new Object[]{1L, new String[]{"dim1"}});
		keysList.add(new Object[]{2L, new String[]{"dim2"}});
		return rpcLookupFunction.eval(keysList);
	}

	private List<List<Row>> getTestResultList() {
		List<List<Row>> resultList = new ArrayList<>();
		resultList.add(Collections.singletonList(
			Row.of(0L, new String[]{"dim0"},
				Row.of(Collections.singletonMap("dim0", "value0"), 100))
		));
		resultList.add(null);
		resultList.add(Collections.singletonList(
			Row.of(2L, new String[]{"dim2"},
				Row.of(Collections.singletonMap("dim2", "value2"), 300))
		));
		return resultList;
	}

	private GetDimInfosBatchRequest getTestBatchRequestThriftObj() {
		GetDimInfosBatchRequest requestObject = new GetDimInfosBatchRequest();
		GetDimInfosRequest request0 = new GetDimInfosRequest();
		request0.setAdId(0L);
		request0.setDimensionList(Collections.singletonList("dim0"));
		GetDimInfosRequest request1 = new GetDimInfosRequest();
		request1.setAdId(1L);
		request1.setDimensionList(Collections.singletonList("dim1"));
		GetDimInfosRequest request2 = new GetDimInfosRequest();
		request2.setAdId(2L);
		request2.setDimensionList(Collections.singletonList("dim2"));
		requestObject.setRequests(Arrays.asList(request0, request1, request2));
		return requestObject;
	}

	@Test
	public void testBatchLookupJoin() {
		List<Collection<Row>> result = testCallThrift();
		verify(thriftRPCClient).sendLookupBatchRequest(getTestBatchRequestThriftObj());
		assertEquals(getTestResultList(), result);
	}

	@Test
	public void testCacheStored() {
		testCallThrift();
		//null value will not be cached
		assertEquals(2, rpcLookupFunction.getCache().size());
		Row resultRow0 = rpcLookupFunction.getCache().getIfPresent(
			Row.of(0L, new String[]{"dim0"}));
		//check cache
		assertEquals(getTestResultList().get(0).get(0), resultRow0);
		Row resultRow2 = rpcLookupFunction.getCache().getIfPresent(
			Row.of(2L, new String[]{"dim2"}));
		assertEquals(getTestResultList().get(2).get(0), resultRow2);
	}

	@Test
	public void testCacheWorks() {
		rpcLookupFunction.getCache().put(Row.of(0L, new String[]{"dim0"}), getTestResultList().get(0).get(0));
		rpcLookupFunction.eval(Collections.singletonList(new Object[]{0L, new String[]{"dim0"}}));
		verify(thriftRPCClient, never()).sendLookupBatchRequest(any());
	}

	@Test
	public void testNullJoin() {
		doAnswer(x -> {
			GetDimInfosBatchResponse batchResponse = new GetDimInfosBatchResponse();
			batchResponse.setResponses(Collections.singletonList(null));
			return batchResponse;
		}).when(thriftRPCClient).sendLookupBatchRequest(any());

		List<Object[]> keysList = new ArrayList<>();
		keysList.add(new Object[]{0L, new String[]{"dim0"}});
		List<Collection<Row>> result = rpcLookupFunction.eval(keysList);
		assertEquals(1, result.size());
		assertNull(result.get(0));
	}

	@After
	public void after() {
		if (rpcLookupFunction != null) {
			rpcLookupFunction.close();
		}
	}
}
