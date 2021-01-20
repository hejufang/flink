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

import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosBatchResponse;
import org.apache.flink.connectors.rpc.thriftexample.GetDimInfosResponse;

import org.junit.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static org.junit.Assert.assertEquals;

/**
 * Test for {@link ThriftUtil}.
 */
public class ThriftUtilTest {
	@Test
	public void testGetInnerListFromInstance() throws Exception {
		GetDimInfosBatchResponse batchResponse = new GetDimInfosBatchResponse();
		GetDimInfosResponse response0 = new GetDimInfosResponse();
		response0.setDimensions(Collections.singletonMap("dim0", "value0"));
		response0.setCode(100);
		GetDimInfosResponse response1 = new GetDimInfosResponse();
		response1.setDimensions(Collections.singletonMap("dim1", "value1"));
		response1.setCode(200);
		batchResponse.setResponses(Arrays.asList(response0, response1));
		List<Object> responseList = ThriftUtil.getInnerListFromInstance(
			batchResponse, GetDimInfosBatchResponse.class, "responses");
		assertEquals(2, responseList.size());

		GetDimInfosResponse r0 = (GetDimInfosResponse) responseList.get(0);
		GetDimInfosResponse r1 = (GetDimInfosResponse) responseList.get(1);

		assertEquals(r0, response0);
		assertEquals(r1, response1);
	}
}
