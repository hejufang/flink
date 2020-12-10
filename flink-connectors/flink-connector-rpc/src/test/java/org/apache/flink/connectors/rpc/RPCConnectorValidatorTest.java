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

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.RPCValidator;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Unit Tests for RPC connector params.
 */
public class RPCConnectorValidatorTest {
	public static void fillInEssentialParams(Map<String, String> paramMap) {
		paramMap.put("connector.type", "rpc");
		paramMap.put("connector.is-dimension-table", "true");
		paramMap.put("connector.consul", "***.***.***");
		paramMap.put("connector.thrift-service-class", "com.thrift.HelloWordService");
		paramMap.put("connector.thrift-method", "connector.thrift-method");
	}

	@Test
	public void testValidateProperties() {
		Map<String, String> paramMap = new HashMap<>();
		fillInEssentialParams(paramMap);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(paramMap);
		new RPCValidator().validate(descriptorProperties);
		new RPCTableFactory().getRPCLookupOptions(descriptorProperties);
	}

	@Test(expected = ValidationException.class)
	public void testIncorrectRequestFailureStrategyProperties() {
		Map<String, String> paramMap = new HashMap<>();
		fillInEssentialParams(paramMap);
		//This is incorrect
		paramMap.put("connector.lookup.request-failure-strategy", "incorrect-param");
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(paramMap);
		new RPCValidator().validate(descriptorProperties);
		new RPCTableFactory().getRPCLookupOptions(descriptorProperties);
	}

	@Test
	public void testTaskFailureProperties() {
		Map<String, String> paramMap = new HashMap<>();
		fillInEssentialParams(paramMap);
		paramMap.put("connector.lookup.request-failure-strategy", "task-failure");
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(paramMap);
		new RPCValidator().validate(descriptorProperties);
		RPCLookupOptions rpcLookupOptions = new RPCTableFactory().getRPCLookupOptions(descriptorProperties);
		assertEquals(RPCRequestFailureStrategy.TASK_FAILURE, rpcLookupOptions.getRequestFailureStrategy());
	}

	@Test
	public void testEmitEmptyProperties() {
		Map<String, String> paramMap = new HashMap<>();
		fillInEssentialParams(paramMap);
		paramMap.put("connector.lookup.request-failure-strategy", "emit-empty");
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(paramMap);
		new RPCValidator().validate(descriptorProperties);
		RPCLookupOptions rpcLookupOptions = new RPCTableFactory().getRPCLookupOptions(descriptorProperties);
		assertEquals(RPCRequestFailureStrategy.EMIT_EMPTY, rpcLookupOptions.getRequestFailureStrategy());
	}

	@Test
	public void testDefaultRequestFailureStrategyProperties() {
		Map<String, String> paramMap = new HashMap<>();
		fillInEssentialParams(paramMap);
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(paramMap);
		new RPCValidator().validate(descriptorProperties);
		RPCLookupOptions rpcLookupOptions = new RPCTableFactory().getRPCLookupOptions(descriptorProperties);
		assertEquals(RPCRequestFailureStrategy.TASK_FAILURE, rpcLookupOptions.getRequestFailureStrategy());
	}
}
