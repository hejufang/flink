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

package org.apache.flink.connector.rpc.util;

import org.apache.flink.connector.rpc.thrift.generated.SelfContainedStruct;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import java.util.HashSet;

/**
 * Tests for {@link DataTypeUtil}.
 */
public class DataTypeUtilTests {

	@Rule
	public ExpectedException thrown = ExpectedException.none();

	@Test
	public void testSelfContainedNodeDetect() {
		thrown.expect(IllegalArgumentException.class);
		thrown.expectMessage("Struct org.apache.flink.connector.rpc.thrift.generated.SelfContainedStruct is " +
			"self-contained, please turn off auto schema inferring.");
		DataTypeUtil.generateFieldsDataType(SelfContainedStruct.class, new HashSet<>());
	}
}
