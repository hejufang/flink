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

package org.apache.flink.formats.protobuf;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for PbFormatUtils.
 */
public class PbFormatUtilsTest {

	@Test
	public void testCamelCaseConverting() {
		checkCamelCaseConverting("vpr6s", "Vpr6S");
		checkCamelCaseConverting("a", "A");
		checkCamelCaseConverting("map1", "Map1");
		checkCamelCaseConverting("int_map", "IntMap");
		checkCamelCaseConverting("int__map", "IntMap");
		checkCamelCaseConverting("int_2map", "Int2Map");
		checkCamelCaseConverting("IntMap", "IntMap");
		checkCamelCaseConverting("intMap", "IntMap");
		checkCamelCaseConverting("2int", "2Int");
		checkCamelCaseConverting("int2", "Int2");
		checkCamelCaseConverting("int_1", "Int1");
		checkCamelCaseConverting("int_", "Int");
	}

	private void checkCamelCaseConverting(String from, String expected) {
		String converted = PbFormatUtils.getStrongCamelCaseJsonName(from);
		Assert.assertEquals(converted, expected);
	}
}
