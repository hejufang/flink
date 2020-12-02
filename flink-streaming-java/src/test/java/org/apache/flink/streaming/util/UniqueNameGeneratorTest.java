/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.streaming.util;

import org.junit.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.Assert.assertEquals;

/**
 * Tests for {@link UniqueNameGenerator}.
 */
public class UniqueNameGeneratorTest {

	@Test
	public void testGetUniqueName() {
		String map = "Map";
		String flatmap = "FlatMap";
		Map<String, Integer> prefixIndexMap = new HashMap<>();
		String uniqueName1 = UniqueNameGenerator.appendSuffixIfNotUnique(map, prefixIndexMap);
		String uniqueName2 = UniqueNameGenerator.appendSuffixIfNotUnique(map, prefixIndexMap);
		String uniqueName3 = UniqueNameGenerator.appendSuffixIfNotUnique(map, prefixIndexMap);
		String longUniqueName1 = UniqueNameGenerator.appendSuffixIfNotUnique(flatmap, prefixIndexMap);
		String longUniqueName2 = UniqueNameGenerator.appendSuffixIfNotUnique(flatmap, prefixIndexMap);
		assertEquals(uniqueName1, map);
		assertEquals(uniqueName2, map + "_1");
		assertEquals(uniqueName3, map + "_2");
		assertEquals(longUniqueName1, flatmap);
		assertEquals(longUniqueName2, flatmap + "_1");
	}
}
