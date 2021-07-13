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

package org.apache.flink.table.runtime.dataview;

import org.apache.flink.api.common.state.ListState;

import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Tests for {@link StateListView}.
 */
public class StateListViewTest {

	private static class MockListState implements ListState<String> {

		public Iterable<String> iterable;

		@Override
		public void update(List<String> values) throws Exception {
			this.iterable = values;
		}

		@Override
		public void addAll(List<String> values) throws Exception {

		}

		@Override
		public Iterable<String> get() throws Exception {
			return iterable;
		}

		@Override
		public void add(String value) throws Exception {

		}

		@Override
		public void clear() {

		}
	}

	@Test
	public void testRemoveElementFromListView() throws Exception {
		MockListState listState = new MockListState();
		StateListView.KeyedStateListView<Void, String> listView =
			new StateListView.KeyedStateListView<>(listState);

		// remove non exist state
		Assert.assertFalse(listView.remove("test"));

		// remove non exist element.
		listState.iterable = new ArrayList<>();
		Assert.assertFalse(listView.remove("test"));

		// remove existed element.
		listState.iterable = Arrays.asList("hello", "world");
		Assert.assertTrue(listView.remove("hello"));
		Assert.assertNotNull(listState.iterable);
		ArrayList<String> values = new ArrayList<>();
		for (String s : listState.iterable) {
			values.add(s);
		}
		Assert.assertEquals(values.size(), 1);
		Assert.assertEquals(values.get(0), "world");
	}

}
