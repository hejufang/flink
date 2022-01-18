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

package org.apache.flink.connector.bmq;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link BmqDynamicTableSource}.
 */
public class BmqDynamicTableSourceTest {
	@Test
	public void testProjectionPushDown() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "bmq");
		descriptor.putString("topic", "topic");
		descriptor.putString("cluster", "cluster");
		descriptor.putString("version", "2");
		descriptor.putString("scan.start-time", "2021-12-31 00");
		descriptor.putString("scan.end-time", "2022-01-01 00");

		BmqDynamicTableSource source = (BmqDynamicTableSource) BmqDynamicTableFactoryTest.createSource(descriptor);
		int[][] firstPushDown = {{0}, {2}};
		source.applyProjection(firstPushDown);
		DataType firstExpected = DataTypes.ROW(
				DataTypes.FIELD("name", DataTypes.STRING()),
				DataTypes.FIELD("time", DataTypes.TIMESTAMP(3)))
			.bridgedTo(RowData.class);
		Assert.assertEquals(firstExpected, source.getProducedDataType());

		int[][] secondPushDown = {{1}};
		source.applyProjection(secondPushDown);
		DataType secondExpected = DataTypes.ROW(DataTypes.FIELD("time", DataTypes.TIMESTAMP(3)))
			.bridgedTo(RowData.class);
		Assert.assertEquals(secondExpected, source.getProducedDataType());
	}
}
