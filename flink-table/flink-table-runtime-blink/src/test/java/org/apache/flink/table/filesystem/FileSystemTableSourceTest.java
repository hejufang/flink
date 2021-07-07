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

package org.apache.flink.table.filesystem;

import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.types.DataType;

import org.junit.Assert;
import org.junit.Test;

/**
 * Test for {@link FileSystemTableSource}.
 */
public class FileSystemTableSourceTest {
	@Test
	public void testProjectionPushDown() {
		DescriptorProperties descriptor = new DescriptorProperties();
		descriptor.putString(FactoryUtil.CONNECTOR.key(), "filesystem");
		descriptor.putString("path", "/tmp");
		descriptor.putString("format", "json");

		FileSystemTableSource source = (FileSystemTableSource) FileSystemTableFactoryTest.createSource(descriptor);
		int[][] firstPushDown = {{0}, {2}};
		source.applyProjection(firstPushDown);
		DataType firstExpected = DataTypes.ROW(
			DataTypes.FIELD("f0", DataTypes.STRING()),
			DataTypes.FIELD("f2", DataTypes.BIGINT()))
			.bridgedTo(RowData.class);
		Assert.assertEquals(firstExpected, source.getProducedDataType());
		int[][] secondPushDown = {{1}};
		source.applyProjection(secondPushDown);
		DataType secondExpected = DataTypes.ROW(DataTypes.FIELD("f2", DataTypes.BIGINT()))
			.bridgedTo(RowData.class);
		Assert.assertEquals(secondExpected, source.getProducedDataType());
	}
}
