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

package org.apache.flink.table.runtime.typeutils;

import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeinfo.BasicTypeInfo;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.runtime.generated.GeneratedRecordComparator;
import org.apache.flink.table.runtime.operators.rank.RetractableTopNFunction;
import org.apache.flink.table.types.logical.IntType;

import org.junit.Test;

import java.io.IOException;
import java.util.Comparator;
import java.util.SortedMap;

import static org.junit.Assert.assertTrue;

/**
 * Tests for {@link SortedMapSerializerSnapshot}.
 */
public class SortedMapSerializerSnapshotTest {

	@Test
	public void testGeneratedComparatorChanged() throws IOException {
		GeneratedRecordComparator oldGeneratedComparator =
			new GeneratedRecordComparator("Class1", "compare1", new Object[0], new Object[]{"sample"});
		Comparator<RowData> oldComparatorWrapper = new RetractableTopNFunction.ComparatorWrapper(oldGeneratedComparator);
		SortedMapTypeInfo<RowData, Integer> oldTypeInfo =
			new SortedMapTypeInfo<>(
				new RowDataTypeInfo(new IntType()),
				BasicTypeInfo.INT_TYPE_INFO,
				oldComparatorWrapper);
		TypeSerializer<SortedMap<RowData, Integer>> oldTypeSerializer = oldTypeInfo.createSerializer(new ExecutionConfig());

		GeneratedRecordComparator newGeneratedComparator =
			new GeneratedRecordComparator("Class2", "compare2", new Object[0], new Object[]{"sample"});
		Comparator<RowData> newComparatorWrapper = new RetractableTopNFunction.ComparatorWrapper(newGeneratedComparator);
		SortedMapTypeInfo<RowData, Integer> newTypeInfo =
			new SortedMapTypeInfo<>(
				new RowDataTypeInfo(new IntType()),
				BasicTypeInfo.INT_TYPE_INFO,
				newComparatorWrapper);
		TypeSerializer<SortedMap<RowData, Integer>> newTypeSerializer = newTypeInfo.createSerializer(new ExecutionConfig());

		TypeSerializerSchemaCompatibility<SortedMap<RowData, Integer>> actualSchemaCompatibility =
			snapshotSerializerAndGetSchemaCompatibilityAfterRestore(oldTypeSerializer, newTypeSerializer);
		assertTrue(actualSchemaCompatibility.isCompatibleAsIs());
	}

	@Test
	public void testGeneratedComparatorBackwardCompatibility() throws IOException {
		GeneratedRecordComparator oldGeneratedComparator =
			new GeneratedRecordComparator("Class1", "compare1", new Object[0]);
		Comparator<RowData> oldComparatorWrapper = new RetractableTopNFunction.ComparatorWrapper(oldGeneratedComparator);
		SortedMapTypeInfo<RowData, Integer> oldTypeInfo =
			new SortedMapTypeInfo<>(
				new RowDataTypeInfo(new IntType()),
				BasicTypeInfo.INT_TYPE_INFO,
				oldComparatorWrapper);
		TypeSerializer<SortedMap<RowData, Integer>> oldTypeSerializer = oldTypeInfo.createSerializer(new ExecutionConfig());

		GeneratedRecordComparator newGeneratedComparator =
			new GeneratedRecordComparator("Class1", "compare1", new Object[0], new Object[]{"sample"});
		Comparator<RowData> newComparatorWrapper = new RetractableTopNFunction.ComparatorWrapper(newGeneratedComparator);
		SortedMapTypeInfo<RowData, Integer> newTypeInfo =
			new SortedMapTypeInfo<>(
				new RowDataTypeInfo(new IntType()),
				BasicTypeInfo.INT_TYPE_INFO,
				newComparatorWrapper);
		TypeSerializer<SortedMap<RowData, Integer>> newTypeSerializer = newTypeInfo.createSerializer(new ExecutionConfig());

		TypeSerializerSchemaCompatibility<SortedMap<RowData, Integer>> actualSchemaCompatibility =
			snapshotSerializerAndGetSchemaCompatibilityAfterRestore(oldTypeSerializer, newTypeSerializer);
		assertTrue(actualSchemaCompatibility.isCompatibleAsIs());
	}

	private <T> TypeSerializerSchemaCompatibility<T> snapshotSerializerAndGetSchemaCompatibilityAfterRestore(
			TypeSerializer<T> oldTypeSerializer,
			TypeSerializer<T> newTypeSerializer) throws IOException {

		TypeSerializerSnapshot<T> testSerializerSnapshot = oldTypeSerializer.snapshotConfiguration();

		DataOutputSerializer out = new DataOutputSerializer(128);
		TypeSerializerSnapshot.writeVersionedSnapshot(out, testSerializerSnapshot);

		DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
		testSerializerSnapshot = TypeSerializerSnapshot.readVersionedSnapshot(
			in, Thread.currentThread().getContextClassLoader());

		return testSerializerSnapshot.resolveSchemaCompatibility(newTypeSerializer);
	}
}
