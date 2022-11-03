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
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.core.memory.DataInputDeserializer;
import org.apache.flink.core.memory.DataOutputSerializer;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.ArrayType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BinaryType;
import org.apache.flink.table.types.logical.CharType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.DoubleType;
import org.apache.flink.table.types.logical.FloatType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.MapType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Test;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static org.junit.Assert.assertTrue;

/**
 * Test suite for the {@link RowDataSerializer.RowDataSerializerSnapshot}.
 */
public class RowDataSerializerSnapshotTest {
	// ------------------------------------------------------------------------------------------------
	//  Scope: tests RowDataSerializerSnapshot#resolveSchemaCompatibility
	// ------------------------------------------------------------------------------------------------

	@Test
	public void testTypeDownwardCompatibility() throws IOException {
		List<Tuple2<LogicalType, LogicalType>> compareMap = new ArrayList<Tuple2<LogicalType, LogicalType>>()  {{
			this.add(new Tuple2(new IntType(), new BigIntType()));
			this.add(new Tuple2(new IntType(false), new DoubleType()));
			this.add(new Tuple2(new TinyIntType(), new BigIntType()));
			this.add(new Tuple2(new FloatType(), new DoubleType()));
			this.add(new Tuple2(new BinaryType(), new VarBinaryType()));
			this.add(new Tuple2(new MapType(new IntType(), new FloatType()), new MapType(new BigIntType(), new DoubleType())));
			this.add(new Tuple2(new ArrayType(new IntType()), new ArrayType(new BigIntType())));
			this.add(new Tuple2(new VarCharType(1), new VarCharType(2)));
			this.add(new Tuple2(new VarCharType(false, 1), new VarCharType(true, 2)));
			this.add(new Tuple2(new CharType(1), new CharType(2)));
			this.add(new Tuple2(new CharType(false, 1), new CharType(true, 2)));
			this.add(new Tuple2(new CharType(2), new VarCharType(2)));
			this.add(new Tuple2(new CharType(false, 2), new VarCharType(true, 2)));
			this.add(new Tuple2(new MapType(new CharType(), new DoubleType()),
				new MapType(new VarCharType(), new DecimalType(4, 3))));
			this.add(new Tuple2(new ArrayType(new IntType()), new ArrayType(new BigIntType())));
			this.add(new Tuple2(RowType.of(new IntType(), new MapType(new CharType(), new DoubleType())),
				RowType.of(new BigIntType(), new MapType(new VarCharType(), new DecimalType(4, 3)))));
		}};
		LogicalType[] oldTypes = compareMap.stream().map(tuple -> tuple.f0).toArray(LogicalType[]::new);
		RowDataTypeInfo oldTypeInfo = new RowDataTypeInfo(oldTypes);
		RowDataSerializer oldSerializer = oldTypeInfo.createSerializer(new ExecutionConfig());

		LogicalType[] newTypes = compareMap.stream().map(tuple -> tuple.f1).toArray(LogicalType[]::new);
		RowDataTypeInfo newTypeInfo = new RowDataTypeInfo(newTypes);
		RowDataSerializer newSerializer = newTypeInfo.createSerializer(new ExecutionConfig());

		TypeSerializerSchemaCompatibility<RowData> result =
			snapshotAndGetSchemaCompatibilityAfterRestore(oldSerializer, newSerializer);

		assertTrue(result.getMessage(), result.isCompatibleAfterMigration());
	}

	private TypeSerializerSchemaCompatibility<RowData> snapshotAndGetSchemaCompatibilityAfterRestore(
			RowDataSerializer previousSerializer,
			RowDataSerializer newSerializer) throws IOException {
		TypeSerializerSnapshot<RowData> snapshot = previousSerializer.snapshotConfiguration();
		DataOutputSerializer out = new DataOutputSerializer(128);
		TypeSerializerSnapshot.writeVersionedSnapshot(out, snapshot);
		DataInputDeserializer in = new DataInputDeserializer(out.getCopyOfBuffer());
		snapshot = TypeSerializerSnapshot.readVersionedSnapshot(
			in, Thread.currentThread().getContextClassLoader());
		return snapshot.resolveSchemaCompatibility(newSerializer);
	}
}
