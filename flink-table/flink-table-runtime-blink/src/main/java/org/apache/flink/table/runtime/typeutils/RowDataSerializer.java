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

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.ExecutionConfig;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.core.memory.DataInputView;
import org.apache.flink.core.memory.DataOutputView;
import org.apache.flink.runtime.memory.AbstractPagedInputView;
import org.apache.flink.runtime.memory.AbstractPagedOutputView;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.binary.BinaryRowData;
import org.apache.flink.table.data.writer.BinaryRowWriter;
import org.apache.flink.table.data.writer.BinaryWriter;
import org.apache.flink.table.dataview.MapViewTypeInfo;
import org.apache.flink.table.runtime.types.InternalSerializers;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.util.InstantiationUtil;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * Serializer for {@link RowData}.
 */
@Internal
public class RowDataSerializer extends AbstractRowDataSerializer<RowData> {
	private static final long serialVersionUID = 1L;

	private BinaryRowDataSerializer binarySerializer;
	private final LogicalType[] types;
	private final TypeSerializer[] fieldSerializers;

	private transient BinaryRowData reuseRow;
	private transient BinaryRowWriter reuseWriter;

	private RowDataSerializer priorRowDataSerializer;
	private transient List<LogicalType> priorNonDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> priorDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> newNonDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> newDistinctTypes = new ArrayList<>();

	public RowDataSerializer(ExecutionConfig config, RowType rowType) {
		this(rowType.getChildren().toArray(new LogicalType[0]),
			rowType.getChildren().stream()
				.map((LogicalType type) -> InternalSerializers.create(type, config))
				.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(ExecutionConfig config, LogicalType... types) {
		this(types, Arrays.stream(types)
			.map((LogicalType type) -> InternalSerializers.create(type, config))
			.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
		this.types = types;
		this.fieldSerializers = fieldSerializers;
		this.binarySerializer = new BinaryRowDataSerializer(types.length);
	}

	@Override
	public TypeSerializer<RowData> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowDataSerializer(types, duplicateFieldSerializers);
	}

	@Override
	public RowData createInstance() {
		// default use binary row to deserializer
		return new BinaryRowData(types.length);
	}

	@Override
	public void setPriorSerializer(TypeSerializer priorSerializer) {
		this.priorRowDataSerializer = (RowDataSerializer) priorSerializer;
		parseDistinctTypes(priorRowDataSerializer.getTypes(), priorNonDistinctTypes, priorDistinctTypes);
		parseDistinctTypes(this.getTypes(), newNonDistinctTypes, newDistinctTypes);
	}

	private static void parseDistinctTypes(
			LogicalType[] types,
			List<LogicalType> nondistinctTypes,
			List<LogicalType> distinctTypes) {
		for (LogicalType type : types) {
			if (type instanceof TypeInformationRawType && ((TypeInformationRawType) type).getTypeInformation() instanceof MapViewTypeInfo) {
				distinctTypes.add(type);
			} else {
				nondistinctTypes.add(type);
			}
		}
	}

	@Override
	public void serialize(RowData row, DataOutputView target) throws IOException {
		if (priorRowDataSerializer != null && row.getArity() == priorRowDataSerializer.getArity()) {
			// mapping the fields from prior row to new row
			GenericRowData newRow = new GenericRowData(this.getArity());

			for (int i = 0; i < newNonDistinctTypes.size(); i++) {
				if (i >= priorNonDistinctTypes.size()) {
					setValueFromRestoredRow(null, newRow, -1, i, newNonDistinctTypes.get(i));
				} else {
					setValueFromRestoredRow(row, newRow, i, i, newNonDistinctTypes.get(i));
				}
			}

			for (int i = 0; i < newDistinctTypes.size(); i++) {
				if (i >= priorDistinctTypes.size()) {
					setValueFromRestoredRow(null, newRow, -1, newNonDistinctTypes.size() + i, newDistinctTypes.get(i));
				} else {
					setValueFromRestoredRow(row, newRow, priorNonDistinctTypes.size() + i, newNonDistinctTypes.size() + i, newDistinctTypes.get(i));
				}
			}

			binarySerializer.serialize(toBinaryRow(newRow), target);
		} else {
			binarySerializer.serialize(toBinaryRow(row), target);
		}
	}

	@Override
	public RowData deserialize(DataInputView source) throws IOException {
		return binarySerializer.deserialize(source);
	}

	@Override
	public RowData deserialize(RowData reuse, DataInputView source) throws IOException {
		if (reuse instanceof BinaryRowData) {
			return binarySerializer.deserialize((BinaryRowData) reuse, source);
		} else {
			return binarySerializer.deserialize(source);
		}
	}

	@Override
	public RowData copy(RowData from) {
		if (from.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
				", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return ((BinaryRowData) from).copy();
		} else {
			return copyRowData(from, new GenericRowData(from.getArity()));
		}
	}

	@Override
	public RowData copy(RowData from, RowData reuse) {
		if (from.getArity() != types.length || reuse.getArity() != types.length) {
			throw new IllegalArgumentException("Row arity: " + from.getArity() +
				", reuse Row arity: " + reuse.getArity() +
				", but serializer arity: " + types.length);
		}
		if (from instanceof BinaryRowData) {
			return reuse instanceof BinaryRowData
				? ((BinaryRowData) from).copy((BinaryRowData) reuse)
				: ((BinaryRowData) from).copy();
		} else {
			return copyRowData(from, reuse);
		}
	}

	@SuppressWarnings("unchecked")
	private RowData copyRowData(RowData from, RowData reuse) {
		GenericRowData ret;
		if (reuse instanceof GenericRowData) {
			ret = (GenericRowData) reuse;
		} else {
			ret = new GenericRowData(from.getArity());
		}
		ret.setRowKind(from.getRowKind());
		for (int i = 0; i < from.getArity(); i++) {
			if (!from.isNullAt(i)) {
				ret.setField(
					i,
					fieldSerializers[i].copy((RowData.get(from, i, types[i])))
				);
			} else {
				ret.setField(i, null);
			}
		}
		return ret;
	}

	@Override
	public void copy(DataInputView source, DataOutputView target) throws IOException {
		binarySerializer.copy(source, target);
	}

	@Override
	public int getArity() {
		return types.length;
	}

	/**
	 * Convert {@link RowData} into {@link BinaryRowData}.
	 * TODO modify it to code gen.
	 */
	@Override
	public BinaryRowData toBinaryRow(RowData row) {
		if (row instanceof BinaryRowData) {
			return (BinaryRowData) row;
		}
		if (reuseRow == null) {
			reuseRow = new BinaryRowData(types.length);
			reuseWriter = new BinaryRowWriter(reuseRow);
		}
		reuseWriter.reset();
		reuseWriter.writeRowKind(row.getRowKind());
		for (int i = 0; i < types.length; i++) {
			if (row.isNullAt(i)) {
				reuseWriter.setNullAt(i);
			} else {
				BinaryWriter.write(reuseWriter, i, RowData.get(row, i, types[i]), types[i], fieldSerializers[i]);
			}
		}
		reuseWriter.complete();
		return reuseRow;
	}

	@Override
	public int serializeToPages(RowData row, AbstractPagedOutputView target) throws IOException {
		return binarySerializer.serializeToPages(toBinaryRow(row), target);
	}

	@Override
	public RowData deserializeFromPages(AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public RowData deserializeFromPages(
		RowData reuse,
		AbstractPagedInputView source) throws IOException {
		throw new UnsupportedOperationException("Not support!");
	}

	@Override
	public RowData mapFromPages(AbstractPagedInputView source) throws IOException {
		//noinspection unchecked
		return binarySerializer.mapFromPages(source);
	}

	@Override
	public RowData mapFromPages(
		RowData reuse,
		AbstractPagedInputView source) throws IOException {
		if (reuse instanceof BinaryRowData) {
			return binarySerializer.mapFromPages((BinaryRowData) reuse, source);
		} else {
			throw new UnsupportedOperationException("Not support!");
		}
	}

	@Override
	public boolean equals(Object obj) {
		if (obj instanceof RowDataSerializer) {
			RowDataSerializer other = (RowDataSerializer) obj;
			return Arrays.equals(types, other.types);
		}

		return false;
	}

	@Override
	public int hashCode() {
		return Arrays.hashCode(types);
	}

	@Override
	public boolean isImmutableType() {
		return false;
	}

	@Override
	public int getLength() {
		return -1;
	}

	@Override
	public TypeSerializerSnapshot<RowData> snapshotConfiguration() {
		return new RowDataSerializerSnapshot(types, fieldSerializers);
	}

	public LogicalType[] getTypes() {
		return types;
	}

	private void setValueFromRestoredRow(
			@Nullable RowData priorRow,
			GenericRowData currentRow,
			int priorIndex,
			int currentIndex,
			LogicalType type) {
		if (priorRow == null) {
			currentRow.setField(currentIndex, null);
			return;
		}

		currentRow.setField(currentIndex, RowData.get(priorRow, priorIndex, type));
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}.
	 */
	public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
		private static final int CURRENT_VERSION = 3;

		private LogicalType[] previousTypes;
		private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;

		@SuppressWarnings("unused")
		public RowDataSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		RowDataSerializerSnapshot(LogicalType[] types, TypeSerializer[] serializers) {
			this.previousTypes = types;
			this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(
				serializers);
		}

		@Override
		public int getCurrentVersion() {
			return CURRENT_VERSION;
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousTypes.length);
			DataOutputViewStream stream = new DataOutputViewStream(out);
			for (LogicalType previousType : previousTypes) {
				InstantiationUtil.serializeObject(stream, previousType);
			}
			nestedSerializersSnapshotDelegate.writeNestedSerializerSnapshots(out);
		}

		@Override
		public void readSnapshot(int readVersion, DataInputView in, ClassLoader userCodeClassLoader)
			throws IOException {
			int length = in.readInt();
			DataInputViewStream stream = new DataInputViewStream(in);
			previousTypes = new LogicalType[length];
			for (int i = 0; i < length; i++) {
				try {
					previousTypes[i] = InstantiationUtil.deserializeObject(
						stream,
						userCodeClassLoader
					);
				}
				catch (ClassNotFoundException e) {
					throw new IOException(e);
				}
			}
			this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader
			);
		}

		@Override
		public RowDataSerializer restoreSerializer() {
			return new RowDataSerializer(
				previousTypes,
				nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		@Override
		public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(TypeSerializer<RowData> newSerializer) {
			if (!(newSerializer instanceof RowDataSerializer)) {
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;
			if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
				if (newRowSerializer.types.length > previousTypes.length) {
					List<LogicalType> priorNonDistinctTypes = new ArrayList<>();
					List<LogicalType> priorDistinctTypes = new ArrayList<>();
					List<LogicalType> newNonDistinctTypes = new ArrayList<>();
					List<LogicalType> newDistinctTypes = new ArrayList<>();

					parseDistinctTypes(previousTypes, priorNonDistinctTypes, priorDistinctTypes);
					parseDistinctTypes(((RowDataSerializer) newSerializer).getTypes(), newNonDistinctTypes, newDistinctTypes);

					for (int i = 0; i < priorNonDistinctTypes.size(); i++) {
						if (!previousTypes[i].equals(newRowSerializer.types[i])) {
							return TypeSerializerSchemaCompatibility.incompatible();
						}
					}

					for (int i = 0; i < priorDistinctTypes.size(); i++) {
						int priorIndex = priorNonDistinctTypes.size() + i;
						int newIndex = newNonDistinctTypes.size() + i;
						if (!previousTypes[priorIndex].equals(newRowSerializer.types[newIndex])) {
							return TypeSerializerSchemaCompatibility.incompatible();
						}
					}

					return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
				}
				return TypeSerializerSchemaCompatibility.incompatible();
			}

			CompositeTypeSerializerUtil.IntermediateCompatibilityResult<RowData> intermediateResult =
				CompositeTypeSerializerUtil.constructIntermediateCompatibilityResult(
					newRowSerializer.fieldSerializers,
					nestedSerializersSnapshotDelegate.getNestedSerializerSnapshots()
				);

			if (intermediateResult.isCompatibleWithReconfiguredSerializer()) {
				RowDataSerializer reconfiguredCompositeSerializer = restoreSerializer();
				return TypeSerializerSchemaCompatibility.compatibleWithReconfiguredSerializer(
					reconfiguredCompositeSerializer);
			}

			return intermediateResult.getFinalResult();
		}
	}
}
