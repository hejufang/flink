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
import org.apache.flink.api.common.typeinfo.RowDataSchemaCompatibilityResolveStrategy;
import org.apache.flink.api.common.typeutils.CompositeTypeSerializerUtil;
import org.apache.flink.api.common.typeutils.NestedSerializersSnapshotDelegate;
import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.api.common.typeutils.TypeSerializerSchemaCompatibility;
import org.apache.flink.api.common.typeutils.TypeSerializerSnapshot;
import org.apache.flink.api.java.typeutils.runtime.DataInputViewStream;
import org.apache.flink.api.java.typeutils.runtime.DataOutputViewStream;
import org.apache.flink.configuration.PipelineOptions;
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
import org.apache.flink.table.types.FieldDigest;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.TypeInformationRawType;
import org.apache.flink.table.types.logical.utils.LogicalTypeUtils;
import org.apache.flink.util.FlinkRuntimeException;
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
	private final RowDataSchemaCompatibilityResolveStrategy strategy;
	private final LogicalType[] types;
	private final TypeSerializer[] fieldSerializers;
	private final FieldDigest[] fieldDigests;
	/**
	 * Will be set exactly before performing state migration. In other scenario, it's always null.
	 * Therefore it's also an implicit flag for {@link RowDataSerializer#serialize(RowData, DataOutputView)}
	 * to distinguish call in state writing period from call in state migration.
	 */
	private RowDataSerializer priorRowDataSerializer;
	/**
	 * A mapping of old row to new row.
	 */
	private transient int[] fieldMapping;
	private transient BinaryRowData reuseRow;
	private transient BinaryRowWriter reuseWriter;
	private transient List<LogicalType> priorNonDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> priorDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> newNonDistinctTypes = new ArrayList<>();
	private transient List<LogicalType> newDistinctTypes = new ArrayList<>();

	public RowDataSerializer(ExecutionConfig config, FieldDigest[] fieldDigests, RowType rowType) {
		this(config,
			rowType.getChildren().toArray(new LogicalType[0]),
			fieldDigests,
			rowType.getChildren().stream()
				.map((LogicalType type) -> InternalSerializers.create(type, config))
				.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(ExecutionConfig config, FieldDigest[] fieldDigests, LogicalType... types) {
		this(config, types, fieldDigests, Arrays.stream(types)
			.map((LogicalType type) -> InternalSerializers.create(type, config))
			.toArray(TypeSerializer[]::new));
	}

	public RowDataSerializer(ExecutionConfig config, RowType rowType) {
		this(config, null, rowType);
	}

	public RowDataSerializer(ExecutionConfig config, LogicalType... types) {
		this(config, null, types);
	}

	public RowDataSerializer(LogicalType[] types, TypeSerializer<?>[] fieldSerializers) {
		this(new ExecutionConfig(), types, null, fieldSerializers);
	}

	public RowDataSerializer(
			@Nullable ExecutionConfig config,
			LogicalType[] types,
			FieldDigest[] fieldDigests,
			TypeSerializer<?>[] fieldSerializers) {
		this(config == null ? PipelineOptions.ROW_DATA_SCHEMA_COMPATIBILITY_RESOLVE_STRATEGY.defaultValue() :
				config.getSchemaCompatibilityResolveStrategy(),
			types,
			fieldDigests,
			fieldSerializers);
	}

	public RowDataSerializer(
			RowDataSchemaCompatibilityResolveStrategy strategy,
			LogicalType[] types,
			FieldDigest[] fieldDigests,
			TypeSerializer<?>[] fieldSerializers) {
		this.strategy = strategy;
		this.types = types;
		this.fieldDigests = fieldDigests;
		this.fieldSerializers = fieldSerializers;
		this.binarySerializer = new BinaryRowDataSerializer(types.length);
	}

	@Override
	public TypeSerializer<RowData> duplicate() {
		TypeSerializer<?>[] duplicateFieldSerializers = new TypeSerializer[fieldSerializers.length];
		for (int i = 0; i < fieldSerializers.length; i++) {
			duplicateFieldSerializers[i] = fieldSerializers[i].duplicate();
		}
		return new RowDataSerializer(strategy, types, fieldDigests, duplicateFieldSerializers);
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
		parseFieldMapping(priorRowDataSerializer);
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

	private void parseFieldMapping(RowDataSerializer priorSerializer) {
		if (priorSerializer.fieldDigests == null || fieldDigests == null) {
			return;
		}
		FieldDigest[] previousDigests = priorSerializer.fieldDigests;
		LogicalType[] previousTypes = priorSerializer.types;
		int[] matched = new int[previousDigests.length];
		int[] mapping = new int[fieldDigests.length];
		Arrays.fill(mapping, -1);
		switch (strategy) {
			case WEAK_RESTRICTIVE:
				for (int i = 0; i < fieldDigests.length; i++) {
					for (int j = 0; j < previousDigests.length; j++) {
						if (matched[j] == 0 && LogicalTypeUtils.isCompatibleWith(previousTypes[j], types[i]) &&
							previousDigests[j].resolveCompatibility(fieldDigests[i])) {
							matched[j] = 1;
							mapping[i] = j;
							break;
						}
					}
				}
				this.fieldMapping = mapping;
				break;
			case STRONG_RESTRICTIVE:
				for (int i = 0; i < fieldDigests.length; i++) {
					for (int j = 0; j < previousDigests.length; j++) {
						if (matched[j] == 0 && previousDigests[j].resolveCompatibility(fieldDigests[i])) {
							if (LogicalTypeUtils.isCompatibleWith(previousTypes[j], types[i])) {
								matched[j] = 1;
								mapping[i] = j;
								break;
							}
						}
					}
				}
				this.fieldMapping = mapping;
				break;
			case EMPTY:
				throw new FlinkRuntimeException("The strategy should not be empty");
			default:
				throw new UnsupportedOperationException(String.format("Unsupported strategy: %s", strategy));
		}
	}

	private void serializeInMigration(RowData record, DataOutputView target) throws IOException {
		GenericRowData newRow = new GenericRowData(this.getArity());
		if (fieldMapping != null) {
			LogicalType[] oldTypes = priorRowDataSerializer.types;
			for (int i = 0; i < fieldMapping.length; i++) {
				LogicalType oldType = fieldMapping[i] >= 0 ? oldTypes[fieldMapping[i]] : null;
				setValueFromRestoredRowWithCast(record, newRow, fieldMapping[i], i, oldType, types[i]);
			}
		} else {
			for (int i = 0; i < newNonDistinctTypes.size(); i++) {
				if (i >= priorNonDistinctTypes.size()) {
					newRow.setField(i, null);
				} else {
					setValueFromRestoredRowWithCast(record, newRow, i, i, priorNonDistinctTypes.get(i),
						newNonDistinctTypes.get(i));
				}
			}

			for (int i = 0; i < newDistinctTypes.size(); i++) {
				int currentIndex = newNonDistinctTypes.size() + i;
				int oldIndex = priorNonDistinctTypes.size() + i;
				if (i >= priorDistinctTypes.size()) {
					newRow.setField(currentIndex, null);
				} else {
					setValueFromRestoredRowWithCast(record, newRow, oldIndex, currentIndex, priorDistinctTypes.get(i),
						newDistinctTypes.get(i));
				}
			}
		}
		binarySerializer.serialize(toBinaryRow(newRow), target);
	}

	@Override
	public void serialize(RowData row, DataOutputView target) throws IOException {
		if (priorRowDataSerializer != null) {
			serializeInMigration(row, target);
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
		return new RowDataSerializerSnapshot(types, fieldDigests, fieldSerializers, strategy);
	}

	public LogicalType[] getTypes() {
		return types;
	}

	private void setValueFromRestoredRowWithCast(
			@Nullable RowData priorRow,
			GenericRowData currentRow,
			int priorIndex,
			int currentIndex,
			LogicalType priorType,
			LogicalType currentType) {
		if (priorIndex < 0) {
			currentRow.setField(currentIndex, null);
			return;
		}
		Object oldValue = RowData.get(priorRow, priorIndex, priorType);
		currentRow.setField(currentIndex, TypeCasts.implicitCast(oldValue, priorType, currentType));
	}

	/**
	 * {@link TypeSerializerSnapshot} for {@link BinaryRowDataSerializer}.
	 */
	public static final class RowDataSerializerSnapshot implements TypeSerializerSnapshot<RowData> {
		private static final int CURRENT_VERSION = 4;

		private LogicalType[] previousTypes;
		private FieldDigest[] previousDigests;
		private NestedSerializersSnapshotDelegate nestedSerializersSnapshotDelegate;
		private RowDataSchemaCompatibilityResolveStrategy strategy;
		private boolean isWriteLengthEnable;

		@SuppressWarnings("unused")
		public RowDataSerializerSnapshot() {
			// this constructor is used when restoring from a checkpoint/savepoint.
		}

		RowDataSerializerSnapshot(
				LogicalType[] types,
				FieldDigest[] digests,
				TypeSerializer[] serializers,
				RowDataSchemaCompatibilityResolveStrategy strategy) {
			this.previousTypes = types;
			this.previousDigests = digests;
			this.nestedSerializersSnapshotDelegate = new NestedSerializersSnapshotDelegate(
				serializers);
			this.strategy = strategy;
			// See details in RowDataSchemaCompatibilityResolveStrategy
			this.isWriteLengthEnable = !RowDataSchemaCompatibilityResolveStrategy.EMPTY.equals(strategy);
		}

		@Override
		public int getCurrentVersion() {
			if (isWriteLengthEnable) {
				return CURRENT_VERSION;
			} else {
				// A trick logic to make states unchanged if users don't enable Digests to be stored in the
				// RowDataSerializerSnapshot.
				// See details in RowDataSchemaCompatibilityResolveStrategy
				return 3;
			}
		}

		@Override
		public void writeSnapshot(DataOutputView out) throws IOException {
			out.writeInt(previousTypes.length);
			DataOutputViewStream stream = new DataOutputViewStream(out);
			for (LogicalType previousType : previousTypes) {
				InstantiationUtil.serializeObject(stream, previousType);
			}
			if (isWriteLengthEnable) {
				if (previousDigests == null) {
					out.writeInt(0);
				} else {
					out.writeInt(previousDigests.length);
					for (FieldDigest previousDigest : previousDigests) {
						InstantiationUtil.serializeObject(stream, previousDigest);
					}
				}
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
			// Since version 4, we support add digests.
			if (readVersion >= 4) {
				int len = in.readInt();
				if (len > 0) {
					previousDigests = new FieldDigest[len];
					for (int i = 0; i < len; i++) {
						try {
							previousDigests[i] = InstantiationUtil.deserializeObject(
								stream,
								userCodeClassLoader
							);
						}
						catch (ClassNotFoundException e) {
							throw new IOException(e);
						}
					}
				}
			} else {
				previousDigests = null;
			}
			this.nestedSerializersSnapshotDelegate = NestedSerializersSnapshotDelegate.readNestedSerializerSnapshots(
				in,
				userCodeClassLoader
			);
		}

		@Override
		public RowDataSerializer restoreSerializer() {
			return new RowDataSerializer(
				strategy,
				previousTypes,
				previousDigests,
				nestedSerializersSnapshotDelegate.getRestoredNestedSerializers()
			);
		}

		/**
		 * @param newSerializer the new serializer to check.
		 * As {@link #fieldDigests} is a newly-added field which not contained by historical serializers.
		 * We would fall back to old checking strategy if {@link #fieldDigests} is absent.
		 */
		@Override
		public TypeSerializerSchemaCompatibility<RowData> resolveSchemaCompatibility(TypeSerializer<RowData> newSerializer) {
			if (!(newSerializer instanceof RowDataSerializer)) {
				String message = String.format("new serializer %s is not a RowDataSerializer.", newSerializer.getClass().getName());
				return TypeSerializerSchemaCompatibility.incompatible(message);
			}

			if (this.previousDigests != null && ((RowDataSerializer) newSerializer).fieldDigests != null) {
				return resolveFieldDigestCompatibility((RowDataSerializer) newSerializer);
			}

			RowDataSerializer newRowSerializer = (RowDataSerializer) newSerializer;

			// generate types string
			final String newTypesString = RowType.of(newRowSerializer.types).asSummaryString();
			final String previousTypesString = RowType.of(previousTypes).asSummaryString();
			boolean isStrictlyEqual = true;

			if (newRowSerializer.types.length > previousTypes.length) {
				List<LogicalType> priorNonDistinctTypes = new ArrayList<>();
				List<LogicalType> priorDistinctTypes = new ArrayList<>();
				List<LogicalType> newNonDistinctTypes = new ArrayList<>();
				List<LogicalType> newDistinctTypes = new ArrayList<>();

				parseDistinctTypes(previousTypes, priorNonDistinctTypes, priorDistinctTypes);
				parseDistinctTypes(((RowDataSerializer) newSerializer).getTypes(), newNonDistinctTypes, newDistinctTypes);

				for (int i = 0; i < priorNonDistinctTypes.size(); i++) {
					if (!LogicalTypeUtils.isCompatibleWith(previousTypes[i], newRowSerializer.types[i])) {
						String message = String.format("new type[%s] is %s, but previous type[%s] is %s, new types are %s, but previous types are %s.",
							i, newRowSerializer.types[i].asSummaryString(), i, previousTypes[i].asSummaryString(), newTypesString, previousTypesString);
						return TypeSerializerSchemaCompatibility.incompatible(message);
					}
				}

				for (int i = 0; i < priorDistinctTypes.size(); i++) {
					int priorIndex = priorNonDistinctTypes.size() + i;
					int newIndex = newNonDistinctTypes.size() + i;
					if (!LogicalTypeUtils.isCompatibleWith(previousTypes[priorIndex], newRowSerializer.types[newIndex])) {
						String message = String.format("new type[%s] is %s, but previous type[%s] is %s, new types are %s, but previous types are %s.",
							newIndex, newRowSerializer.types[newIndex].asSummaryString(), priorIndex, previousTypes[priorIndex].asSummaryString(), newTypesString, previousTypesString);
						return TypeSerializerSchemaCompatibility.incompatible(message);
					}
				}
				return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
			} else if (newRowSerializer.types.length < previousTypes.length) {
				String message = String.format("new types are %s, but previous types are %s.", newTypesString,
					previousTypesString);
				return TypeSerializerSchemaCompatibility.incompatible(message);
			} else if (!Arrays.equals(previousTypes, newRowSerializer.types)) {
				for (int i = 0; i < previousTypes.length; i++) {
					if (!previousTypes[i].equals(newRowSerializer.types[i])) {
						if (!LogicalTypeUtils.isCompatibleWith(previousTypes[i], newRowSerializer.types[i])) {
							String message = String.format("new type[%s] is %s, but previous type[%s] is %s, new types are %s, but previous types are %s.",
								i, newRowSerializer.types[i].asSummaryString(), i, previousTypes[i].asSummaryString(), newTypesString, previousTypesString);
							return TypeSerializerSchemaCompatibility.incompatible(message);
						}
						isStrictlyEqual = false;
					}
				}
				if (!isStrictlyEqual) {
					return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
				}
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

		private TypeSerializerSchemaCompatibility<RowData> resolveFieldDigestCompatibility(RowDataSerializer newSerializer) {
			FieldDigest[] newDigests = newSerializer.fieldDigests;
			LogicalType[] newTypes = newSerializer.types;
			int[] matched = new int[previousDigests.length];
			switch (newSerializer.strategy) {
				case WEAK_RESTRICTIVE:
					int matchedCounts = 0;
					for (int i = 0; i < newDigests.length; i++) {
						for (int j = 0; j < previousDigests.length; j++) {
							if (matched[j] == 0 && LogicalTypeUtils.isCompatibleWith(previousTypes[j], newTypes[i]) &&
								previousDigests[j].resolveCompatibility(newDigests[i])) {
								matched[j] = 1;
								matchedCounts++;
								break;
							}
						}
					}
					if (matchedCounts > 0) {
						return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
					} else {
						String message = String.format("The new types are %s, but previous types are %s.",
							asSummaryString(newDigests, newTypes), asSummaryString(previousDigests, previousTypes));
						return TypeSerializerSchemaCompatibility.incompatible(message);
					}
				case STRONG_RESTRICTIVE:
					for (int i = 0; i < newDigests.length; i++) {
						for (int j = 0; j < previousDigests.length; j++) {
							if (matched[j] == 0 && previousDigests[j].resolveCompatibility(newDigests[i])) {
								if (LogicalTypeUtils.isCompatibleWith(previousTypes[j], newTypes[i])) {
									matched[j] = 1;
									break;
								} else {
									String message = String.format("The new type of %s is %s, but previous type of it " +
											"is %s, new types are %s, but previous types are %s.", newDigests[i], newTypes[i],
										previousTypes[j], asSummaryString(newDigests, newTypes), asSummaryString(previousDigests, previousTypes));
									return TypeSerializerSchemaCompatibility.incompatible(message);
								}
							}
						}
					}
					return TypeSerializerSchemaCompatibility.compatibleAfterMigration();
				case EMPTY:
					throw new FlinkRuntimeException("The strategy should not be empty");
				default:
					throw new UnsupportedOperationException(String.format("Unsupported strategy: %s", newSerializer.strategy));
			}
		}

		private String asSummaryString(FieldDigest[] digests, LogicalType[] types) {
			StringBuilder sb = new StringBuilder();
			for (int i = 0; i < digests.length; i++) {
				if (i > 0) {
					sb.append(", ");
				}
				sb.append(digests[i].toString());
				sb.append("[");
				sb.append(types[i].asSummaryString());
				sb.append("]");
			}
			return sb.toString();
		}
	}
}
