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

package org.apache.flink.connector.abase;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.connector.abase.executor.AbaseLookupCollectionExecutor;
import org.apache.flink.connector.abase.executor.AbaseLookupExecutor;
import org.apache.flink.connector.abase.executor.AbaseLookupGeneralExecutor;
import org.apache.flink.connector.abase.executor.AbaseLookupSchemaExecutor;
import org.apache.flink.connector.abase.executor.AbaseLookupSpecifyHashKeyExecutor;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.connector.abase.utils.AbaseValueType;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;
import org.apache.flink.util.Preconditions;

import javax.annotation.Nullable;

import java.util.Arrays;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkState;

/**
 * A {@link DynamicTableSource} for abase.
 */
public class AbaseTableSource implements LookupTableSource, SupportsProjectionPushDown {
	private final AbaseNormalOptions normalOptions;
	private final AbaseLookupOptions lookupOptions;
	@Nullable
	private final DecodingFormat<DeserializationSchema<RowData>> decodingFormat;
	private TableSchema schema;

	public AbaseTableSource(
			AbaseNormalOptions normalOptions,
			AbaseLookupOptions lookupOptions,
			TableSchema schema,
			@Nullable DecodingFormat<DeserializationSchema<RowData>> decodingFormat) {
		this.normalOptions = normalOptions;
		this.lookupOptions = lookupOptions;
		this.schema = schema;
		this.decodingFormat = decodingFormat;
	}

	@Override
	public LookupRuntimeProvider getLookupRuntimeProvider(LookupContext context) {
		int[] pkRuntimeIdx = getPrimaryKeyRuntimeIndices(context.getKeys(), normalOptions.getKeyIndices());

		Set<String> pkNames = Arrays.stream(normalOptions.getKeyIndices()).mapToObj(
			i -> schema.getFieldName(i).get()).collect(Collectors.toSet());
		DataType realDataType = schema.toPhysicalRowDataTypeWithFilter(column -> !pkNames.contains(column.getName()));
		List<DataType> childrenType = realDataType.getChildren();
		AbaseLookupExecutor abaseLookupExecutor = null;

		// 1. general datatype with value format
		if (decodingFormat != null) {
			RowData.FieldGetter[] fieldGetters = IntStream
				.range(0, childrenType.size())
				.mapToObj(pos -> RowData.createFieldGetter(childrenType.get(pos).getLogicalType(), pos))
				.toArray(RowData.FieldGetter[]::new);
			DeserializationSchema<RowData> deserializationSchema = decodingFormat.createRuntimeDecoder(context, realDataType);
			abaseLookupExecutor = new AbaseLookupSchemaExecutor(normalOptions, fieldGetters, deserializationSchema);

		// 2. general datatype without format
		} else if (normalOptions.getAbaseValueType().equals(AbaseValueType.GENERAL)) {
			abaseLookupExecutor = new AbaseLookupGeneralExecutor(normalOptions, schema.getFieldDataTypes());

		// 3. hash datatype with hash keys specified
		} else if (normalOptions.isSpecifyHashFields()) {
			abaseLookupExecutor = new AbaseLookupSpecifyHashKeyExecutor(
				normalOptions,
				schema.getFieldNames(),
				schema.getFieldDataTypes(),
				lookupOptions.getRequestedHashKeys());

		// 4. get values of hash/list/set/zset datatype as a whole
		} else {
			assert childrenType.size() == 1;
			DataStructureConverter converter = context.createDataStructureConverter(childrenType.get(0));
			abaseLookupExecutor = new AbaseLookupCollectionExecutor(
				normalOptions,
				converter);
		}

		return TableFunctionProvider.of(new AbaseLookupFunction(
			pkRuntimeIdx,
			normalOptions,
			lookupOptions,
			abaseLookupExecutor));
	}

	/**
	 * Get lookup key position among all runtime lookup keys.
	 *
	 * <p>Since abase/redis only support primary keys defined in the schema as lookup keys,
	 * it's necessary only to get positions of primary keys and ignore others.
	 * @param lookKeys runtime lookup key positions in the schema which is sorted
	 * @param pks primary key positions defined in the schema which is also sorted
	 * @return lookup key position among all runtime lookup keys
	 */
	private static int[] getPrimaryKeyRuntimeIndices(int[][] lookKeys, int[] pks) {
		int[] pos = new int[pks.length];
		int posIdx = 0;
		int pkIdx = 0;
		for (int i = 0; i < lookKeys.length; i++) {
			int[] innerKeyArr = lookKeys[i];
			Preconditions.checkArgument(innerKeyArr.length == 1,
				"Abase/redis only support non-nested look up keys");
			if (innerKeyArr[0] == pks[pkIdx++]) {
				pos[posIdx++] = i;
				if (pkIdx == pks.length) {
					break;
				}
			}
		}
		checkState(posIdx == pos.length, "Some primary keys are not inferred as lookup keys, "
			+ "inferred list of primary keys indices: " + Arrays.toString(Arrays.copyOf(pos, posIdx)) + ", and total "
			+ "list of defined primary keys indices: " + Arrays.toString(pks));
		return pos;
	}

	@Override
	public long getLaterJoinMs() {
		return lookupOptions.getLaterRetryMs();
	}

	@Override
	public int getLaterJoinRetryTimes() {
		return lookupOptions.getLaterRetryTimes();
	}

	@Override
	public Optional<Boolean> isInputKeyByEnabled() {
		return Optional.ofNullable(lookupOptions.isInputKeyByEnabled());
	}

	@Override
	public DynamicTableSource copy() {
		return new AbaseTableSource(
			normalOptions,
			lookupOptions,
			schema,
			decodingFormat);
	}

	@Override
	public String asSummaryString() {
		return normalOptions.getStorage();
	}

	@Override
	public boolean supportsNestedProjection() {
		return false;
	}

	@Override
	public void applyProjection(int[][] projectedFields) {
		this.schema = TableSchemaUtils.projectSchema(schema, projectedFields);
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof AbaseTableSource)) {
			return false;
		}
		AbaseTableSource that = (AbaseTableSource) o;
		return Objects.equals(normalOptions, that.normalOptions) &&
			Objects.equals(lookupOptions, that.lookupOptions) &&
			Objects.equals(decodingFormat, that.decodingFormat) &&
			Objects.equals(schema, that.schema);
	}

	@Override
	public int hashCode() {
		return Objects.hash(normalOptions, lookupOptions, decodingFormat, schema);
	}
}
