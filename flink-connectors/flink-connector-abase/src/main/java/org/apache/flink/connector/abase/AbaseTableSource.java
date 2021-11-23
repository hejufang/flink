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
import org.apache.flink.connector.abase.executor.AbaseLookupExecutor;
import org.apache.flink.connector.abase.executor.AbaseLookupExecutorFactory;
import org.apache.flink.connector.abase.options.AbaseLookupOptions;
import org.apache.flink.connector.abase.options.AbaseNormalOptions;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.connector.source.LookupTableSource;
import org.apache.flink.table.connector.source.TableFunctionProvider;
import org.apache.flink.table.connector.source.abilities.SupportsProjectionPushDown;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.utils.TableSchemaUtils;

import javax.annotation.Nullable;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.IntStream;

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
		DataType realDataType;
		if (normalOptions.getKeyIndex() >= 0) {
			int keyIndex = normalOptions.getKeyIndex();
			String keyName = schema.getFieldName(keyIndex).get();
			realDataType = schema
				.toPhysicalRowDataTypeWithFilter(tableColumn -> !tableColumn.getName().equals(keyName));
		} else {
			realDataType = schema.toRowDataType();
		}
		DataStructureConverter converter = context.createDataStructureConverter(realDataType);
		List<DataType> childrenType = realDataType.getChildren();
		RowData.FieldGetter[] fieldGetters = IntStream
			.range(0, childrenType.size())
			.mapToObj(pos -> RowData.createFieldGetter(childrenType.get(pos).getLogicalType(), pos))
			.toArray(RowData.FieldGetter[]::new);
		AbaseLookupExecutor abaseLookupExecutor = AbaseLookupExecutorFactory.getAbaseLookupExecutor(
			normalOptions,
			lookupOptions,
			schema.getFieldDataTypes(),
			schema.getFieldNames(),
			converter,
			fieldGetters,
			decodingFormat == null ? null : decodingFormat.createRuntimeDecoder(context, realDataType));
		return TableFunctionProvider.of(new AbaseLookupFunction(
			normalOptions,
			lookupOptions,
			abaseLookupExecutor));
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
