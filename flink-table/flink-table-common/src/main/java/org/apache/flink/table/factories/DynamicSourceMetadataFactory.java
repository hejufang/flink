package org.apache.flink.table.factories;

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

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.util.FlinkRuntimeException;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * DynamicSourceMetadataFactory that parse and validate metadata.
 */
public abstract class DynamicSourceMetadataFactory implements Serializable {
	/**
	 * DynamicSourceMetadata.
	 */
	public interface DynamicSourceMetadata extends Serializable {
		Class<?> getLogicalClass();
	}

	protected abstract DynamicSourceMetadata findMetadata(String name);

	protected abstract String getMetadataValues();

	public Map<Integer, DynamicSourceMetadata> parseWithSchema(String metaInfo, TableSchema tableSchema) {
		Map<Integer, DynamicSourceMetadata> metadataMap = new HashMap<>();
		String[] metaPairList = metaInfo.split(",");
		RowType rowType = (RowType) tableSchema.toRowDataType().getLogicalType();
		for (String metaPair: metaPairList) {
			String[] kvPair = metaPair.split("=");
			String metaName = kvPair[0];
			String metaColumnName = kvPair[1];
			DynamicSourceMetadata metadata = findMetadata(metaName);
			if (metadata == null) {
				throw new FlinkRuntimeException(String.format("Metadata `%s` is not find, meta string is `%s`, valid collection is `%s`.",
					metaName, metaInfo, getMetadataValues()));
			}

			DataType dataType = tableSchema.getFieldDataType(metaColumnName).orElseThrow(
				() -> new FlinkRuntimeException(String.format("Column `%s` not find in table schema `%s`", metaColumnName, tableSchema.toString())));
			if (!metadata.getLogicalClass().isInstance(dataType.getLogicalType())) {
				throw new FlinkRuntimeException(String.format("Column `%s` must be type `%s`", metaColumnName, metadata.getLogicalClass().getName()));
			}
			metadataMap.put(rowType.getFieldIndex(metaColumnName), metadata);
		}
		return metadataMap;
	}

	public DataType getWithoutMetaDataTypes(TableSchema tableSchema, Set<Integer> metadataIndexList) {
		Set<String> columnSet = metadataIndexList.stream().map(i -> tableSchema.getFieldName(i).get()).collect(Collectors.toSet());
		return tableSchema.toPhysicalRowDataTypeWithFilter(tableColumn -> !columnSet.contains(tableColumn.getName()));
	}
}
