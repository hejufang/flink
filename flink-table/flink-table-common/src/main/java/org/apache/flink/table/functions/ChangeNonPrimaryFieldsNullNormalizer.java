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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.Internal;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.RowKind;
import org.apache.flink.util.Preconditions;

/**
 * The {@link RowData} {@link DeleteNormalizer} which changes non-primary fields to null.
 */
@Internal
public class ChangeNonPrimaryFieldsNullNormalizer implements DeleteNormalizer<RowData> {
	private static final long serialVersionUID = 1L;

	private final int[] primaryKeyIndices;
	private final RowData.FieldGetter[] primaryKeyGetters;

	private ChangeNonPrimaryFieldsNullNormalizer(
			int[] primaryKeyIndices,
			RowData.FieldGetter[] primaryKeyGetters) {
		this.primaryKeyIndices = primaryKeyIndices;
		this.primaryKeyGetters = primaryKeyGetters;
	}

	@Override
	public RowData apply(RowData rowData) {
		if (rowData.getRowKind() != RowKind.DELETE) {
			return rowData;
		}
		GenericRowData result = new GenericRowData(RowKind.INSERT, rowData.getArity());
		for (int i = 0; i < primaryKeyIndices.length; ++i) {
			result.setField(primaryKeyIndices[i], primaryKeyGetters[i].getFieldOrNull(rowData));
		}
		return result;
	}

	public static ChangeNonPrimaryFieldsNullNormalizer of(TableSchema schema) {
		final int[] primaryKeyIndices = schema.getPrimaryKeyIndices();
		Preconditions.checkNotNull(primaryKeyIndices, "Primary should be set.");

		final RowData.FieldGetter[] primaryKeyFieldGetters = new RowData.FieldGetter[primaryKeyIndices.length];
		for (int i = 0; i < primaryKeyIndices.length; ++i) {
			//noinspection OptionalGetWithoutIsPresent
			primaryKeyFieldGetters[i] = RowData.createFieldGetter(
				schema.getFieldDataType(primaryKeyIndices[i]).get().getLogicalType(),
				primaryKeyIndices[i]);
		}
		return new ChangeNonPrimaryFieldsNullNormalizer(primaryKeyIndices, primaryKeyFieldGetters);
	}
}
