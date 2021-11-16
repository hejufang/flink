/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.connector.bytesql.table.executor;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import java.util.Arrays;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * A executor that extracts primary keys from the supplied stream elements and executes a SQL query for them.
 */
public abstract class KeyedBatchStatementExecutor<V> extends ByteSQLBatchStatementExecutor {
	protected final int[] pkFields;
	protected final String deleteSQL;
	protected final Map<GenericRowData, V> reduceBuffer;

	public KeyedBatchStatementExecutor(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType) {
		super(options, insertOptions, rowType);
		String[] pkFieldNames = insertOptions.getKeyFields();
		List<String> nameList = rowType.getFieldNames();
		this.pkFields = Arrays.stream(pkFieldNames).mapToInt(nameList::indexOf).toArray();
		this.deleteSQL = ByteSQLUtils.getDeleteStatement(options.getTableName(), pkFieldNames);
		this.reduceBuffer = new LinkedHashMap<>();
	}

	@Override
	public boolean isBufferEmpty() {
		return reduceBuffer.isEmpty();
	}

	protected GenericRowData getPrimaryKey(RowData row) {
		GenericRowData pks = new GenericRowData(pkFields.length);
		for (int i = 0; i < pkFields.length; i++) {
			pks.setField(i, fieldGetters[pkFields[i]].getFieldOrNull(row));
		}
		return pks;
	}

	/**
	 * Merge two to-update row to one row. For each field, just get last not null value of two
	 * rows. If some values of both rows are null, then the merged values are null too.
	 * The oldRow can be a null row. Meanwhile, the newRow won't be null as only incoming
	 * not null rows will need this function.
	 */
	@VisibleForTesting
	protected RowData mergeRow(RowData newRow, RowData oldRow) {
		if (oldRow == null){
			return newRow;
		} else {
			GenericRowData merged = new GenericRowData(newRow.getArity());
			for (int i = 0; i < newRow.getArity(); i++) {
				RowData select = newRow.isNullAt(i) ? oldRow : newRow;
				merged.setField(i, this.fieldGetters[i].getFieldOrNull(select));
			}
			merged.setRowKind(newRow.getRowKind());
			return merged;
		}
	}
}
