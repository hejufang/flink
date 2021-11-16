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
import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * A batch executor in which only non-null values will be written.
 * Delete/retract messages cannot be merged with insert messages if null values should be ignore.
 * Therefore we use a tuple as the value of map.
 * f0 of the tuple is a flag that means a delete should be executed before the value f1 (if exist) being insert.
 * f1 of the tuple is the value that will be inserted if it's not null.
 * For detail for setting the tuple, see {@link #addToBatch}.
 */
public class PartiallyUpdateStatementExecutor extends KeyedBatchStatementExecutor<Tuple2<Boolean, RowData>> {
	private static final long serialVersionUID = 1L;

	public PartiallyUpdateStatementExecutor(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType) {
		super(options, insertOptions, rowType);
	}

	/**
	 * Update the buffer map.
	 * For a given key, a initial tuple will depend on the first incoming message:
	 * 1. delete -> tuple(true, null);
	 * 2. update -> tuple(false, update row);
	 * After initialization, per each message coming, the tuple will be update:
	 * 1. tuple(true, null), delete -> tuple(true, null);
	 * 2. tuple(true, null), update -> tuple(true, update row);
	 * 3. tuple(false, update row), delete -> tuple(true, null);
	 * 4. tuple(false, update row), update -> tuple(false, update row(merged if null is ignored));
	 * 5. tuple(true, update row), delete -> tuple(true, null);
	 * 6. tuple(true, update row), update ->  tuple(true, update row(merged if null is ignored)).
	 */
	@Override
	public void addToBatch(RowData record){
		GenericRowData primaryKey = getPrimaryKey(record);
		if (record.getRowKind() == RowKind.DELETE) {
			reduceBuffer.put(primaryKey, new Tuple2<>(true, null));
		} else if (reduceBuffer.containsKey(primaryKey)) {
			Tuple2<Boolean, RowData> value = reduceBuffer.get(primaryKey);
			value.f1 = mergeRow(record, value.f1);
		} else {
			reduceBuffer.put(primaryKey, new Tuple2<>(false, record));
		}
	}

	@Override
	public void executeBatch(ByteSQLTransaction transaction) throws ByteSQLException {
		String sql;
		for (Map.Entry<GenericRowData, Tuple2<Boolean, RowData>> entry : reduceBuffer.entrySet()) {
			Tuple2<Boolean, RowData> tuple = entry.getValue();
			if (tuple.f0) {
				GenericRowData pk = entry.getKey();
				sql = ByteSQLUtils.generateActualSql(deleteSQL, pk.getFields());
				transaction.rawQuery(sql);
			}
			if (tuple.f1 != null) {
				sql = generateUpsertSQLWithoutNull(tuple.f1);
				transaction.rawQuery(sql);
			}
		}
		transaction.commit();
		reduceBuffer.clear();
	}

	/**
	 * Generate a sql that ignore the null values.
	 */
	@VisibleForTesting
	protected String generateUpsertSQLWithoutNull(RowData row) throws ByteSQLException {
		List<Object> fields = new ArrayList<>(row.getArity());
		List<String> fieldNameList = new ArrayList<>(row.getArity());
		for (int i = 0; i < row.getArity(); i++) {
			if (!row.isNullAt(i)) {
				fields.add(fieldGetters[i].getFieldOrNull(row));
				fieldNameList.add(fieldNames[i]);
			}
		}
		String upsertFormatter = ByteSQLUtils
			.getUpsertStatement(options.getTableName(),
				fieldNameList.toArray(new String[]{}), insertOptions.getTtlSeconds());
		return ByteSQLUtils.generateActualSql(upsertFormatter, fields.toArray());
	}
}
