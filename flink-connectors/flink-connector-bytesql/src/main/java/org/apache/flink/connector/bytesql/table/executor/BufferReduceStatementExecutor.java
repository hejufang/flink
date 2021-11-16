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

import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLInsertOptions;
import org.apache.flink.connector.bytesql.table.descriptors.ByteSQLOptions;
import org.apache.flink.connector.bytesql.util.ByteSQLUtils;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.types.RowKind;

import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;

import java.util.Map;

/**
 * A batch Executor in which the buffer will be reduced by the primary keys.
 */
public class BufferReduceStatementExecutor extends KeyedBatchStatementExecutor<RowData> {
	private static final long serialVersionUID = 1L;

	public BufferReduceStatementExecutor(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType) {
		super(options, insertOptions, rowType);
	}

	/**
	 * When a delete message coming, a null value will be stored to indicate a delete command.
	 */
	@Override
	public void addToBatch(RowData record){
		GenericRowData primaryKey = getPrimaryKey(record);
		if (record.getRowKind() == RowKind.DELETE) {
			reduceBuffer.put(primaryKey, null);
		} else if (reduceBuffer.containsKey(primaryKey)) {
			RowData merged = mergeRow(record, reduceBuffer.get(primaryKey));
			reduceBuffer.put(primaryKey, merged);
		} else {
			reduceBuffer.put(primaryKey, record);
		}
	}

	@Override
	public void executeBatch(ByteSQLTransaction transaction) throws ByteSQLException {
		String sql;
		for (Map.Entry<GenericRowData, RowData> entry : reduceBuffer.entrySet()) {
			RowData value = entry.getValue();
			if (value != null) {
				Object[] fields = new Object[value.getArity()];
				for (int i = 0; i < value.getArity(); i++) {
					fields[i] = fieldGetters[i].getFieldOrNull(value);
				}
				sql = ByteSQLUtils.generateActualSql(upsertSQL, fields);
			} else {
				GenericRowData pk = entry.getKey();
				sql = ByteSQLUtils.generateActualSql(deleteSQL, pk.getFields());
			}
			transaction.rawQuery(sql);
		}
		transaction.commit();
		reduceBuffer.clear();
	}
}
