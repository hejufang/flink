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
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.logical.RowType;

import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;

import java.util.ArrayList;
import java.util.List;

/**
 * AppendOnly executor that executes only upsert statements
 * for given the records (without any pre-processing).
 */
public class AppendOnlyStatementExecutor extends ByteSQLBatchStatementExecutor {
	private static final long serialVersionUID = 1L;
	private final List<RowData> buffer;

	public AppendOnlyStatementExecutor(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType) {
		super(options, insertOptions, rowType);
		this.buffer = new ArrayList<>();
	}

	@Override
	public boolean isBufferEmpty() {
		return buffer.isEmpty();
	}

	@Override
	public void addToBatch(RowData record){
		buffer.add(record);
	}

	@Override
	public void executeBatch(ByteSQLTransaction transaction) throws ByteSQLException {
		int[] writableColIndices = insertOptions.getWritableColIndices();
		for (RowData record: buffer) {
			Object[] fields = new Object[writableColIndices.length];
			for (int i = 0; i < writableColIndices.length; i++) {
				fields[i] = fieldGetters[writableColIndices[i]].getFieldOrNull(record);
			}
			String sql = ByteSQLUtils.generateActualSql(upsertSQL, fields);
			transaction.rawQuery(sql);
		}
		transaction.commit();
		buffer.clear();
	}
}
