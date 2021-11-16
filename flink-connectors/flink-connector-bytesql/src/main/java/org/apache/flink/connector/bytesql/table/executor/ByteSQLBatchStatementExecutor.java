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
import org.apache.flink.table.data.RowData.FieldGetter;
import org.apache.flink.table.types.logical.RowType;

import com.bytedance.infra.bytesql4j.ByteSQLTransaction;
import com.bytedance.infra.bytesql4j.exception.ByteSQLException;

import java.io.Serializable;
import java.util.List;
import java.util.stream.IntStream;

/**
 * Abstract Sink Executor.
 */
public abstract class ByteSQLBatchStatementExecutor implements Serializable {

	protected final ByteSQLOptions options;
	protected final ByteSQLInsertOptions insertOptions;
	protected final String upsertSQL;

	protected final String[] fieldNames;
	protected final FieldGetter[] fieldGetters;

	public ByteSQLBatchStatementExecutor(
			ByteSQLOptions options,
			ByteSQLInsertOptions insertOptions,
			RowType rowType) {
		this.options = options;
		this.insertOptions = insertOptions;
		List<String> nameList = rowType.getFieldNames();
		this.fieldNames = nameList.toArray(new String[]{});
		this.upsertSQL = ByteSQLUtils
			.getUpsertStatement(options.getTableName(), fieldNames, insertOptions.getTtlSeconds());
		this.fieldGetters = IntStream
			.range(0, fieldNames.length)
			.mapToObj(pos -> RowData.createFieldGetter(rowType.getTypeAt(pos), pos))
			.toArray(RowData.FieldGetter[]::new);
	}

	public abstract void addToBatch(RowData record);

	public abstract void executeBatch(ByteSQLTransaction transaction) throws ByteSQLException;

	public abstract boolean isBufferEmpty();
}
