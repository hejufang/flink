/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.state.table.connector.converter;

import org.apache.flink.api.common.typeutils.TypeSerializer;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.types.Row;

/**
 * OperatorStateRowDataConverter.
 */
public class OperatorStateRowDataConverter<T> implements RowDataConverter<T> {

	private final DynamicTableSource.DataStructureConverter converter;
	private FormatterFactory.Formatter formatter;

	public OperatorStateRowDataConverter(DynamicTableSource.DataStructureConverter converter, TypeSerializer valueSerializer) {
		this.converter = converter;
		this.formatter = FormatterFactory.getFormatter(valueSerializer);
	}

	@Override
	public RowData converterToRowData(T value, Context context) {

		Row row = new Row(1);
		row.setField(0, formatter.format(value));

		return (RowData) converter.toInternal(row);
	}
}
