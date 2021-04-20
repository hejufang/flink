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

package org.apache.flink.table.planner.utils;

import org.apache.flink.streaming.api.operators.DebugLoggingConverter;
import org.apache.flink.streaming.runtime.streamrecord.StreamRecord;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.conversion.DataStructureConverter;

/**
 * A {{DebugLoggingConverter}} which is used to convert SQL's interval data
 * structures to printable string.
 */
public class DataTypeDebugLoggingConverter implements DebugLoggingConverter {

	public static final long serialVersionUID = 1L;

	private final DataStructureConverter<Object, Object> converter;

	public DataTypeDebugLoggingConverter(DataStructureConverter<Object, Object> converter) {
		this.converter = converter;
	}

	@Override
	public String convert(StreamRecord<?> object) {
		RowData value = (RowData) object.getValue();
		// this format is same with `PrintTableSinkFactory`
		return String.format("%s(%s)",
			value.getRowKind().shortString(),
			converter.toExternal(value));
	}
}
