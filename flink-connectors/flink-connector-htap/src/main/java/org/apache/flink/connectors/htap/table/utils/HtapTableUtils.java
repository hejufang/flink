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

package org.apache.flink.connectors.htap.table.utils;

import org.apache.flink.connectors.htap.connector.HtapFilterInfo;
import org.apache.flink.connectors.htap.connector.HtapTableInfo;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.expressions.Expression;
import org.apache.flink.table.types.DataType;

import com.bytedance.htap.meta.ColumnSchema;
import com.bytedance.htap.meta.Schema;

import java.util.Map;
import java.util.Optional;

/**
 * HtapTableUtils.
 */
public class HtapTableUtils {

	// TODO: schema and props can be put into HtapTableInfo for extension
	public static HtapTableInfo createTableInfo(
			String tableName,
			TableSchema schema,
			Map<String, String> props) {
		HtapTableInfo tableInfo = HtapTableInfo.forTable(tableName);
		return tableInfo;
	}

	public static TableSchema htapToFlinkSchema(Schema schema) {
		TableSchema.Builder builder = TableSchema.builder();

		for (ColumnSchema column : schema.getColumns()) {
			DataType flinkType =
				HtapTypeUtils.toFlinkType(column.getType(), column.getMysqlType(),
					column.getTypeAttributes()).nullable();
			builder.field(column.getName(), flinkType);
		}

		return builder.build();
	}

	/**
	 * Converts Flink Expression to HtapFilterInfo.
	 */
	public static Optional<HtapFilterInfo> toHtapFilterInfo(Expression predicate) {
		// TODO: htap not support predicates push down now
		return Optional.empty();
	}
}
