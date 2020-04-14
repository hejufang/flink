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

package org.apache.flink.table.descripters;

import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.FormatDescriptorValidator;

/**
 * Binlog validator.
 */
public class BinlogValidator extends FormatDescriptorValidator {
	public static final String FORMAT_TYPE_VALUE = "binlog";
	public static final String FORMAT_IGNORE_PARSE_ERRORS = "format.ignore-parse-errors";
	public static final String FORMAT_TARGET_TABLE = "format.target-table";
	public static final String MESSAGE = "message";
	public static final String ENTRY = "entry";
	public static final String PAYLOAD = "payload";
	public static final String HEADER = "header";
	public static final String TABLE = "table";
	public static final String BODY = "body";
	public static final String BINLOG_HEADER = "binlog_header";
	public static final String BINLOG_BODY = "binlog_body";
	public static final String ROWDATAS = "rowdatas";
	public static final String BEFORE_IMAGE = "before_image";
	public static final String AFTER_IMAGE = "after_image";
	public static final String BEFORE_PREFIX = "before_";
	public static final String AFTER_PREFIX = "after_";
	public static final String INDEX_COLUMN = "index";
	public static final String NAME_COLUMN = "name";
	public static final String IS_PK_COLUMN = "is_pk";
	public static final String UPDATED_COLUMN = "updated";
	public static final String IS_NULLABLE_COLUMN = "is_nullable";
	public static final String NULL_COLUMN = "null";
	public static final String SQL_TYPE_COLUMN = "sql_type";
	public static final String VALUE_COLUMN = "value";
	public static final String IS_UNSIGNED_COLUMN = "is_unsigned";

	// --------------- values in binlog begin --------------------
	public static final String CHAR = "char";
	public static final String VARCHAR = "varchar";
	public static final String TEXT = "text";
	public static final String TINYINT = "tinyint";
	public static final String SMALLINT = "smallint";
	public static final String INT = "int";
	public static final String BIGINT = "bigint";
	public static final String REAL = "real";
	public static final String FLOAT = "float";
	public static final String DOUBLE = "double";
	public static final String DATE = "date";
	public static final String TIME = "time";
	public static final String TIMESTAMP = "timestamp";
	public static final String DATETIME = "datetime";
	// --------------- values in binlog end --------------------

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateBoolean(FORMAT_IGNORE_PARSE_ERRORS, true);
		properties.validateString(FORMAT_TARGET_TABLE, false, 1);
	}
}
