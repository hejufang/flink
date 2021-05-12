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

package org.apache.flink.formats.binlog;

import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.DecimalData;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.data.StringData;
import org.apache.flink.table.data.TimestampData;
import org.apache.flink.table.runtime.functions.SqlDateTimeUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.BigIntType;
import org.apache.flink.table.types.logical.BooleanType;
import org.apache.flink.table.types.logical.DateType;
import org.apache.flink.table.types.logical.DecimalType;
import org.apache.flink.table.types.logical.IntType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.table.types.logical.RowType;
import org.apache.flink.table.types.logical.SmallIntType;
import org.apache.flink.table.types.logical.TimeType;
import org.apache.flink.table.types.logical.TimestampType;
import org.apache.flink.table.types.logical.TinyIntType;
import org.apache.flink.table.types.logical.VarBinaryType;
import org.apache.flink.table.types.logical.VarCharType;

import org.junit.Assert;
import org.junit.Test;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;

/**
 * BinlogRowDeserializationSchemaTest.
 */
public class BinlogRowDeserializationSchemaTest {
	private static final int TEST_FIELD_NUM = 3;
	private static final byte[] TEST_MESSAGE = new byte[]{16, 7, 24, 1, 34, -86, 24, 10, -33, 2, 24, -85, -121, -19, -2, 5, 40, 0, 48, -84, -113, -123, -54, 2, 56, 4, 64, -85, -121, -19, -2, 5, 74, 20, 102, 108, 105, 110, 107, 95, 107, 101, 121, 95, 105, 110, 100, 105, 99, 97, 116, 111, 114, 115, 82, 15, 102, 108, 105, 110, 107, 95, 116, 121, 112, 101, 95, 116, 101, 115, 116, 88, 3, 98, 11, 10, 7, 80, 82, 73, 77, 65, 82, 89, 16, 0, 106, -103, 1, 10, 7, 103, 116, 105, 100, 83, 101, 116, 18, -115, 1, 53, 97, 54, 53, 52, 102, 56, 99, 45, 54, 53, 55, 102, 45, 49, 49, 101, 56, 45, 98, 51, 53, 101, 45, 54, 99, 57, 50, 98, 102, 54, 51, 57, 55, 51, 55, 58, 49, 45, 52, 57, 49, 51, 55, 57, 51, 48, 44, 102, 99, 51, 99, 57, 56, 101, 55, 45, 56, 53, 100, 99, 45, 49, 49, 101, 56, 45, 56, 54, 54, 99, 45, 54, 99, 57, 50, 98, 102, 53, 100, 55, 50, 50, 54, 58, 49, 45, 51, 49, 57, 52, 53, 50, 44, 53, 53, 48, 99, 55, 50, 53, 101, 45, 54, 53, 55, 102, 45, 49, 49, 101, 56, 45, 98, 51, 53, 101, 45, 54, 99, 57, 50, 98, 102, 54, 51, 57, 50, 49, 100, 58, 49, 45, 49, 50, 49, 56, 54, 49, 56, 57, 106, 31, 10, 11, 108, 111, 103, 102, 105, 108, 101, 78, 97, 109, 101, 18, 16, 109, 121, 115, 113, 108, 45, 98, 105, 110, 46, 48, 48, 49, 54, 55, 50, 106, 26, 10, 13, 108, 111, 103, 102, 105, 108, 101, 79, 102, 102, 115, 101, 116, 18, 9, 56, 55, 56, 54, 55, 57, 54, 51, 50, 106, 18, 10, 11, 101, 118, 101, 110, 116, 76, 101, 110, 103, 116, 104, 18, 3, 51, 53, 48, 106, 15, 10, 8, 122, 111, 110, 101, 78, 97, 109, 101, 18, 3, 67, 83, 84, 106, 19, 10, 10, 122, 111, 110, 101, 79, 102, 102, 115, 101, 116, 18, 5, 50, 56, 56, 48, 48, 18, -59, 21, 10, -69, 21, 10, 53, 8, 0, 18, 2, 105, 100, 24, 1, 40, 0, 48, 0, 58, 3, 105, 110, 116, 66, 1, 52, 72, 1, 106, 29, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 16, 105, 110, 116, 40, 49, 49, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 10, 58, 8, 1, 18, 9, 116, 101, 115, 116, 95, 100, 97, 116, 101, 24, 0, 40, 0, 48, 0, 58, 4, 100, 97, 116, 101, 66, 10, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 100, 97, 116, 101, 10, 56, 8, 2, 18, 9, 116, 101, 115, 116, 95, 116, 105, 109, 101, 24, 0, 40, 0, 48, 0, 58, 4, 116, 105, 109, 101, 66, 8, 49, 48, 58, 51, 55, 58, 48, 48, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 116, 105, 109, 101, 10, 79, 8, 3, 18, 13, 116, 101, 115, 116, 95, 100, 97, 116, 101, 116, 105, 109, 101, 24, 0, 40, 0, 48, 0, 58, 8, 100, 97, 116, 101, 116, 105, 109, 101, 66, 19, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 32, 49, 48, 58, 52, 51, 58, 53, 56, 72, 0, 106, 21, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 8, 100, 97, 116, 101, 116, 105, 109, 101, 10, 82, 8, 4, 18, 14, 116, 101, 115, 116, 95, 116, 105, 109, 101, 115, 116, 97, 109, 112, 24, 0, 40, 0, 48, 0, 58, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, 66, 19, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 32, 49, 48, 58, 52, 51, 58, 53, 56, 72, 0, 106, 22, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, 10, 73, 8, 5, 18, 11, 116, 101, 115, 116, 95, 98, 105, 110, 97, 114, 121, 24, 0, 40, 1, 48, 0, 58, 9, 118, 97, 114, 98, 105, 110, 97, 114, 121, 66, 8, 1, -2, 3, 23, 21, 32, -10, -1, 72, 0, 106, 27, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 14, 118, 97, 114, 98, 105, 110, 97, 114, 121, 40, 49, 48, 48, 41, 10, 56, 8, 6, 18, 9, 116, 101, 115, 116, 95, 98, 108, 111, 98, 24, 0, 40, 1, 48, 0, 58, 4, 98, 108, 111, 98, 66, 8, -23, 32, -95, 84, 75, 5, 6, 44, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 98, 108, 111, 98, 10, 79, 8, 7, 18, 9, 116, 101, 115, 116, 95, 116, 101, 120, 116, 24, 0, 40, 1, 48, 0, 58, 4, 116, 101, 120, 116, 66, 31, 116, 104, 105, 115, 32, 105, 115, 32, 122, 104, 97, 110, 103, 121, 117, 110, 102, 97, 110, 32, 98, 105, 110, 108, 111, 103, 32, 116, 101, 115, 116, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 116, 101, 120, 116, 10, 61, 8, 8, 18, 12, 116, 101, 115, 116, 95, 116, 105, 110, 121, 105, 110, 116, 24, 0, 40, 1, 48, 0, 58, 7, 116, 105, 110, 121, 105, 110, 116, 66, 1, 49, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 116, 105, 110, 121, 105, 110, 116, 40, 49, 41, 10, 66, 8, 9, 18, 13, 116, 101, 115, 116, 95, 115, 109, 97, 108, 108, 105, 110, 116, 24, 0, 40, 1, 48, 0, 58, 8, 115, 109, 97, 108, 108, 105, 110, 116, 66, 3, 49, 50, 55, 72, 0, 106, 24, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 11, 115, 109, 97, 108, 108, 105, 110, 116, 40, 54, 41, 10, 54, 8, 10, 18, 12, 116, 101, 115, 116, 95, 105, 110, 116, 101, 103, 101, 114, 24, 0, 40, 1, 48, 0, 58, 3, 105, 110, 116, 66, 1, 51, 72, 0, 106, 20, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 7, 105, 110, 116, 40, 49, 49, 41, 10, 71, 8, 11, 18, 11, 116, 101, 115, 116, 95, 98, 105, 103, 105, 110, 116, 24, 0, 40, 1, 48, 0, 58, 6, 98, 105, 103, 105, 110, 116, 66, 13, 49, 53, 56, 54, 51, 49, 51, 56, 51, 56, 48, 48, 48, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 98, 105, 103, 105, 110, 116, 40, 50, 48, 41, 10, 69, 8, 12, 18, 17, 116, 101, 115, 116, 95, 116, 101, 115, 116, 95, 100, 101, 99, 105, 109, 97, 108, 24, 0, 40, 1, 48, 0, 58, 7, 100, 101, 99, 105, 109, 97, 108, 66, 1, 48, 72, 0, 106, 26, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 13, 100, 101, 99, 105, 109, 97, 108, 40, 49, 48, 44, 48, 41, 10, 85, 8, 13, 18, 12, 116, 101, 115, 116, 95, 118, 97, 114, 99, 104, 97, 114, 24, 0, 40, 1, 48, 0, 58, 7, 118, 97, 114, 99, 104, 97, 114, 66, 23, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 118, 97, 114, 99, 104, 97, 114, 46, 72, 0, 106, 25, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 12, 118, 97, 114, 99, 104, 97, 114, 40, 50, 53, 53, 41, 10, 76, 8, 14, 18, 15, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 95, 116, 101, 115, 116, 24, 0, 40, 1, 48, 0, 58, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 66, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 10, 68, 8, 15, 18, 13, 108, 111, 110, 103, 116, 101, 120, 116, 95, 116, 101, 115, 116, 24, 0, 40, 1, 48, 0, 58, 8, 108, 111, 110, 103, 116, 101, 120, 116, 66, 8, 108, 111, 110, 103, 116, 101, 120, 116, 72, 0, 106, 21, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 8, 108, 111, 110, 103, 116, 101, 120, 116, 10, 77, 8, 16, 18, 12, 100, 101, 99, 105, 109, 97, 108, 95, 116, 101, 115, 116, 24, 0, 40, 1, 48, 0, 58, 7, 100, 101, 99, 105, 109, 97, 108, 66, 14, 49, 46, 50, 51, 52, 53, 54, 55, 56, 57, 101, 43, 48, 57, 72, 0, 106, 26, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 13, 100, 101, 99, 105, 109, 97, 108, 40, 49, 49, 44, 48, 41, 10, 65, 8, 17, 18, 17, 116, 101, 115, 116, 95, 117, 110, 115, 105, 103, 110, 101, 100, 95, 105, 110, 116, 24, 0, 40, 1, 48, 1, 58, 3, 105, 110, 116, 72, 1, 106, 29, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 16, 105, 110, 116, 40, 49, 49, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 10, 72, 8, 18, 18, 18, 116, 101, 115, 116, 95, 117, 110, 115, 105, 103, 110, 101, 100, 95, 108, 111, 110, 103, 24, 0, 40, 1, 48, 1, 58, 6, 98, 105, 103, 105, 110, 116, 72, 1, 106, 32, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 19, 98, 105, 103, 105, 110, 116, 40, 50, 48, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 18, 55, 8, 0, 18, 2, 105, 100, 24, 1, 32, 0, 40, 0, 48, 0, 58, 3, 105, 110, 116, 66, 1, 52, 72, 1, 106, 29, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 16, 105, 110, 116, 40, 49, 49, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 18, 60, 8, 1, 18, 9, 116, 101, 115, 116, 95, 100, 97, 116, 101, 24, 0, 32, 0, 40, 0, 48, 0, 58, 4, 100, 97, 116, 101, 66, 10, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 100, 97, 116, 101, 18, 58, 8, 2, 18, 9, 116, 101, 115, 116, 95, 116, 105, 109, 101, 24, 0, 32, 0, 40, 0, 48, 0, 58, 4, 116, 105, 109, 101, 66, 8, 49, 48, 58, 51, 55, 58, 48, 48, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 116, 105, 109, 101, 18, 81, 8, 3, 18, 13, 116, 101, 115, 116, 95, 100, 97, 116, 101, 116, 105, 109, 101, 24, 0, 32, 0, 40, 0, 48, 0, 58, 8, 100, 97, 116, 101, 116, 105, 109, 101, 66, 19, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 32, 49, 48, 58, 52, 51, 58, 53, 56, 72, 0, 106, 21, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 8, 100, 97, 116, 101, 116, 105, 109, 101, 18, 84, 8, 4, 18, 14, 116, 101, 115, 116, 95, 116, 105, 109, 101, 115, 116, 97, 109, 112, 24, 0, 32, 0, 40, 0, 48, 0, 58, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, 66, 19, 50, 48, 50, 48, 45, 48, 52, 45, 48, 56, 32, 49, 48, 58, 52, 51, 58, 53, 56, 72, 0, 106, 22, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 9, 116, 105, 109, 101, 115, 116, 97, 109, 112, 18, 75, 8, 5, 18, 11, 116, 101, 115, 116, 95, 98, 105, 110, 97, 114, 121, 24, 0, 32, 0, 40, 1, 48, 0, 58, 9, 118, 97, 114, 98, 105, 110, 97, 114, 121, 66, 8, 1, -2, 3, 23, 21, 32, -10, -1, 72, 0, 106, 27, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 14, 118, 97, 114, 98, 105, 110, 97, 114, 121, 40, 49, 48, 48, 41, 18, 58, 8, 6, 18, 9, 116, 101, 115, 116, 95, 98, 108, 111, 98, 24, 0, 32, 0, 40, 1, 48, 0, 58, 4, 98, 108, 111, 98, 66, 8, -23, 32, -95, 84, 75, 5, 6, 44, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 98, 108, 111, 98, 18, 81, 8, 7, 18, 9, 116, 101, 115, 116, 95, 116, 101, 120, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 4, 116, 101, 120, 116, 66, 31, 116, 104, 105, 115, 32, 105, 115, 32, 122, 104, 97, 110, 103, 121, 117, 110, 102, 97, 110, 32, 98, 105, 110, 108, 111, 103, 32, 116, 101, 115, 116, 72, 0, 106, 17, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 4, 116, 101, 120, 116, 18, 63, 8, 8, 18, 12, 116, 101, 115, 116, 95, 116, 105, 110, 121, 105, 110, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 7, 116, 105, 110, 121, 105, 110, 116, 66, 1, 49, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 116, 105, 110, 121, 105, 110, 116, 40, 49, 41, 18, 68, 8, 9, 18, 13, 116, 101, 115, 116, 95, 115, 109, 97, 108, 108, 105, 110, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 8, 115, 109, 97, 108, 108, 105, 110, 116, 66, 3, 49, 50, 55, 72, 0, 106, 24, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 11, 115, 109, 97, 108, 108, 105, 110, 116, 40, 54, 41, 18, 56, 8, 10, 18, 12, 116, 101, 115, 116, 95, 105, 110, 116, 101, 103, 101, 114, 24, 0, 32, 0, 40, 1, 48, 0, 58, 3, 105, 110, 116, 66, 1, 51, 72, 0, 106, 20, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 7, 105, 110, 116, 40, 49, 49, 41, 18, 73, 8, 11, 18, 11, 116, 101, 115, 116, 95, 98, 105, 103, 105, 110, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 6, 98, 105, 103, 105, 110, 116, 66, 13, 49, 53, 56, 54, 51, 49, 51, 56, 51, 56, 48, 48, 48, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 98, 105, 103, 105, 110, 116, 40, 50, 48, 41, 18, 71, 8, 12, 18, 17, 116, 101, 115, 116, 95, 116, 101, 115, 116, 95, 100, 101, 99, 105, 109, 97, 108, 24, 0, 32, 0, 40, 1, 48, 0, 58, 7, 100, 101, 99, 105, 109, 97, 108, 66, 1, 48, 72, 0, 106, 26, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 13, 100, 101, 99, 105, 109, 97, 108, 40, 49, 48, 44, 48, 41, 18, 87, 8, 13, 18, 12, 116, 101, 115, 116, 95, 118, 97, 114, 99, 104, 97, 114, 24, 0, 32, 0, 40, 1, 48, 0, 58, 7, 118, 97, 114, 99, 104, 97, 114, 66, 23, 84, 104, 105, 115, 32, 105, 115, 32, 97, 32, 116, 101, 115, 116, 32, 118, 97, 114, 99, 104, 97, 114, 46, 72, 0, 106, 25, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 12, 118, 97, 114, 99, 104, 97, 114, 40, 50, 53, 53, 41, 18, 78, 8, 14, 18, 15, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 95, 116, 101, 115, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 66, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 72, 0, 106, 23, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 10, 109, 101, 100, 105, 117, 109, 116, 101, 120, 116, 18, 70, 8, 15, 18, 13, 108, 111, 110, 103, 116, 101, 120, 116, 95, 116, 101, 115, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 8, 108, 111, 110, 103, 116, 101, 120, 116, 66, 8, 108, 111, 110, 103, 116, 101, 120, 116, 72, 0, 106, 21, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 8, 108, 111, 110, 103, 116, 101, 120, 116, 18, 79, 8, 16, 18, 12, 100, 101, 99, 105, 109, 97, 108, 95, 116, 101, 115, 116, 24, 0, 32, 0, 40, 1, 48, 0, 58, 7, 100, 101, 99, 105, 109, 97, 108, 66, 14, 49, 46, 50, 51, 52, 53, 54, 55, 56, 57, 101, 43, 48, 57, 72, 0, 106, 26, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 13, 100, 101, 99, 105, 109, 97, 108, 40, 49, 49, 44, 48, 41, 18, 79, 8, 17, 18, 17, 116, 101, 115, 116, 95, 117, 110, 115, 105, 103, 110, 101, 100, 95, 105, 110, 116, 24, 0, 32, 1, 40, 1, 48, 0, 58, 3, 105, 110, 116, 66, 10, 50, 49, 52, 55, 52, 56, 51, 54, 52, 56, 72, 1, 106, 29, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 16, 105, 110, 116, 40, 49, 49, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 18, 95, 8, 18, 18, 18, 116, 101, 115, 116, 95, 117, 110, 115, 105, 103, 110, 101, 100, 95, 108, 111, 110, 103, 24, 0, 32, 1, 40, 1, 48, 0, 58, 6, 98, 105, 103, 105, 110, 116, 66, 19, 57, 50, 50, 51, 51, 55, 50, 48, 51, 54, 56, 53, 52, 55, 55, 53, 56, 48, 56, 72, 1, 106, 32, 10, 9, 109, 121, 115, 113, 108, 84, 121, 112, 101, 18, 19, 98, 105, 103, 105, 110, 116, 40, 50, 48, 41, 32, 117, 110, 115, 105, 103, 110, 101, 100, 16, 3, 32, -47, -20, -124, 3};

	@Test
	public void testDeserialization() throws Exception {
		RowType rowType = createRowType();
		BinlogRowDeserializationSchema schema =
			new BinlogRowDeserializationSchema(rowType, null, "flink_type_test",
				false, BinlogOptions.BINLOG_HEADER, BinlogOptions.BINLOG_BODY, new HashSet<>());
		schema.open(null);
		RowData rowData = schema.deserialize(TEST_MESSAGE);
		int fieldNum = 2;
		String newText = "this is zhangyunfan binlog test";

		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), 4);
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			SqlDateTimeUtils.dateToInternal(new Date(1586275200000L)));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			SqlDateTimeUtils.timeToInternal(new Time(9420000)));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			TimestampData.fromTimestamp(new Timestamp(1586313838000L)));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), StringData.fromString(newText));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), (byte) 1);
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), (short) 127);
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), 1586313838000L);
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			StringData.fromString("This is a test varchar."));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), StringData.fromString("mediumtext"));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), StringData.fromString("longtext"));
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			DecimalData.fromBigDecimal(new BigDecimal("1.23456789e+09"), 10, 0));
		assertTestRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), null, Integer.MAX_VALUE + 1L, true);
		assertTestRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM), null,
			DecimalData.fromBigDecimal(new BigDecimal(Long.MAX_VALUE).add(BigDecimal.ONE), 19, 0), true);
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum++, TEST_FIELD_NUM),
			new byte[]{-23, 32, -95, 84, 75, 5, 6, 44});
		assertUnChangedRow((GenericRowData) rowData.getRow(fieldNum, TEST_FIELD_NUM),
			new byte[]{1, -2, 3, 23, 21, 32, -10, -1});
	}

	private RowType createRowType() {
		BinlogRowFormatFactory binlogRowFormatFactory = new BinlogRowFormatFactory();
		TableSchema tableSchema = binlogRowFormatFactory.getTableSchema(new HashMap<>());

		List<RowType.RowField> rowFields = new ArrayList<>();
		DataType[] types = tableSchema.getFieldDataTypes();
		String[] fields = tableSchema.getFieldNames();
		for (int i = 0; i < tableSchema.getFieldCount(); i++) {
			rowFields.add(new RowType.RowField(fields[i], types[i].getLogicalType()));
		}
		rowFields.add(new RowType.RowField("id", getInnerRowType(() -> new IntType(true))));
		rowFields.add(new RowType.RowField("test_date", getInnerRowType(() -> new DateType(true))));
		rowFields.add(new RowType.RowField("test_time", getInnerRowType(TimeType::new)));
		rowFields.add(new RowType.RowField("test_datetime", getInnerRowType(TimestampType::new)));
		rowFields.add(new RowType.RowField("test_text", getInnerRowType(VarCharType::new)));
		rowFields.add(new RowType.RowField("test_tinyint", getInnerRowType(() -> new TinyIntType(true))));
		rowFields.add(new RowType.RowField("test_smallint", getInnerRowType(() -> new SmallIntType(true))));
		rowFields.add(new RowType.RowField("test_bigint", getInnerRowType(() -> new BigIntType(true))));
		rowFields.add(new RowType.RowField("test_varchar", getInnerRowType(VarCharType::new)));
		rowFields.add(new RowType.RowField("mediumtext_test", getInnerRowType(VarCharType::new)));
		rowFields.add(new RowType.RowField("longtext_test", getInnerRowType(VarCharType::new)));
		rowFields.add(new RowType.RowField("decimal_test", getInnerRowType(DecimalType::new)));
		rowFields.add(new RowType.RowField("test_unsigned_int", getInnerRowType(BigIntType::new)));
		rowFields.add(new RowType.RowField("test_unsigned_long", getInnerRowType(() -> new DecimalType(19))));
		rowFields.add(new RowType.RowField("test_blob", getInnerRowType(VarBinaryType::new)));
		rowFields.add(new RowType.RowField("test_binary", getInnerRowType(VarBinaryType::new)));
		return new RowType(rowFields);
	}

	private RowType getInnerRowType(LogicalTypeGetter typeGetter) {
		List<RowType.RowField> rowFields = new ArrayList<>();
		rowFields.add(new RowType.RowField("before_value", typeGetter.get()));
		rowFields.add(new RowType.RowField("after_value", typeGetter.get()));
		rowFields.add(new RowType.RowField("after_updated", new BooleanType(true)));
		return new RowType(rowFields);
	}

	private void assertUnChangedRow(GenericRowData rowData, Object value) {
		assertTestRow(rowData, value, value, false);
	}

	private void assertTestRow(GenericRowData rowData, Object oldValue, Object newValue, boolean changed) {
		assertEquals(rowData.getField(0), oldValue);
		assertEquals(rowData.getField(1), newValue);
		assertEquals(rowData.getBoolean(2), changed);
	}

	private void assertEquals(Object expected, Object actual) {
		if (expected instanceof byte[] && actual instanceof byte[]) {
			Assert.assertArrayEquals((byte[]) expected, (byte[]) actual);
		} else {
			Assert.assertEquals(expected, actual);
		}
	}

	interface LogicalTypeGetter {
		LogicalType get();
	}
}
