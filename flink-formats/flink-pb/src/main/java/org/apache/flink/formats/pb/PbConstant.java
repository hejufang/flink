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

package org.apache.flink.formats.pb;

/**
 * Constant key value of PB.
 */
public class PbConstant {
	public static final String KEY = "key";
	public static final String VALUE = "value";

	public static final String PB_METHOD_GET_DESCRIPTOR = "getDescriptor";
	public static final String PB_METHOD_NEW_BUILDER = "newBuilder";

	public static final String FORMAT_TYPE_VALUE = "pb";
	public static final String FORMAT_PB_CLASS = "format.pb-class";
	public static final String FORMAT_PB_WITH_WRAPPER = "format.pb-with-wrapper";
	public static final String FORMAT_PB_WRAPPER_NAME = "root";
	public static final String FORMAT_PB_SKIP_BYTES = "format.pb-skip-bytes";
	public static final String FORMAT_PB_SINK_WITH_SIZE_HEADER = "format.pb-sink-with-size-header";
	public static final String FORMAT_PB_MESSAGE = "format.pb-message";
	public static final String FORMAT_PB_FAIL_ON_DESERIALIZED = "format.fail-on-deserialized";

	public static final String FORMAT_BINLOG_TYPE_VALUE = "pb_binlog";
	public static final String FORMAT_BINLOG_TYPE_HEADER = "header";
	public static final String FORMAT_BINLOG_TYPE_ENTRY_TYPE = "entryType";
	public static final String FORMAT_BINLOG_TYPE_TRANSACTION_BEGIN = "TransactionBegin";
	public static final String FORMAT_BINLOG_TYPE_TRANSACTION_END = "TransactionEnd";
	public static final String FORMAT_BINLOG_TYPE_ROW_CHANGE = "RowChange";
	public static final String FORMAT_BINLOG_TYPE_STORE_VALUE = "storeValue";
}
