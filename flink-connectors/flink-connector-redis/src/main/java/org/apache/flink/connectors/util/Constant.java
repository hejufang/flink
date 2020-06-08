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

package org.apache.flink.connectors.util;

/**
 *  Default value and constants.
 */
public class Constant {

	public static final int GET_RESOURCE_MAX_RETRIES_DEFAULT = 5;
	public static final int FLUSH_MAX_RETRIES_DEFAULT = 5;
	public static final int BATCH_SIZE_DEFAULT = 10;
	public static final int TTL_DEFAULT = -1;

	public static final String STORAGE_REDIS = "redis";
	public static final String STORAGE_ABASE = "abase";

	public static final String INCR_MODE = "incr";
	public static final String INSERT_MODE = "insert";

	public static final String REDIS_DATATYPE_STRING = "string";
	public static final String REDIS_DATATYPE_HASH = "hash";
	public static final String REDIS_DATATYPE_LIST = "list";
	public static final String REDIS_DATATYPE_SET = "set";
	public static final String REDIS_DATATYPE_ZSET = "zset";
}
