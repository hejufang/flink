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

package org.apache.flink.table.descriptors;

/**
 * The validator for Doris.
 */
public class DorisValidator extends ConnectorDescriptorValidator {
	public static final String DORIS = "doris";
	public static final String CONNECTOR_DORIS_FE_LIST = "connector.doris-fe-list";
	public static final String CONNECTOR_CLUSTER = "connector.cluster";
	public static final String CONNECTOR_DATA_CENTER = "connector.data-center";
	public static final String CONNECTOR_DORIS_FE_PSM = "connector.doris-fe-psm";
	public static final String CONNECTOR_USER = "connector.user";
	public static final String CONNECTOR_PASSWORD = "connector.password";
	public static final String CONNECTOR_DB_NAME = "connector.db-name";
	public static final String CONNECTOR_TABLE_NAME = "connector.table-name";
	public static final String CONNECTOR_KEYS = "connector.keys";
	public static final String CONNECTOR_TABLE_MODEL = "connector.table-model";
	public static final String CONNECTOR_DATA_FORMAT = "connector.data-format";
	public static final String CONNECTOR_COLUMN_SEPARATOR = "connector.column-separator";
	public static final String CONNECTOR_MAX_BYTES_PER_BATCH = "connector.max-bytes-per-batch";
	public static final String CONNECTOR_MAX_PENDING_BATCH_NUM = "connector.max-pending-batch-num";
	public static final String CONNECTOR_MAX_PENDING_TIME_MS = "connector.max-pending-time-ms";
	public static final String CONNECTOR_MAX_FILTER_RATIO = "connector.max-filter-ratio";
	public static final String CONNECTOR_RETRY_INTERVAL_MS = "connector.retry-interval-ms";
	public static final String CONNECTOR_MAX_RETRY_NUM = "connector.max-retry-num";
	public static final String CONNECTOR_FE_UPDATE_INTERVAL_MS = "connector.fe-update-interval-ms";
	public static final String CONNECTOR_PARALLELISM = "connector.parallelism";
	public static final String CONNECTOR_SEQUENCE_COLUMN = "connector.sequence-column";
	public static final String CONNECTOR_TIMEOUT_MS = "connector.timeout-ms";
	public static final String CONNECTOR_FIELD_MAPPING = "connector.field-mapping";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, DORIS, false);
		properties.validateString(CONNECTOR_CLUSTER, false, 1);
		properties.validateString(CONNECTOR_USER, false, 1);
		// password can be empty.
		properties.validateString(CONNECTOR_PASSWORD, false, 0);
		properties.validateString(CONNECTOR_DB_NAME, false, 1);
		properties.validateString(CONNECTOR_TABLE_NAME, false, 1);
		// keys can be empty.
		properties.validateString(CONNECTOR_KEYS, false, 0);

		properties.validateString(CONNECTOR_DORIS_FE_LIST, true, 1);
		properties.validateString(CONNECTOR_DATA_CENTER, true, 1);
		properties.validateString(CONNECTOR_DORIS_FE_PSM, true, 1);
		properties.validateString(CONNECTOR_TABLE_MODEL, true, 1);
		properties.validateString(CONNECTOR_DATA_FORMAT, true, 1);
		properties.validateString(CONNECTOR_COLUMN_SEPARATOR, true, 1);
		properties.validateInt(CONNECTOR_MAX_BYTES_PER_BATCH, true, 1);
		properties.validateInt(CONNECTOR_MAX_PENDING_BATCH_NUM, true, 1);
		properties.validateInt(CONNECTOR_MAX_PENDING_TIME_MS, true, 1);
		properties.validateFloat(CONNECTOR_MAX_FILTER_RATIO, true, 0);
		properties.validateInt(CONNECTOR_RETRY_INTERVAL_MS, true, 1);
		properties.validateInt(CONNECTOR_MAX_RETRY_NUM, true, 1);
		properties.validateInt(CONNECTOR_FE_UPDATE_INTERVAL_MS, true, 1);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
		properties.validateString(CONNECTOR_SEQUENCE_COLUMN, true, 1);
		properties.validateInt(CONNECTOR_TIMEOUT_MS, true, 1);
		properties.validateString(CONNECTOR_FIELD_MAPPING, true, 1);
	}
}
