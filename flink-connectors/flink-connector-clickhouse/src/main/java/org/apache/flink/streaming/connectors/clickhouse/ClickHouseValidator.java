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

package org.apache.flink.streaming.connectors.clickhouse;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.util.Preconditions;

import java.util.Optional;

/**
 * ClickHouse validator.
 */
public class ClickHouseValidator extends ConnectorDescriptorValidator {

	public static final String CONNECTOR_TYPE_VALUE_CLICKHOUSE = "clickhouse";

	public static final String CONNECTOR_URL = "connector.url";
	public static final String CONNECTOR_DB = "connector.db";
	public static final String CONNECTOR_TABLE = "connector.table";
	public static final String CONNECTOR_TABLE_SIGN_COLUMN = "connector.table.sign.column";
	public static final String CONNECTOR_DRIVER = "connector.driver";
	public static final String CONNECTOR_USERNAME = "connector.username";
	public static final String CONNECTOR_PASSWORD = "connector.password";
	public static final String CONNECTOR_PSM = "connector.psm";

	public static final String CONNECTOR_WRITE_FLUSH_MAX_ROWS = "connector.write.flush.max-rows";
	public static final String CONNECTOR_WRITE_FLUSH_INTERVAL = "connector.write.flush.interval";

	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		validateCommonProperties(properties);
		validateSinkProperties(properties);
	}

	private void validateCommonProperties(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_TABLE, false, 1);
		properties.validateString(CONNECTOR_TABLE_SIGN_COLUMN, false, 1);
		properties.validateString(CONNECTOR_URL, true);
		properties.validateString(CONNECTOR_PSM, true);
		properties.validateString(CONNECTOR_DRIVER, true);
		properties.validateString(CONNECTOR_USERNAME, true);
		properties.validateString(CONNECTOR_PASSWORD, true);

		final String url = properties.getOptionalString(CONNECTOR_URL).orElse(null);
		final String psm = properties.getOptionalString(CONNECTOR_PSM).orElse(null);

		if (null == url && null == psm) {
			throw new IllegalArgumentException("jdbc url or clickhouse psm must not be provided");
		}

		Optional<String> password = properties.getOptionalString(CONNECTOR_PASSWORD);
		if (password.isPresent()) {
			Preconditions.checkArgument(
				properties.getOptionalString(CONNECTOR_USERNAME).isPresent(),
				"Database username must be provided when database password is provided");
		}
	}

	private void validateSinkProperties(DescriptorProperties properties) {
		properties.validateInt(CONNECTOR_WRITE_FLUSH_MAX_ROWS, true);
		properties.validateDuration(CONNECTOR_WRITE_FLUSH_INTERVAL, true, 1);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
	}

}
