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

package org.apache.flink.table.descriptors;

import org.apache.flink.annotation.Internal;
import org.apache.flink.util.Preconditions;

/**
 * Validator for {@link ConnectorDescriptor}.
 */
@Internal
public abstract class ConnectorDescriptorValidator implements DescriptorValidator {

	/**
	 * Prefix for connector-related properties.
	 */
	public static final String CONNECTOR = "connector";

	/**
	 * Key for describing the type of the connector. Usually used for factory discovery.
	 */
	public static final String CONNECTOR_TYPE = "connector.type";

	/**
	 * Key for describing the property version. This property can be used for backwards
	 * compatibility in case the property format changes.
	 */
	public static final String CONNECTOR_PROPERTY_VERSION = "connector.property-version";

	/**
	 * Key for describing the version of the connector. This property can be used for different
	 * connector versions (e.g. Kafka 0.8 or Kafka 0.11).
	 */
	public static final String CONNECTOR_VERSION = "connector.version";

	/**
	 * Key for describing the parallelism of the connector.
	 */
	public static final String CONNECTOR_PARALLELISM = "connector.parallelism";

	/**
	 * Key for describing the keyby field.
	 */
	public static final String CONNECTOR_KEYBY_FIELDS = "connector.keyby-fields";

	/**
	 * Key for whether log failures only.
	 * */
	public static final String CONNECTOR_LOG_FAILURES_ONLY = "connector.log-failures-only";

	/**
	 * Key for field indices whether cache null value in lookup join.
	 */
	public static final String CONNECTOR_LOOKUP_CACHE_NULL_VALUE = "connector.cache-null-value";

	/**
	 * Key for fields and metadata mapping.
	 */
	public static final String METADATA_FIELDS_MAPPING = "metadata.fields.mapping";

	/**
	 * Key for field indices and metadata mapping, for internal use.
	 */
	public static final String METADATA_FIELD_INDEX_MAPPING = "metadata.field-index.mapping";

	/**
	 * Key for field names of primary keys, delimited by ','.
	 */
	public static final String CONNECTOR_KEY_FIELDS = "connector.key.fields";

	/**
	 * Key for flag to indicate whether to hash the input stream by join key.
	 */
	public static final String CONNECTOR_LOOKUP_ENABLE_INPUT_KEYBY = "connector.lookup.enable-input-keyby";

	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateString(CONNECTOR_TYPE, false, 1);
		properties.validateInt(CONNECTOR_PROPERTY_VERSION, true, 0);
		properties.validateInt(CONNECTOR_PARALLELISM, true, 1);
	}

	protected void checkAllOrNone(DescriptorProperties properties, String[] propertyNames) {
		int presentCount = 0;
		for (String name : propertyNames) {
			if (properties.getOptionalString(name).isPresent()) {
				presentCount++;
			}
		}
		Preconditions.checkArgument(presentCount == 0 || presentCount == propertyNames.length,
			"Either all or none of the following properties should be provided:\n" + String.join("\n", propertyNames));
	}
}
