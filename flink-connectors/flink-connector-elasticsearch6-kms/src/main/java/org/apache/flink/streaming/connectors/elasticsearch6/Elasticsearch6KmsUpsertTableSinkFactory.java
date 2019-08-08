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

package org.apache.flink.streaming.connectors.elasticsearch6;

import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchUpsertTableSinkBase;
import org.apache.flink.table.descriptors.DescriptorProperties;

import com.bytedance.ee.kms.KMSConfiguration;

import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ElasticsearchValidator.CONNECTOR_VERSION_VALUE_6_KMS;

/**
 * es6 kms table factory.
 */
public class Elasticsearch6KmsUpsertTableSinkFactory extends Elasticsearch6UpsertTableSinkFactory {
	private static final String CONNECTOR_KMS_PSM = "connector.kms-psm";
	private static final String CONNECTOR_KMS_APPROLE = "connector.kms-approle";
	private static final String CONNECTOR_KMS_SECRET = "connector.kms-secret";
	private static final String CONNECTOR_KMS_FILENAME = "connector.kms-filename";

	@Override
	protected String elasticsearchVersion() {
		return CONNECTOR_VERSION_VALUE_6_KMS;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = super.supportedProperties();
		properties.add(CONNECTOR_KMS_PSM);
		properties.add(CONNECTOR_KMS_APPROLE);
		properties.add(CONNECTOR_KMS_SECRET);
		properties.add(CONNECTOR_KMS_FILENAME);
		return properties;
	}

	@Override
	protected Map<ElasticsearchUpsertTableSinkBase.SinkOption, String> getSinkOptions(DescriptorProperties descriptorProperties) {
		Map<ElasticsearchUpsertTableSinkBase.SinkOption, String> sinkOptions = super.getSinkOptions(descriptorProperties);

		String kmsPsm = descriptorProperties.getString(CONNECTOR_KMS_PSM);
		String appRole = descriptorProperties.getString(CONNECTOR_KMS_APPROLE);
		String secret = descriptorProperties.getString(CONNECTOR_KMS_SECRET);
		String fileName = descriptorProperties.getString(CONNECTOR_KMS_FILENAME);

		Map<String, String> kmsConf = new KMSConfiguration(appRole, secret).getConfig(fileName, kmsPsm);
		sinkOptions.putIfAbsent(ElasticsearchUpsertTableSinkBase.SinkOption.ENABLE_PASSWORD_CONFIG, "true");
		sinkOptions.putIfAbsent(ElasticsearchUpsertTableSinkBase.SinkOption.USERNAME, kmsConf.get("username"));
		sinkOptions.putIfAbsent(ElasticsearchUpsertTableSinkBase.SinkOption.PASSWORD, kmsConf.get("password"));
		return sinkOptions;
	}
}
