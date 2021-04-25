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

package org.apache.flink.connector.doris;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.descriptors.DescriptorProperties;
import org.apache.flink.table.descriptors.DorisValidator;
import org.apache.flink.table.factories.StreamTableSinkFactory;
import org.apache.flink.table.sinks.StreamTableSink;
import org.apache.flink.types.Row;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.inf.compute.hsap.doris.DataFormat;
import com.bytedance.inf.compute.hsap.doris.TableModel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.apache.flink.table.descriptors.ConnectorDescriptorValidator.CONNECTOR_TYPE;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_CLUSTER;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_COLUMN_SEPARATOR;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_DATA_CENTER;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_DATA_FORMAT;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_DB_NAME;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_DORIS_FE_LIST;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_DORIS_FE_PSM;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_FE_UPDATE_INTERVAL_MS;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_FIELD_MAPPING;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_KEYS;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_MAX_BYTES_PER_BATCH;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_MAX_FILTER_RATIO;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_MAX_PENDING_BATCH_NUM;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_MAX_PENDING_TIME_MS;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_MAX_RETRY_NUM;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_PARALLELISM;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_PASSWORD;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_RETRY_INTERVAL_MS;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_SEQUENCE_COLUMN;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_TABLE_MODEL;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_TABLE_NAME;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_TIMEOUT_MS;
import static org.apache.flink.table.descriptors.DorisValidator.CONNECTOR_USER;
import static org.apache.flink.table.descriptors.DorisValidator.DORIS;
import static org.apache.flink.table.descriptors.Schema.SCHEMA;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_NAME;
import static org.apache.flink.table.descriptors.Schema.SCHEMA_TYPE;

/**
 * Factory for creating configured instances of {@link DorisUpsertTableSink}.
 */
public class DorisTableFactory implements StreamTableSinkFactory<Tuple2<Boolean, Row>> {

	@Override
	public Map<String, String> requiredContext() {
		Map<String, String> context = new HashMap<>();
		context.put(CONNECTOR_TYPE, DORIS);
		return context;
	}

	@Override
	public List<String> supportedProperties() {
		List<String> properties = new ArrayList<>();

		properties.add(CONNECTOR_DORIS_FE_LIST);
		properties.add(CONNECTOR_CLUSTER);
		properties.add(CONNECTOR_DATA_CENTER);
		properties.add(CONNECTOR_DORIS_FE_PSM);
		properties.add(CONNECTOR_USER);
		properties.add(CONNECTOR_PASSWORD);
		properties.add(CONNECTOR_DB_NAME);
		properties.add(CONNECTOR_TABLE_NAME);
		properties.add(CONNECTOR_KEYS);
		properties.add(CONNECTOR_TABLE_MODEL);
		properties.add(CONNECTOR_DATA_FORMAT);
		properties.add(CONNECTOR_COLUMN_SEPARATOR);
		properties.add(CONNECTOR_MAX_BYTES_PER_BATCH);
		properties.add(CONNECTOR_MAX_PENDING_BATCH_NUM);
		properties.add(CONNECTOR_MAX_PENDING_TIME_MS);
		properties.add(CONNECTOR_MAX_FILTER_RATIO);
		properties.add(CONNECTOR_RETRY_INTERVAL_MS);
		properties.add(CONNECTOR_MAX_RETRY_NUM);
		properties.add(CONNECTOR_FE_UPDATE_INTERVAL_MS);
		properties.add(CONNECTOR_PARALLELISM);
		properties.add(CONNECTOR_SEQUENCE_COLUMN);
		properties.add(CONNECTOR_TIMEOUT_MS);
		properties.add(CONNECTOR_FIELD_MAPPING);

		// schema
		properties.add(SCHEMA + ".#." + SCHEMA_TYPE);
		properties.add(SCHEMA + ".#." + SCHEMA_NAME);

		return properties;
	}

	@Override
	public StreamTableSink<Tuple2<Boolean, Row>> createStreamTableSink(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = getValidatedProperties(properties);
		DorisOptions dorisOptions = getDorisOptions(descriptorProperties);
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		return new DorisUpsertTableSink(tableSchema, dorisOptions);
	}

	private DescriptorProperties getValidatedProperties(Map<String, String> properties) {
		final DescriptorProperties descriptorProperties = new DescriptorProperties(true);
		descriptorProperties.putProperties(properties);
		validate(descriptorProperties);
		return descriptorProperties;
	}

	private void validate(DescriptorProperties descriptorProperties) {
		new DorisValidator().validate(descriptorProperties);
	}

	private DorisOptions getDorisOptions(DescriptorProperties descriptorProperties) {
		DorisOptions.Builder dorisOptionsBuilder = DorisOptions.builder();

		// convert string to List<Pair<String, Integer>>.
		List<Pair<String, Integer>> dorisFeList = getDorisFrontendList(descriptorProperties);
		dorisOptionsBuilder.setDorisFEList(dorisFeList);
		// get columns from tableSchema.
		TableSchema tableSchema = descriptorProperties.getTableSchema(SCHEMA);
		dorisOptionsBuilder.setColumns(tableSchema.getFieldNames());
		// convert String to String[].
		dorisOptionsBuilder.setKeys(descriptorProperties.getString(CONNECTOR_KEYS).split(","));
		// convert String to TableModel.
		TableModel tableModel = TableModel.parseTableModel(
			descriptorProperties.getOptionalString(CONNECTOR_TABLE_MODEL).orElse("aggregate"));
		dorisOptionsBuilder.setTableModel(tableModel);
		// convert String to DataFormat.
		DataFormat dataFormat = DataFormat.parseFormat(
			descriptorProperties.getOptionalString(CONNECTOR_DATA_FORMAT).orElse("csv"));
		dorisOptionsBuilder.setDataFormat(dataFormat);

		// set other required params.
		dorisOptionsBuilder.setCluster(descriptorProperties.getString(CONNECTOR_CLUSTER));
		dorisOptionsBuilder.setUser(descriptorProperties.getString(CONNECTOR_USER));
		dorisOptionsBuilder.setPassword(descriptorProperties.getString(CONNECTOR_PASSWORD));
		dorisOptionsBuilder.setDbname(descriptorProperties.getString(CONNECTOR_DB_NAME));
		dorisOptionsBuilder.setTableName(descriptorProperties.getString(CONNECTOR_TABLE_NAME));

		// set other optional params.
		descriptorProperties.getOptionalString(CONNECTOR_DATA_CENTER)
			.ifPresent(dorisOptionsBuilder::setDataCenter);
		descriptorProperties.getOptionalString(CONNECTOR_DORIS_FE_PSM)
			.ifPresent(dorisOptionsBuilder::setDorisFEPsm);
		descriptorProperties.getOptionalString(CONNECTOR_COLUMN_SEPARATOR)
			.ifPresent(dorisOptionsBuilder::setColumnSeparator);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_BYTES_PER_BATCH)
			.ifPresent(dorisOptionsBuilder::setMaxBytesPerBatch);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_PENDING_BATCH_NUM)
			.ifPresent(dorisOptionsBuilder::setMaxPendingBatchNum);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_PENDING_TIME_MS)
			.ifPresent(dorisOptionsBuilder::setMaxPendingTimeMs);
		descriptorProperties.getOptionalFloat(CONNECTOR_MAX_FILTER_RATIO)
			.ifPresent(dorisOptionsBuilder::setMaxFilterRatio);
		descriptorProperties.getOptionalInt(CONNECTOR_RETRY_INTERVAL_MS)
			.ifPresent(dorisOptionsBuilder::setRetryIntervalMs);
		descriptorProperties.getOptionalInt(CONNECTOR_MAX_RETRY_NUM)
			.ifPresent(dorisOptionsBuilder::setMaxRetryNum);
		descriptorProperties.getOptionalInt(CONNECTOR_FE_UPDATE_INTERVAL_MS)
			.ifPresent(dorisOptionsBuilder::setFeUpdateIntervalMs);
		descriptorProperties.getOptionalInt(CONNECTOR_PARALLELISM)
			.ifPresent(dorisOptionsBuilder::setParallelism);
		descriptorProperties.getOptionalString(CONNECTOR_SEQUENCE_COLUMN)
			.ifPresent(dorisOptionsBuilder::setSequenceColumn);
		descriptorProperties.getOptionalInt(CONNECTOR_TIMEOUT_MS)
			.ifPresent(dorisOptionsBuilder::setTimeoutMs);
		descriptorProperties.getOptionalString(CONNECTOR_FIELD_MAPPING)
			.ifPresent(dorisOptionsBuilder::setFieldMapping);

		return dorisOptionsBuilder.build();
	}

	/**
	 * A typical example of input is: "10.196.81.207:8030,10.196.81.224:8030".
	 */
	private List<Pair<String, Integer>> getDorisFrontendList(DescriptorProperties descriptorProperties) {
		String dorisFesAsString = descriptorProperties.getOptionalString(CONNECTOR_DORIS_FE_LIST).orElse(null);
		if (dorisFesAsString == null) {
			return null;
		}
		List<Pair<String, Integer>> dorisFeList = new ArrayList<>();
		try {
			String[] dorisFes = dorisFesAsString.split(",");
			for (String dorisFe : dorisFes) {
				String[] dorisFeAddress = dorisFe.split(":");
				String host = dorisFeAddress[0];
				String port = dorisFeAddress[1];
				dorisFeList.add(new ImmutablePair<>(host, Integer.parseInt(port)));
			}
		} catch (Exception e) {
			throw new FlinkRuntimeException(String.format("get doris FE list failed, "
				+ "the data causes error is '%s'.", dorisFesAsString));
		}
		return dorisFeList;
	}
}
