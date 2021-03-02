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

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.FactoryUtil.TableFactoryHelper;
import org.apache.flink.util.FlinkRuntimeException;

import com.bytedance.inf.compute.hsap.doris.DataFormat;
import com.bytedance.inf.compute.hsap.doris.TableModel;
import org.apache.commons.lang3.tuple.ImmutablePair;
import org.apache.commons.lang3.tuple.Pair;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.createTableFactoryHelper;

/**
 * Doris connector factory.
 */
public class DorisDynamicTableFactory implements DynamicTableSinkFactory {

	private static final String IDENTIFIER = "doris";

	private static final ConfigOption<String> DORIS_FE_LIST = ConfigOptions
		.key("doris-fe-list")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines the address of FEs.");

	private static final ConfigOption<String> CLUSTER = ConfigOptions
		.key("cluster")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines the doris cluster.");

	private static final ConfigOption<String> DATA_CENTER = ConfigOptions
		.key("data-center")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines doris data center.");

	private static final ConfigOption<String> DORIS_FE_PSM = ConfigOptions
		.key("doris-fe-psm")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines psm of doris FE, used to get FE list through consul.");

	private static final ConfigOption<String> USER = ConfigOptions
		.key("user")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines user of target cluster.");

	private static final ConfigOption<String> PASSWORD = ConfigOptions
		.key("password")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines password of target cluster.");

	private static final ConfigOption<String> DB_NAME = ConfigOptions
		.key("db-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines DB name which we want to access.");

	private static final ConfigOption<String> TABLE_NAME = ConfigOptions
		.key("table-name")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines table name which we want to access.");

	private static final ConfigOption<String> KEYS = ConfigOptions
		.key("keys")
		.stringType()
		.noDefaultValue()
		.withDescription("Required. It defines the keys of table, and is seperated by ','.");

	private static final ConfigOption<String> TABLE_MODEL = ConfigOptions
		.key("table-model")
		.stringType()
		.defaultValue("aggregate")
		.withDescription("Optional. It defines the type of table model, which can be "
			+ "'aggregate', 'uniq' or 'duplicate'.");

	private static final ConfigOption<String> DATA_FORMAT = ConfigOptions
		.key("data-format")
		.stringType()
		.defaultValue("csv")
		.withDescription("Optional. It defines the data format that would be written into doris. "
			+ "It can be csv or json.");

	private static final ConfigOption<String> COLUMN_SEPARATOR = ConfigOptions
		.key("column-separator")
		.stringType()
		.defaultValue("\\x01")
		.withDescription("Optional. It defines separator for csv format.");

	private static final ConfigOption<String> SEQUENCE_COLUMN = ConfigOptions
		.key("sequence-column")
		.stringType()
		.noDefaultValue()
		.withDescription("Optional. It defines sequence-column config in doris client.");

	private static final ConfigOption<Integer> MAX_BYTES_PER_BATCH = ConfigOptions
		.key("max-bytes-per-batch")
		.intType()
		.defaultValue(104857600)
		.withDescription("Optional. It defines the maximum data volume per batch.");

	private static final ConfigOption<Integer> MAX_PENDING_BATCH_NUM = ConfigOptions
		.key("max-pending-batch-num")
		.intType()
		.defaultValue(3)
		.withDescription("Optional. It defines the maximum amount of batch which is waiting to send.");

	private static final ConfigOption<Integer> MAX_PENDING_TIME_MS = ConfigOptions
		.key("max-pending-time-ms")
		.intType()
		.defaultValue(300000)
		.withDescription("Optional. It defines maximum time that an existing batch can wait to be sent, "
			+ "if a batch has been waited for this duration, it will be sent anyway.");

	private static final ConfigOption<Float> MAX_FILTER_RATIO = ConfigOptions
		.key("max-filter-ratio")
		.floatType()
		.defaultValue(0.0f)
		.withDescription("Optional. It defines the maximum failure rate of data writing that we can bear.");

	private static final ConfigOption<Integer> RETRY_INTERVAL_MS = ConfigOptions
		.key("retry-interval-ms")
		.intType()
		.defaultValue(1000)
		.withDescription("Optional. It defines the interval of retrying.");

	private static final ConfigOption<Integer> MAX_RETRY_NUM = ConfigOptions
		.key("max_retry_num")
		.intType()
		.defaultValue(-1)
		.withDescription("Optional. It defines max retry times and -1 means unlimited.");

	private static final ConfigOption<Integer> FE_UPDATE_INTERVAL_MS = ConfigOptions
		.key("fe_update_interval_ms")
		.intType()
		.defaultValue(15000)
		.withDescription("Optional. It defines the interval of updating FE list "
			+ "when we use doris-fe-psm to get FE list.");

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		TableFactoryHelper helper = createTableFactoryHelper(this, context);
		helper.validate();

		TableSchema tableSchema = context.getCatalogTable().getSchema();

		return new DorisDynamicTableSink(tableSchema, getDorisOptions(helper.getOptions(), tableSchema));
	}

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(CLUSTER);
		set.add(USER);
		set.add(PASSWORD);
		set.add(DB_NAME);
		set.add(TABLE_NAME);
		set.add(KEYS);
		return set;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> set = new HashSet<>();
		set.add(DORIS_FE_LIST);
		set.add(DORIS_FE_PSM);
		set.add(DATA_CENTER);
		set.add(TABLE_MODEL);
		set.add(DATA_FORMAT);
		set.add(COLUMN_SEPARATOR);
		set.add(MAX_BYTES_PER_BATCH);
		set.add(MAX_PENDING_BATCH_NUM);
		set.add(MAX_PENDING_TIME_MS);
		set.add(MAX_FILTER_RATIO);
		set.add(RETRY_INTERVAL_MS);
		set.add(MAX_RETRY_NUM);
		set.add(FE_UPDATE_INTERVAL_MS);
		set.add(PARALLELISM);
		set.add(SEQUENCE_COLUMN);
		return set;
	}

	private DorisOptions getDorisOptions(ReadableConfig readableConfig, TableSchema tableSchema) {
		DorisOptions.Builder dorisOptions = DorisOptions.builder();

		// convert string to List<Pair<String, Integer>> .
		dorisOptions.setDorisFEList(getDorisFeList(readableConfig.get(DORIS_FE_LIST)));
		dorisOptions.setDorisFEPsm(readableConfig.get(DORIS_FE_PSM));
		dorisOptions.setCluster(readableConfig.get(CLUSTER));
		dorisOptions.setDataCenter(readableConfig.get(DATA_CENTER));
		dorisOptions.setUser(readableConfig.get(USER));
		dorisOptions.setPassword(readableConfig.get(PASSWORD));
		dorisOptions.setDbname(readableConfig.get(DB_NAME));
		dorisOptions.setTableName(readableConfig.get(TABLE_NAME));
		// get columns from tableSchema.
		dorisOptions.setColumns(tableSchema.getFieldNames());
		// convert String to String[].
		dorisOptions.setKeys(readableConfig.get(KEYS).split(","));
		// convert String to TableModel.
		dorisOptions.setTableModel(TableModel.parseTableModel(readableConfig.get(TABLE_MODEL)));
		// convert String to DataFormat.
		dorisOptions.setDataFormat(DataFormat.parseFormat(readableConfig.get(DATA_FORMAT)));
		dorisOptions.setColumnSeparator(readableConfig.get(COLUMN_SEPARATOR));
		dorisOptions.setMaxBytesPerBatch(readableConfig.get(MAX_BYTES_PER_BATCH));
		dorisOptions.setMaxPendingBatchNum(readableConfig.get(MAX_PENDING_BATCH_NUM));
		dorisOptions.setMaxPendingTimeMs(readableConfig.get(MAX_PENDING_TIME_MS));
		dorisOptions.setMaxFilterRatio(readableConfig.get(MAX_FILTER_RATIO));
		dorisOptions.setRetryIntervalMs(readableConfig.get(RETRY_INTERVAL_MS));
		dorisOptions.setMaxRetryNum(readableConfig.get(MAX_RETRY_NUM));
		dorisOptions.setFeUpdateIntervalMs(readableConfig.get(FE_UPDATE_INTERVAL_MS));
		dorisOptions.setParallelism(readableConfig.get(PARALLELISM));
		readableConfig.getOptional(SEQUENCE_COLUMN).ifPresent(dorisOptions::setSequenceColumn);

		return dorisOptions.build();
	}

	/**
	 * A typical example of input is: "10.196.81.207:8030,10.196.81.224:8030".
	 */
	private List<Pair<String, Integer>> getDorisFeList(String dorisFesAsString) {
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
