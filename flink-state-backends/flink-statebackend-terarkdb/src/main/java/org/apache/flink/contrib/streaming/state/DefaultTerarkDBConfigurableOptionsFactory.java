/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.contrib.streaming.state;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.MemorySize;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.util.Preconditions;

import org.terarkdb.ColumnFamilyOptions;
import org.terarkdb.DBOptions;
import org.terarkdb.ReadOptions;
import org.terarkdb.WriteOptions;

import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

/**
 * An implementation of {@link TerarkDBConfigurableOptions} using options provided by {@link TerarkDBConfigurableOptions}.
 * It acts as the default options factory within {@link TerarkDBStateBackend} if the user did not define a {@link RocksDBOptionsFactory}.
 */
public class DefaultTerarkDBConfigurableOptionsFactory implements ConfigurableRocksDBOptionsFactory {

	private final DefaultConfigurableOptionsFactory defaultConfigurableOptionsFactory;

	private final Map<String, String> configuredOptions = new HashMap<>();

	public DefaultTerarkDBConfigurableOptionsFactory(DefaultConfigurableOptionsFactory defaultConfigurableOptionsFactory) {
		this.defaultConfigurableOptionsFactory = defaultConfigurableOptionsFactory;
	}

	private static final ConfigOption<?>[] TERARKDB_CANDIDATE_CONFIGS = new ConfigOption<?>[] {
			TerarkDBConfigurableOptions.MAX_SUB_COMPACTIONS,
			TerarkDBConfigurableOptions.BLOB_SIZE,
			TerarkDBConfigurableOptions.BLOB_FILE_SIZE
	};

	private static final Set<ConfigOption<?>> TERARKDB_POSITIVE_INT_CONFIG_SET = new HashSet<>();

	private static final Set<ConfigOption<?>> TERARKDB_SIZE_CONFIG_SET = new HashSet<>(Arrays.asList(
			TerarkDBConfigurableOptions.BLOB_SIZE,
			TerarkDBConfigurableOptions.BLOB_FILE_SIZE
	));

	@Override
	public DefaultTerarkDBConfigurableOptionsFactory configure(ReadableConfig configuration) {
		this.defaultConfigurableOptionsFactory.configure(configuration);
		for (ConfigOption<?> option : TERARKDB_CANDIDATE_CONFIGS) {
			Optional<?> newValue = configuration.getOptional(option);

			if (newValue.isPresent()) {
				checkArgumentValid(option, newValue.get());
				setInternal(option.key(), newValue.get().toString());
			}
		}
		return this;
	}

	@Override
	public ColumnFamilyOptions createColumnOptions(ColumnFamilyOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		ColumnFamilyOptions columnFamilyOptions = defaultConfigurableOptionsFactory.createColumnOptions(currentOptions, handlesToClose);

		if (isOptionConfigured(TerarkDBConfigurableOptions.BLOB_SIZE)) {
			columnFamilyOptions.setBlobSize(getBlobSize());
		}

		if (isOptionConfigured(TerarkDBConfigurableOptions.BLOB_FILE_SIZE)) {
			columnFamilyOptions.setTargetBlobFileSize(getBlobFileSize());
		}

		if (isOptionConfigured(TerarkDBConfigurableOptions.MAX_SUB_COMPACTIONS)) {
			columnFamilyOptions.setMaxSubcompactions(getMaxSubCompactions());
		} else {
			columnFamilyOptions.setMaxSubcompactions(1);
		}

		return columnFamilyOptions;
	}

	@Override
	public DBOptions createDBOptions(DBOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		DBOptions dbOptions = defaultConfigurableOptionsFactory.createDBOptions(currentOptions, handlesToClose);
		dbOptions.setCheckPointFakeFlush(false);
		return dbOptions;
	}

	@Override
	public RocksDBNativeMetricOptions createNativeMetricsOptions(RocksDBNativeMetricOptions nativeMetricOptions) {
		return defaultConfigurableOptionsFactory.createNativeMetricsOptions(nativeMetricOptions);
	}

	@Override
	public WriteOptions createWriteOptions(WriteOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		return defaultConfigurableOptionsFactory.createWriteOptions(currentOptions, handlesToClose);
	}

	@Override
	public ReadOptions createReadOptions(ReadOptions currentOptions, Collection<AutoCloseable> handlesToClose) {
		return defaultConfigurableOptionsFactory.createReadOptions(currentOptions, handlesToClose);
	}

	// --------------------------------------------------------------------------
	// TerarkDB CF config
	// --------------------------------------------------------------------------

	private int getMaxSubCompactions() {
		return Math.max(Integer.parseInt(getInternal(TerarkDBConfigurableOptions.MAX_SUB_COMPACTIONS.key())), 1);
	}

	public DefaultTerarkDBConfigurableOptionsFactory setMaxSubCompactions(int maxSubCompactions) {
		setInternal(TerarkDBConfigurableOptions.MAX_SUB_COMPACTIONS.key(), Integer.toString(Math.max(maxSubCompactions, 1)));
		return this;
	}

	private long getBlobSize() {
		return MemorySize.parseBytes(getInternal(TerarkDBConfigurableOptions.BLOB_SIZE.key()));
	}

	public DefaultTerarkDBConfigurableOptionsFactory setBlobSize(String blobSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blobSize) >= 0,
				"Invalid configuration " + blobSize + " for blob size.");
		setInternal(TerarkDBConfigurableOptions.BLOB_SIZE.key(), blobSize);

		return this;
	}

	private long getBlobFileSize() {
		return MemorySize.parseBytes(getInternal(TerarkDBConfigurableOptions.BLOB_FILE_SIZE.key()));
	}

	public DefaultTerarkDBConfigurableOptionsFactory setBlobFileSize(String blobFileSize) {
		Preconditions.checkArgument(MemorySize.parseBytes(blobFileSize) >= 0,
				"Invalid configuration " + blobFileSize + " for blob file size.");
		setInternal(TerarkDBConfigurableOptions.BLOB_FILE_SIZE.key(), blobFileSize);

		return this;
	}

	private boolean isOptionConfigured(ConfigOption<?> configOption) {
		return configuredOptions.containsKey(configOption.key());
	}

	/**
	 * Helper method to check whether the (key,value) is valid through given configuration and returns the formatted value.
	 *
	 * @param option The configuration key which is configurable in {@link TerarkDBConfigurableOptions}.
	 * @param value The value within given configuration.
	 */
	private static void checkArgumentValid(ConfigOption<?> option, Object value) {
		final String key = option.key();

		if (TERARKDB_POSITIVE_INT_CONFIG_SET.contains(option)) {
			Preconditions.checkArgument((Integer) value  > 0,
					"Configured value for key: " + key + " must be larger than 0.");
		} else if (TERARKDB_SIZE_CONFIG_SET.contains(option)) {
			Preconditions.checkArgument(((MemorySize) value).getBytes() > 0,
					"Configured size for key" + key + " must be larger than 0.");
		}
	}

	/**
	 * Sets the configuration with (key, value) if the key is predefined, otherwise throws IllegalArgumentException.
	 *
	 * @param key The configuration key, if key is not predefined, throws IllegalArgumentException out.
	 * @param value The configuration value.
	 */
	private void setInternal(String key, String value) {
		Preconditions.checkArgument(value != null && !value.isEmpty(),
				"The configuration value must not be empty.");

		configuredOptions.put(key, value);
	}

	/**
	 * Returns the value in string format with the given key.
	 *
	 * @param key The configuration-key to query in string format.
	 */
	private String getInternal(String key) {
		Preconditions.checkArgument(configuredOptions.containsKey(key),
				"The configuration " + key + " has not been configured.");

		return configuredOptions.get(key);
	}
}
