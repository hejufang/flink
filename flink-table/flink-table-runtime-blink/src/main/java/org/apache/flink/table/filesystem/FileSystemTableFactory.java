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

package org.apache.flink.table.filesystem;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableSinkFactory;
import org.apache.flink.table.factories.DynamicTableSourceFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.TableFactory;

import java.util.HashSet;
import java.util.Set;

import static org.apache.flink.table.filesystem.FileSystemOptions.COMPRESS_CODEC;

/**
 * File system {@link TableFactory}.
 *
 * <p>1.The partition information should be in the file system path, whether it's a temporary
 * table or a catalog table.
 * 2.Support insert into (append) and insert overwrite.
 * 3.Support static and dynamic partition inserting.
 */
public class FileSystemTableFactory implements
		DynamicTableSourceFactory,
		DynamicTableSinkFactory {

	public static final String IDENTIFIER = "filesystem";

	@Override
	public String factoryIdentifier() {
		return IDENTIFIER;
	}

	@Override
	public DynamicTableSink createDynamicTableSink(Context context) {
		validate(FactoryUtil.createTableFactoryHelper(this, context));
		return new FileSystemTableSink(context);
	}

	@Override
	public DynamicTableSource createDynamicTableSource(Context context) {
		FactoryUtil.TableFactoryHelper helper = FactoryUtil.createTableFactoryHelper(this, context);
		validate(helper);
		final ReadableConfig config = helper.getOptions();
		DecodingFormat<DeserializationSchema<RowData>> decodingFormat = null;
		if (config.getOptional(COMPRESS_CODEC).isPresent()) {
			decodingFormat = helper.discoverDecodingFormat(
				DeserializationFormatFactory.class,
				FactoryUtil.FORMAT);
		}
		return new FileSystemTableSource(context, decodingFormat);
	}

	private void validate(FactoryUtil.TableFactoryHelper helper) {
		// Except format options, some formats like parquet and orc can not list all supported options.
		helper.validateExcept(helper.getOptions().get(FactoryUtil.FORMAT) + ".");
	}

	@Override
	public Set<ConfigOption<?>> requiredOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FileSystemOptions.PATH);
		options.add(FactoryUtil.FORMAT);
		return options;
	}

	@Override
	public Set<ConfigOption<?>> optionalOptions() {
		Set<ConfigOption<?>> options = new HashSet<>();
		options.add(FileSystemOptions.PARTITION_DEFAULT_NAME);
		options.add(FileSystemOptions.SINK_ROLLING_POLICY_FILE_SIZE);
		options.add(FileSystemOptions.SINK_ROLLING_POLICY_ROLLOVER_INTERVAL);
		options.add(FileSystemOptions.SINK_ROLLING_POLICY_CHECK_INTERVAL);
		options.add(FileSystemOptions.SINK_SHUFFLE_BY_PARTITION);
		options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_KIND);
		options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_CLASS);
		options.add(FileSystemOptions.PARTITION_TIME_EXTRACTOR_TIMESTAMP_PATTERN);
		options.add(FileSystemOptions.SINK_PARTITION_COMMIT_TRIGGER);
		options.add(FileSystemOptions.SINK_PARTITION_COMMIT_DELAY);
		options.add(FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_KIND);
		options.add(FileSystemOptions.SINK_PARTITION_COMMIT_POLICY_CLASS);
		options.add(FileSystemOptions.SINK_PARTITION_COMMIT_SUCCESS_FILE_NAME);
		options.add(COMPRESS_CODEC);
		options.add(FileSystemOptions.ENCODE_AS_CHANGELOG);
		options.add(FileSystemOptions.CHANGELOG_COLUMN_NAME);
		return options;
	}
}
