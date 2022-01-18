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

package org.apache.flink.connector.bmq;

import org.apache.flink.connector.bmq.config.BmqSourceConfig;
import org.apache.flink.connector.bmq.config.Metadata;
import org.apache.flink.connector.bmq.generated.MetaRecord;
import org.apache.flink.core.fs.BlockLocation;
import org.apache.flink.core.fs.FileStatus;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.api.TableSchema;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;

import org.apache.avro.Schema;
import org.apache.avro.generic.IndexedRecord;
import org.apache.hadoop.conf.Configuration;
import org.apache.parquet.avro.AvroParquetWriter;
import org.apache.parquet.hadoop.ParquetWriter;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;

import static org.junit.Assert.assertEquals;

/**
 * Integration tests for {@link BmqFileInputFormat}.
 */
public class BmqFileInputFormatITCase {
	private static final String OFFSET = "offset_column";
	private static final String PARTITION = "partition_column";
	private static final String TIMESTAMP = "timestamp_column";
	private static final String NAME = "name";
	private static final String COUNT = "count";

	private static final TableSchema SOURCE_SCHEMA = TableSchema.builder()
		.field(NAME, DataTypes.STRING())
		.field(COUNT, DataTypes.DOUBLE())
		.build();

	private static final TableSchema SOURCE_META_WITH_OFFSET_SCHEMA = TableSchema.builder()
		.field(OFFSET, DataTypes.BIGINT())
		.field(PARTITION, DataTypes.INT())
		.field(TIMESTAMP, DataTypes.BIGINT())
		.field(NAME, DataTypes.STRING())
		.field(COUNT, DataTypes.DOUBLE())
		.build();

	private static final TableSchema SOURCE_META_WITHOUT_OFFSET_SCHEMA = TableSchema.builder()
		.field(PARTITION, DataTypes.INT())
		.field(TIMESTAMP, DataTypes.BIGINT())
		.field(NAME, DataTypes.STRING())
		.field(COUNT, DataTypes.DOUBLE())
		.build();

	public static final Schema META_SCHEMA = getTestSchema("meta.avsc");

	private static Path testPath;

	@ClassRule
	public static TemporaryFolder tempRoot = new TemporaryFolder();

	@BeforeClass
	public static void setup() throws Exception {
		testPath = createTestParquetFile(1000);
	}

	@Test
	public void testFullScan() throws Exception {
		int[] projectedFields = {0, 1};
		BmqSourceConfig bmqSourceConfig = createBmqSourceConfig();
		BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(SOURCE_SCHEMA, bmqSourceConfig, projectedFields, -1);

		{
			bmqFileInputFormat.open(createInputSplit());

			List<String> collected = new ArrayList<>();
			while (!bmqFileInputFormat.reachedEnd()) {
				RowData rowData;
				if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
					String name = rowData.getString(0).toString();
					double count = rowData.getDouble(1);
					collected.add(String.join(",", name, String.valueOf(count)));
				}
			}

			List<String> expected = new ArrayList<>();
			for (int i = 250; i < 750; i++) {
				expected.add(String.format("name,%.1f", (double) i));
			}
			assertEquals(expected, collected);
		}

		{
			bmqFileInputFormat.open(createInputSplit(250, 2000));

			List<String> collected = new ArrayList<>();
			while (!bmqFileInputFormat.reachedEnd()) {
				RowData rowData;
				if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
					String name = rowData.getString(0).toString();
					double count = rowData.getDouble(1);
					collected.add(String.join(",", name, String.valueOf(count)));
				}
			}

			List<String> expected = new ArrayList<>();
			for (int i = 250; i < 1000; i++) {
				expected.add(String.format("name,%.1f", (double) i));
			}
			assertEquals(expected, collected);
		}
	}

	@Test
	public void testLimitScan() throws Exception {
		int[] projectedFields = {0, 1};
		BmqSourceConfig bmqSourceConfig = createBmqSourceConfig();
		BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(SOURCE_SCHEMA, bmqSourceConfig, projectedFields, 100);
		bmqFileInputFormat.open(createInputSplit());

		List<String> collected = new ArrayList<>();
		while (!bmqFileInputFormat.reachedEnd()) {
			RowData rowData;
			if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
				String name = rowData.getString(0).toString();
				double count = rowData.getDouble(1);
				collected.add(String.format("%s,%.1f", name, count));
			}
		}

		List<String> expected = new ArrayList<>();
		for (int i = 250; i < 350; i++) {
			expected.add(String.format("name,%.1f", (double) i));
		}
		assertEquals(expected, collected);
	}

	@Test
	public void testProjectScan() throws Exception {
		int[] projectedFields = {1};
		BmqSourceConfig bmqSourceConfig = createBmqSourceConfig();
		BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(SOURCE_SCHEMA, bmqSourceConfig, projectedFields, -1);
		bmqFileInputFormat.open(createInputSplit());

		List<String> collected = new ArrayList<>();
		while (!bmqFileInputFormat.reachedEnd()) {
			RowData rowData;
			if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
				double count = rowData.getDouble(0);
				collected.add(String.valueOf(count));
			}
		}

		List<String> expected = new ArrayList<>();
		for (int i = 250; i < 750; i++) {
			expected.add(String.format("%.1f", (double) i));
		}
		assertEquals(expected, collected);
	}

	@Test
	public void testFullScanWithAllMeta() throws Exception {
		int[] projectedFields = {0, 1, 2, 3, 4};
		BmqSourceConfig bmqSourceConfig = createBmqSourceConfig(
			"__partition_id=partition_column,__timestamp=timestamp_column,__offset=offset_column",
			SOURCE_META_WITH_OFFSET_SCHEMA);
		BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(
			SOURCE_META_WITH_OFFSET_SCHEMA,
			bmqSourceConfig,
			projectedFields,
			100);
		bmqFileInputFormat.open(createInputSplit());

		List<String> collected = new ArrayList<>();
		while (!bmqFileInputFormat.reachedEnd()) {
			RowData rowData;
			if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
				long offset = rowData.getLong(0);
				int partition = rowData.getInt(1);
				long timestamp = rowData.getLong(2);
				String name = rowData.getString(3).toString();
				double count = rowData.getDouble(4);
				collected.add(String.format("%d,%d,%d,%s,%.1f", offset, partition, timestamp, name, count));
			}
		}

		List<String> expected = new ArrayList<>();
		for (int i = 250; i < 350; i++) {
			expected.add(String.format("%d,0,%d,name,%.1f", i, i, (double) i));
		}
		assertEquals(expected, collected);
	}

	@Test
	public void testFullScanWithMetaExceptOffset() throws Exception {
		int[] projectedFields = {0, 1, 2, 3};
		BmqSourceConfig bmqSourceConfig = createBmqSourceConfig(
			"__partition_id=partition_column,__timestamp=timestamp_column",
			SOURCE_META_WITHOUT_OFFSET_SCHEMA);
		BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(
			SOURCE_META_WITHOUT_OFFSET_SCHEMA,
			bmqSourceConfig,
			projectedFields,
			-1);
		bmqFileInputFormat.open(createInputSplit());

		List<String> collected = new ArrayList<>();
		while (!bmqFileInputFormat.reachedEnd()) {
			RowData rowData;
			if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
				int partition = rowData.getInt(0);
				long timestamp = rowData.getLong(1);
				String name = rowData.getString(2).toString();
				double count = rowData.getDouble(3);
				collected.add(String.format("%d,%d,%s,%.1f", partition, timestamp, name, count));
			}
		}

		List<String> expected = new ArrayList<>();
		for (int i = 250; i < 750; i++) {
			expected.add(String.format("0,%d,name,%.1f", i, (double) i));
		}
		assertEquals(expected, collected);
	}

	@Test
	public void testProjectionScanWithAllMeta() throws Exception {
		{
			int[] projectedFields = {3, 4};
			BmqSourceConfig bmqSourceConfig = createBmqSourceConfig(
				"__partition_id=partition_column,__timestamp=timestamp_column,__offset=offset_column",
				SOURCE_META_WITH_OFFSET_SCHEMA);
			BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(
				SOURCE_META_WITH_OFFSET_SCHEMA,
				bmqSourceConfig,
				projectedFields,
				100);
			bmqFileInputFormat.open(createInputSplit());

			List<String> collected = new ArrayList<>();
			while (!bmqFileInputFormat.reachedEnd()) {
				RowData rowData;
				if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
					String name = rowData.getString(0).toString();
					double count = rowData.getDouble(1);
					collected.add(String.join(",", name, String.valueOf(count)));
				}
			}

			List<String> expected = new ArrayList<>();
			for (int i = 250; i < 350; i++) {
				expected.add(String.format("name,%.1f", (double) i));
			}
			assertEquals(expected, collected);
		}

		{
			TableSchema sourceMetaWithOffsetSchema2 = TableSchema.builder()
				.field(PARTITION, DataTypes.INT())
				.field(TIMESTAMP, DataTypes.BIGINT())
				.field(NAME, DataTypes.STRING())
				.field(COUNT, DataTypes.DOUBLE())
				.field(OFFSET, DataTypes.BIGINT())
				.build();
			int[] projectedFields = {2, 3};
			BmqSourceConfig bmqSourceConfig = createBmqSourceConfig(
				"__partition_id=partition_column,__timestamp=timestamp_column,__offset=offset_column",
				sourceMetaWithOffsetSchema2);
			BmqFileInputFormat bmqFileInputFormat = createBmqFileInputFormat(
				sourceMetaWithOffsetSchema2,
				bmqSourceConfig,
				projectedFields,
				100);
			bmqFileInputFormat.open(createInputSplit());

			List<String> collected = new ArrayList<>();
			while (!bmqFileInputFormat.reachedEnd()) {
				RowData rowData;
				if ((rowData = bmqFileInputFormat.nextRecord(null)) != null) {
					String name = rowData.getString(0).toString();
					double count = rowData.getDouble(1);
					collected.add(String.join(",", name, String.valueOf(count)));
				}
			}

			List<String> expected = new ArrayList<>();
			for (int i = 250; i < 350; i++) {
				expected.add(String.format("name,%.1f", (double) i));
			}
			assertEquals(expected, collected);
		}
	}

	/**
	 * Create a test Parquet file with a given number of rows.
	 */
	private static Path createTestParquetFile(int numberOfRows) throws Exception {
		List<IndexedRecord> records = createRecordList(numberOfRows);
		Path path = createTempParquetFile(tempRoot.getRoot(), META_SCHEMA, records);
		return path;
	}

	private static List<IndexedRecord> createRecordList(long numberOfRows) {
		List<IndexedRecord> records = new ArrayList<>(0);
		for (long i = 0; i < numberOfRows; i++) {
			final MetaRecord metaRecord = MetaRecord.newBuilder()
				.setOffset$1(i)
				.setPartitionId$1(0)
				.setTimestamp$1(i)
				.setName("name")
				.setCount(new Long(i).doubleValue())
				.build();
			records.add(metaRecord);
		}
		return records;
	}

	private static Path createTempParquetFile(File folder, Schema schema, List<IndexedRecord> records) throws IOException {
		Path path = new Path(folder.getPath(), UUID.randomUUID().toString());
		ParquetWriter<IndexedRecord> writer = AvroParquetWriter.<IndexedRecord>builder(
			new org.apache.hadoop.fs.Path(path.toUri())).withSchema(schema).withRowGroupSize(10).build();

		for (IndexedRecord record : records) {
			writer.write(record);
		}

		writer.close();
		return path;
	}

	private static Schema getTestSchema(String schemaName) {
		try {
			InputStream inputStream = BmqFileInputFormatITCase.class.getClassLoader()
				.getResourceAsStream("avro/" + schemaName);
			return new Schema.Parser().parse(inputStream);
		} catch (IOException e) {
			throw new RuntimeException(e);
		}
	}

	private BmqFileInputSplit createInputSplit() throws Exception {
		return createInputSplit(250, 750);
	}

	private BmqFileInputSplit createInputSplit(int startOffset, int endOffset) throws Exception {
		FileSystem fs = testPath.getFileSystem();
		FileStatus pathFile = fs.getFileStatus(testPath);
		final BlockLocation[] blocks = fs.getFileBlockLocations(pathFile, 0, pathFile.getLen());
		Set<String> hosts = new HashSet<String>();
		for (BlockLocation hdfsBlock : blocks) {
			hosts.addAll(Arrays.asList(hdfsBlock.getHosts()));
		}

		return BmqFileInputSplit.builder()
			.setSplitNumber(0)
			.setPath(testPath)
			.setStart(0)
			.setLength(pathFile.getLen())
			.setHosts(hosts.toArray(new String[0]))
			.setStartOffset(0)
			.setExpectedStartOffset(startOffset)
			.setExpectedEndOffset(endOffset)
			.build();
	}

	private BmqFileInputFormat createBmqFileInputFormat(
			TableSchema tableSchema,
			BmqSourceConfig bmqSourceConfig,
			int[] projectedFields,
			int limit) {
		String topic = bmqSourceConfig.getTopic();
		String cluster = bmqSourceConfig.getCluster();
		long startMs = bmqSourceConfig.getScanStartTimeMs();
		long endMs = bmqSourceConfig.getScanEndTimeMs();
		Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap = bmqSourceConfig.getMetadataMap();

		return BmqFileInputFormat.builder()
			.setCluster(cluster)
			.setTopic(topic)
			.setStartMs(startMs)
			.setEndMs(endMs)
			.setFullFieldNames(tableSchema.getFieldNames())
			.setFullFieldTypes(tableSchema.getFieldDataTypes())
			.setSelectedFields(projectedFields)
			.setMetadataMap(metadataMap)
			.setLimit(limit)
			.setConf(new Configuration())
			.setUtcTimeStamp(true)
			.build();
	}

	private BmqSourceConfig createBmqSourceConfig() {
		return createBmqSourceConfig(null, null);
	}

	private BmqSourceConfig createBmqSourceConfig(String metaInfo, TableSchema tableSchema) {
		String dummyCluster = "dummy";
		String dummyTopic = "dummy";
		long startMs = -1;
		long endMs = -1;

		BmqSourceConfig bmqSourceConfig = new BmqSourceConfig();
		bmqSourceConfig.setTopic(dummyTopic);
		bmqSourceConfig.setCluster(dummyCluster);
		bmqSourceConfig.setScanStartTimeMs(startMs);
		bmqSourceConfig.setScanEndTimeMs(endMs);

		if (metaInfo != null) {
			DynamicSourceMetadataFactory factory = new DynamicSourceMetadataFactory() {
				@Override
				protected DynamicSourceMetadata findMetadata(String name) {
					return Metadata.findByName(name);
				}

				@Override
				protected String getMetadataValues() {
					return Metadata.getValuesString();
				}
			};
			bmqSourceConfig.setMetadataMap(factory.parseWithSchema(metaInfo, tableSchema));
		}

		return bmqSourceConfig;
	}
}
