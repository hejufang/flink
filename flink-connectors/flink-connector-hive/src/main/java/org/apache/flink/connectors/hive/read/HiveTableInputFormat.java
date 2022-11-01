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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.annotation.VisibleForTesting;
import org.apache.flink.api.common.io.CheckpointableInputFormat;
import org.apache.flink.api.common.io.LocatableInputSplitAssigner;
import org.apache.flink.api.common.io.statistics.BaseStatistics;
import org.apache.flink.api.java.hadoop.common.HadoopInputFormatCommonBase;
import org.apache.flink.api.java.tuple.Tuple3;
import org.apache.flink.connectors.hive.FlinkHiveException;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.hive.client.HiveShimLoader;
import org.apache.flink.table.catalog.hive.util.HiveTypeUtil;
import org.apache.flink.table.data.GenericRowData;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.LogicalType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.ql.io.IOConstants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.security.Credentials;
import org.apache.hadoop.security.UserGroupInformation;
import org.apache.hadoop.util.ReflectionUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import static org.apache.flink.util.Preconditions.checkNotNull;
import static org.apache.hadoop.mapreduce.lib.input.FileInputFormat.INPUT_DIR;

/**
 * The HiveTableInputFormat are inspired by the HCatInputFormat and HadoopInputFormatBase.
 * It's used to read from hive partition/non-partition table.
 */
public class HiveTableInputFormat extends HadoopInputFormatCommonBase<RowData, HiveTableInputSplit>
		implements CheckpointableInputFormat<HiveTableInputSplit, Long> {

	private static final long serialVersionUID = 3L;

	private static final Logger LOG = LoggerFactory.getLogger(HiveTableInputFormat.class);

	// schema evolution configs are not available in older versions of IOConstants, let's define them ourselves
	private static final String SCHEMA_EVOLUTION_COLUMNS = "schema.evolution.columns";
	private static final String SCHEMA_EVOLUTION_COLUMNS_TYPES = "schema.evolution.columns.types";

	private JobConf jobConf;

	private String hiveVersion;

	private List<String> partitionKeys;

	private DataType[] fieldTypes;

	private String[] fieldNames;

	//For non-partition hive table, partitions only contains one partition which partitionValues is empty.
	private List<HiveTablePartition> partitions;

	// indices of fields to be returned, with projection applied (if any)
	private int[] selectedFields;

	//We should limit the input read count of this splits, -1 represents no limit.
	private long limit;

	private boolean useMapRedReader;

	private transient long currentReadCount = 0L;

	// Total processed record number of this input format.
	// Caution: This field may be reset to 0 when task failover.
	private transient long totalReadCount = 0L;

	private boolean createSplitInParallel;
	private boolean useFastGetSplits;

	@VisibleForTesting
	protected transient SplitReader reader;

	public HiveTableInputFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields,
			long limit,
			String hiveVersion,
			boolean useMapRedReader,
			boolean createSplitInParallel,
			boolean useFastGetSplits) {
		super(jobConf.getCredentials());
		this.partitionKeys = catalogTable.getPartitionKeys();
		this.fieldTypes = catalogTable.getSchema().getFieldDataTypes();
		this.fieldNames = catalogTable.getSchema().getFieldNames();
		this.limit = limit;
		this.hiveVersion = hiveVersion;
		checkNotNull(catalogTable, "catalogTable can not be null.");
		this.partitions = checkNotNull(partitions, "partitions can not be null.");
		this.jobConf = new JobConf(jobConf);
		int rowArity = catalogTable.getSchema().getFieldCount();
		selectedFields = projectedFields != null ? projectedFields : IntStream.range(0, rowArity).toArray();
		this.useMapRedReader = useMapRedReader;
		this.createSplitInParallel = createSplitInParallel;
		this.useFastGetSplits = useFastGetSplits;
		LOG.info("Create split in parallel: {}, useFastGetSplits {} ",
			createSplitInParallel, useFastGetSplits);
	}

	public JobConf getJobConf() {
		return jobConf;
	}

	@Override
	public void configure(org.apache.flink.configuration.Configuration parameters) {
	}

	@Override
	public void open(HiveTableInputSplit split) throws IOException {
		HiveTablePartition partition = split.getHiveTablePartition();
		if (!useMapRedReader && useOrcVectorizedRead(partition)) {
			this.reader = new HiveVectorizedOrcSplitReader(
					hiveVersion, jobConf, fieldNames, fieldTypes, selectedFields, split);
		} else if (!useMapRedReader && useParquetVectorizedRead(partition)) {
			this.reader = new HiveVectorizedParquetSplitReader(
					hiveVersion, jobConf, fieldNames, fieldTypes, selectedFields, split);
		} else {
			JobConf clonedConf = new JobConf(jobConf);
			addSchemaToConf(clonedConf);
			this.reader = new HiveMapredSplitReader(clonedConf, partitionKeys, fieldTypes, selectedFields, split,
					HiveShimLoader.loadHiveShim(hiveVersion));
		}
		currentReadCount = 0L;
	}

	// Hive readers may rely on the schema info in configuration
	private void addSchemaToConf(JobConf jobConf) {
		// set columns/types -- including partition cols
		List<String> typeStrs = Arrays.stream(fieldTypes)
				.map(t -> HiveTypeUtil.toHiveTypeInfo(t, true).toString())
				.collect(Collectors.toList());
		jobConf.set(IOConstants.COLUMNS, String.join(",", fieldNames));
		jobConf.set(IOConstants.COLUMNS_TYPES, String.join(",", typeStrs));
		// set schema evolution -- excluding partition cols
		int numNonPartCol = fieldNames.length - partitionKeys.size();
		jobConf.set(SCHEMA_EVOLUTION_COLUMNS, String.join(",", Arrays.copyOfRange(fieldNames, 0, numNonPartCol)));
		jobConf.set(SCHEMA_EVOLUTION_COLUMNS_TYPES, String.join(",", typeStrs.subList(0, numNonPartCol)));

		// in older versions, parquet reader also expects the selected col indices in conf, excluding part cols
		String readColIDs = Arrays.stream(selectedFields)
				.filter(i -> i < numNonPartCol)
				.mapToObj(String::valueOf)
				.collect(Collectors.joining(","));
		jobConf.set(ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR, readColIDs);
	}

	@Override
	public void reopen(HiveTableInputSplit split, Long state) throws IOException {
		this.open(split);
		this.currentReadCount = state;
		this.reader.seekToRow(state, new GenericRowData(selectedFields.length));
	}

	@Override
	public Long getCurrentState() {
		return currentReadCount;
	}

	private static boolean isVectorizationUnsupported(LogicalType t) {
		switch (t.getTypeRoot()) {
			case CHAR:
			case VARCHAR:
			case BOOLEAN:
			case BINARY:
			case VARBINARY:
			case DECIMAL:
			case TINYINT:
			case SMALLINT:
			case INTEGER:
			case BIGINT:
			case FLOAT:
			case DOUBLE:
			case DATE:
			case TIME_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITHOUT_TIME_ZONE:
			case TIMESTAMP_WITH_LOCAL_TIME_ZONE:
				return false;
			case TIMESTAMP_WITH_TIME_ZONE:
			case INTERVAL_YEAR_MONTH:
			case INTERVAL_DAY_TIME:
			case ARRAY:
			case MULTISET:
			case MAP:
			case ROW:
			case DISTINCT_TYPE:
			case STRUCTURED_TYPE:
			case NULL:
			case RAW:
			case SYMBOL:
			default:
				return true;
		}
	}

	private boolean useParquetVectorizedRead(HiveTablePartition partition) {
		if (!isParquet(partition.getStorageDescriptor())) {
			return false;
		}

		for (int i : selectedFields) {
			if (isVectorizationUnsupported(fieldTypes[i].getLogicalType())) {
				LOG.info("Fallback to hadoop mapred reader, unsupported field type: " + fieldTypes[i]);
				return false;
			}
		}

		LOG.info("Use flink parquet ColumnarRowData reader.");
		return true;
	}

	private boolean useOrcVectorizedRead(HiveTablePartition partition) {
		if (!isOrc(partition.getStorageDescriptor())) {
			return false;
		}

		for (int i : selectedFields) {
			if (isVectorizationUnsupported(fieldTypes[i].getLogicalType())) {
				LOG.info("Fallback to hadoop mapred reader, unsupported field type: " + fieldTypes[i]);
				return false;
			}
		}

		LOG.info("Use flink orc ColumnarRowData reader.");
		return true;
	}

	@Override
	public boolean reachedEnd() throws IOException {
		// currentReadCount will be save in state, but totalReadCount will not.
		// So, currentReadCount may be greater than totalReadCount.
		if (limit > 0 && (currentReadCount >= limit || totalReadCount >= limit)) {
			return true;
		} else {
			return reader.reachedEnd();
		}
	}

	@Override
	public boolean reachedAllSplitsEnd() {
		return limit > 0 && totalReadCount >= limit;
	}

	@Override
	public RowData nextRecord(RowData reuse) throws IOException {
		currentReadCount++;
		totalReadCount++;
		return reader.nextRecord(reuse);
	}

	@Override
	public void close() throws IOException {
		if (this.reader != null) {
			this.reader.close();
			this.reader = null;
		}
	}

	@Override
	public HiveTableInputSplit[] createInputSplits(int minNumSplits)
			throws IOException {
		return createInputSplits(minNumSplits, s -> s);
	}

	public static HiveTableInputSplit[] createInputSplits(
			int minNumSplits,
			List<HiveTablePartition> partitions,
			JobConf jobConf) throws IOException {
		return createInputSplits(minNumSplits, partitions, jobConf, false, s -> s, false);
	}

	protected HiveTableInputSplit[] createInputSplits(
			int minNumSplits,
			Function<InputSplit[], InputSplit[]> sortFunction) throws IOException {
		return createInputSplits(minNumSplits, partitions, jobConf,
			createSplitInParallel, sortFunction, useFastGetSplits);
	}

	private static HiveTableInputSplit[] createInputSplits(
			int minNumSplits,
			List<HiveTablePartition> partitions,
			JobConf jobConf,
			boolean createSplitInParallel,
			Function<InputSplit[], InputSplit[]> sortFunction,
			boolean useFastGetSplits) throws IOException {
		if (createSplitInParallel) {
			return createInputSplitsInParallel(minNumSplits, partitions, jobConf, sortFunction, useFastGetSplits);
		} else {
			return createInputSplitsSerially(minNumSplits, partitions, jobConf, sortFunction, useFastGetSplits);
		}
	}

	private static HiveTableInputSplit[] createInputSplitsSerially(
			int minNumSplits,
			List<HiveTablePartition> partitions,
			JobConf jobConf,
			Function<InputSplit[], InputSplit[]> sortFunction,
			boolean useFastGetSplits) throws IOException {
		List<HiveTableInputSplit> hiveSplits = new ArrayList<>();
		int splitNum = 0;
		AtomicReference<FileSystem> fsRef = new AtomicReference<>();
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			InputFormat format = prepareInputFormat(fsRef, sd, jobConf, useFastGetSplits);
			if (format == null) {
				continue;
			}
			ReflectionUtils.setConf(format, jobConf);
			jobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			org.apache.hadoop.mapred.InputSplit[] splitArray =
				sortFunction.apply(format.getSplits(jobConf, minNumSplits));
			for (org.apache.hadoop.mapred.InputSplit inputSplit : splitArray) {
				hiveSplits.add(new HiveTableInputSplit(splitNum++, inputSplit, jobConf, partition));
			}
		}

		return hiveSplits.toArray(new HiveTableInputSplit[0]);
	}

	private static HiveTableInputSplit[] createInputSplitsInParallel(
			int minNumSplits,
			List<HiveTablePartition> partitions,
			JobConf jobConf,
			Function<InputSplit[], InputSplit[]> sortFunction,
			boolean useFastGetSplits) throws IOException {
		List<HiveTableInputSplit> hiveSplits = new ArrayList<>();
		int splitNum = 0;
		List<CompletableFuture<Tuple3<HiveTablePartition, InputSplit[], IOException>>> futureList = new ArrayList<>();
		AtomicReference<FileSystem> fsRef = new AtomicReference<>();
		for (HiveTablePartition partition : partitions) {
			StorageDescriptor sd = partition.getStorageDescriptor();
			JobConf newJobConf = new JobConf(jobConf);
			InputFormat format = prepareInputFormat(fsRef, sd, newJobConf, useFastGetSplits);
			if (format == null) {
				continue;
			}
			ReflectionUtils.setConf(format, newJobConf);
			newJobConf.set(INPUT_DIR, sd.getLocation());
			//TODO: we should consider how to calculate the splits according to minNumSplits in the future.
			CompletableFuture<Tuple3<HiveTablePartition, InputSplit[], IOException>> future =
				CompletableFuture.supplyAsync(() -> {
					try {
						return Tuple3.of(partition, format.getSplits(newJobConf, minNumSplits), null);
					} catch (IOException e) {
						return Tuple3.of(null, null, e);
					}
				});
			futureList.add(future);
		}

		for (CompletableFuture<Tuple3<HiveTablePartition, InputSplit[], IOException>> future : futureList) {
			try {
				Tuple3<HiveTablePartition, InputSplit[], IOException> tuple3 = future.get();
				if (tuple3.f2 != null) {
					throw tuple3.f2;
				}
				HiveTablePartition partition = tuple3.f0;
				InputSplit[] splitArray = sortFunction.apply(tuple3.f1);

				if (splitArray != null) {
					for (org.apache.hadoop.mapred.InputSplit inputSplit : splitArray) {
						hiveSplits.add(new HiveTableInputSplit(splitNum++, inputSplit, jobConf, partition));
					}
				}
			} catch (InterruptedException | ExecutionException e) {
				throw new FlinkRuntimeException("Failed to create spite.", e);
			}
		}

		return hiveSplits.toArray(new HiveTableInputSplit[0]);
	}

	private static InputFormat prepareInputFormat(
			AtomicReference<FileSystem> fsRef,
			StorageDescriptor sd,
			JobConf jobConf,
			boolean useFastFileFormat) throws IOException {
		Path inputPath = new Path(sd.getLocation());
		if (fsRef.get() == null) {
			fsRef.set(inputPath.getFileSystem(jobConf));
		}
		// it's possible a partition exists in metastore but the data has been removed
		if (!fsRef.get().exists(inputPath)) {
			return null;
		}
		if (useFastFileFormat) {
			if (isOrc(sd) || isParquet(sd)) {
				LOG.info("We will use FastFileInputFormat instead of {} to get splits.",
					sd.getSerdeInfo().getSerializationLib());
				return new FastFileInputFormat();
			} else {
				LOG.warn("Current we only use FastFileInputFormat in parquet and orc source, " +
					"current source is {}", sd.getSerdeInfo().getSerializationLib());
			}
		}
		try {
			return (InputFormat) Class.forName(sd.getInputFormat(), true,
				Thread.currentThread().getContextClassLoader()).newInstance();
		} catch (Exception e) {
			throw new FlinkHiveException("Unable to instantiate the hadoop input format", e);
		}
	}

	@Override
	public BaseStatistics getStatistics(BaseStatistics cachedStats) {
		// no statistics available
		return null;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
		return new LocatableInputSplitAssigner(inputSplits);
	}

	private static boolean isOrc(StorageDescriptor sd) {
		return sd.getSerdeInfo().getSerializationLib().toLowerCase().contains("orc");
	}

	private static boolean isParquet(StorageDescriptor sd) {
		return sd.getSerdeInfo().getSerializationLib().toLowerCase().contains("parquet");
	}

	// --------------------------------------------------------------------------------------------
	//  Custom serialization methods
	// --------------------------------------------------------------------------------------------

	private void writeObject(ObjectOutputStream out) throws IOException {
		super.write(out);
		jobConf.write(out);
		out.writeObject(partitionKeys);
		out.writeObject(fieldTypes);
		out.writeObject(fieldNames);
		out.writeObject(partitions);
		out.writeObject(selectedFields);
		out.writeObject(limit);
		out.writeObject(hiveVersion);
		out.writeBoolean(useMapRedReader);
		out.writeBoolean(createSplitInParallel);
		out.writeBoolean(useFastGetSplits);
		writeExtraField(out);
	}

	@SuppressWarnings("unchecked")
	private void readObject(ObjectInputStream in) throws IOException, ClassNotFoundException {
		super.read(in);
		if (jobConf == null) {
			jobConf = new JobConf();
		}
		jobConf.readFields(in);
		jobConf.getCredentials().addAll(this.credentials);
		Credentials currentUserCreds = getCredentialsFromUGI(UserGroupInformation.getCurrentUser());
		if (currentUserCreds != null) {
			jobConf.getCredentials().addAll(currentUserCreds);
		}
		partitionKeys = (List<String>) in.readObject();
		fieldTypes = (DataType[]) in.readObject();
		fieldNames = (String[]) in.readObject();
		partitions = (List<HiveTablePartition>) in.readObject();
		selectedFields = (int[]) in.readObject();
		limit = (long) in.readObject();
		hiveVersion = (String) in.readObject();
		useMapRedReader = in.readBoolean();
		createSplitInParallel = in.readBoolean();
		useFastGetSplits = in.readBoolean();
		readExtraField(in);
	}

	protected void readExtraField(ObjectInputStream in) throws IOException {
	}

	protected void writeExtraField(ObjectOutputStream out) throws IOException {
	}
}
