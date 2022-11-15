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

package org.apache.flink.connectors.hive.read;

import org.apache.flink.api.java.tuple.Tuple2;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.connectors.hive.HiveTableSource;
import org.apache.flink.connectors.hive.JobConfWrapper;
import org.apache.flink.streaming.api.functions.source.ContinuousFileSyncReaderOperator;
import org.apache.flink.streaming.api.functions.source.RichSourceFunction;
import org.apache.flink.streaming.api.watermark.Watermark;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.catalog.ObjectPath;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.catalog.hive.util.HiveReflectionUtils;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.IMetaStoreClient;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.mapred.JobConf;
import org.mortbay.log.Log;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.TreeSet;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * This class is highly similar with {@link HiveContinuousMonitoringFunction},
 * it is responsible for:
 *
 * <ol>
 *     <li>Monitoring specific partitions of hive meta store.</li>
 *     <li>Deciding which partitions should be further read and processed.</li>
 *     <li>Creating the {@link HiveTableInputSplit splits} corresponding to those partitions.</li>
 *     <li>Assigning them to downstream tasks for further processing.</li>
 * </ol>
 *
 * <p>The splits to be read are forwarded to the downstream {@link ContinuousFileSyncReaderOperator}
 * which can have parallelism greater than one but read the splits synchronously.
 *
 * <p><b>IMPORTANT NOTE: </b> The partitions of hive should include at least date partition or both date and hour partition. Meanwhile,
 * we use current system date as base time to probing the partitions in hive. We construct the partition name in certain range
 * and sort them in descending order. Then choose the first partition list which satisfied the time requirement that can be obtained and forward the
 * corresponding splits to downstream.
 */
public class HiveContinuousMonitoringNewestPartitionFunction
	extends RichSourceFunction<TimestampedHiveInputSplit> {

	private static final Logger LOG = LoggerFactory.getLogger(HiveContinuousMonitoringNewestPartitionFunction.class);

	private final HiveShim hiveShim;

	protected final JobConfWrapper conf;

	protected final ObjectPath tablePath;

	protected final int readerParallelism;

	private final List<String> partitionKeys;

	private final String[] fieldNames;

	private final DataType[] fieldTypes;

	private volatile boolean isRunning = true;

	private long scanCostMs;
	private final long  scanIntervalMs;

	private final int countOfScanTimes;

	/** The first position indicates the date partition name, and the second position is the corresponding pattern. */
	private final Tuple2<String, String> datePartition;

	private final Tuple2<String, String> hourPartition;

	private final int forwardPartitionNum;

	private final String partitionFilter;

	/** f0 represents forward range and f1 represents backward range.*/
	private final Tuple2<Integer, Integer> partitionPendingRange;

	private final long partitionPendingTimeout;

	/** Transform the current date into specified format for partition discovery.*/
	private final SimpleDateFormat dateFormat;

	private final Calendar cal;

	/** The first position indicates the Date type corresponding to the partition name, and the second position is the name of the corresponding partition. */
	private Date newestPartitionDate;

	private PartitionList newestPartitions;

	/** Record the time which newest partition was discovered.*/
	private long newestPartDiscTime;

	private final int probeLevel;

	protected IMetaStoreClient client;

	private transient Properties tableProps;

	private transient String defaultPartitionName;

	private final short partitionNumLimit = 10000;

	private final int hiveClientRetryTimes;

	private final boolean useFlinkGetSplits;

	public HiveContinuousMonitoringNewestPartitionFunction(
			HiveShim hiveShim,
			JobConf conf,
			ObjectPath tablePath,
			CatalogTable catalogTable,
			int readerParallelism,
			long scanIntervalMs,
			int countOfScanTimes,
			Tuple2<String, String> datePartition,
			Tuple2<String, String> hourPartition,
			int forwardPartitionNum,
			String partitionFilter,
			Tuple2<Integer, Integer> partitionPendingRange,
			long partitionPendingTimeout,
			int hiveClientRetryTimes,
			boolean useFlinkGetSplits) {

		this.hiveShim = hiveShim;
		this.conf = new JobConfWrapper(conf);
		this.tablePath = tablePath;
		this.readerParallelism = Math.max(readerParallelism, 1);
		this.partitionKeys = catalogTable.getPartitionKeys();
		this.fieldNames = catalogTable.getSchema().getFieldNames();
		this.fieldTypes = catalogTable.getSchema().getFieldDataTypes();

		this.scanCostMs = 0L;
		this.scanIntervalMs = scanIntervalMs;
		this.countOfScanTimes = countOfScanTimes;

		this.datePartition = datePartition;
		this.hourPartition = hourPartition;
		this.forwardPartitionNum = forwardPartitionNum;
		this.partitionFilter = partitionFilter;
		this.partitionPendingRange = partitionPendingRange;
		this.partitionPendingTimeout = partitionPendingTimeout;

		String formattedPattern;
		if (this.hourPartition != null){
			formattedPattern = this.datePartition.f1 + " " + this.hourPartition.f1;
			this.probeLevel = Calendar.HOUR;
		} else {
			formattedPattern = this.datePartition.f1;
			this.probeLevel = Calendar.DATE;
		}
		this.dateFormat = new SimpleDateFormat(formattedPattern);
		this.dateFormat.setLenient(false);
		this.cal = Calendar.getInstance();

		this.hiveClientRetryTimes = hiveClientRetryTimes;
		this.useFlinkGetSplits = useFlinkGetSplits;
	}

	@Override
	public void run(SourceContext<TimestampedHiveInputSplit> context) throws Exception {

		int curScanTimes = 0;

		// initialize the hive client for it can not be serialized
		int curRetryTimes = 0;
		while (true) {
			try {
				this.client = this.hiveShim.getHiveMetastoreClient(new HiveConf(this.conf.conf(), HiveConf.class));
				Table hiveTable = this.client.getTable(this.tablePath.getDatabaseName(), this.tablePath.getObjectName());
				this.tableProps = HiveReflectionUtils.getTableMetadata(hiveShim, hiveTable);
				this.defaultPartitionName = this.conf.conf().get(HiveConf.ConfVars.DEFAULTPARTITIONNAME.varname,
					HiveConf.ConfVars.DEFAULTPARTITIONNAME.defaultStrVal);
				break;
			} catch (Exception e) {
				if (curRetryTimes == this.hiveClientRetryTimes) {
					throw new RuntimeException("Retry " + curRetryTimes + " times to initialize hive client and table information failed. Exceed the maximum times " + this.hiveClientRetryTimes + ".");
				}
				LOG.warn("Initialize hive client and table information failed. Please check the hive status. Now retry " + curRetryTimes + " times.", e);
				curRetryTimes++;
			}
			Thread.sleep(2000);
		}

		while (isRunning) {

			if ((curScanTimes < this.countOfScanTimes) || this.countOfScanTimes < 0) {

				this.scanCostMs = System.currentTimeMillis();
				monitorAndForwardNewestSplits(context);
				this.scanCostMs = System.currentTimeMillis() - this.scanCostMs;

				if ((curScanTimes < this.countOfScanTimes)) {
					curScanTimes++;
				}

			} else {
				LOG.info("The continuous monitoring function is terminated because the monitoring time reach the scan.count-of-scan-times.");
				isRunning = false;
			}

			LOG.info("The scan cost for newest partition is {} ms.", this.scanCostMs);

			if (this.scanIntervalMs > this.scanCostMs) {
				Thread.sleep(this.scanIntervalMs - this.scanCostMs);
			} else {
				LOG.warn("The scan cost {} ms has exceeds the scan interval time {} ms. Maybe the hive can not return partition information in time.", this.scanCostMs, this.scanIntervalMs);
			}
		}
	}

	private void monitorAndForwardNewestSplits(
		SourceContext<TimestampedHiveInputSplit> context) throws Exception {

		PartitionList obtainedPartitions = getNewestPartitionsList();

		if (this.newestPartitions == null && obtainedPartitions == null) {
			throw new FlinkRuntimeException("Can not find the newest partition for the first time and terminate the program. The reason might be:\n" +
				"1.The partition dose not generate in the partition-pending-range(both forward and backward). Please check the hive table and increase the pending range.\n" +
				"2.The partition name is not the same with your hive table. Please check the date-partition-pattern [" + this.datePartition.f0 + ", " + this.datePartition.f1 + "]" +
				" and hour-partition-pattern (if existd) [" + (this.hourPartition == null ? "null" : this.hourPartition.f0) + ", " + (this.hourPartition == null ? "null" : this.hourPartition.f1) + "] is illegal.\n" +
				"3.All founded partitions is partially generated. Not satisfied the forward-partition-num config which is " + this.forwardPartitionNum + ".");
		} else if (obtainedPartitions == null) {
			LOG.warn("While discovering new partition it returns null which means no partition is discovered. Use old partition instead. Program will not be terminated but it is better to check hive state.");
			return;
		} else {
			if (this.newestPartitions != null && this.newestPartitions.equals(obtainedPartitions)) {
				if (System.currentTimeMillis() - this.newestPartDiscTime > this.partitionPendingTimeout) {
					throw new TimeoutException("The old partition has maintained over " + this.partitionPendingTimeout + " ms. Please check why the new partition has not been generated or increase partition-pending-timeout.");
				}
				LOG.info("The founded newest partition {} is the same as the last time. Forwarding process will not be executed.", this.newestPartitions.parNamesToString());
				return;
			}

			this.newestPartitions = obtainedPartitions;
			this.newestPartDiscTime = System.currentTimeMillis();
		}

		LOG.info("Found newest partition {} of table {}. Forwarding newest partition splits to downstream operators.", this.newestPartitions.parNamesToString(), tablePath.getFullName());

		List<Tuple2<HiveTableInputSplit, Integer>> splitsWithTs = new ArrayList<>();
		HiveTableInputSplit[] splits;
		for (int i = 0; i < this.newestPartitions.size; i++) {
			// could read in parallel but serially for now
			splits = HiveTableInputFormat.createInputSplits(
				this.readerParallelism,
				Collections.singletonList(toHiveTablePartition(this.newestPartitions.get(i))),
				this.conf.conf(),
				this.useFlinkGetSplits);

			for (HiveTableInputSplit split : splits) {
				splitsWithTs.add(new Tuple2<>(split, this.newestPartitions.get(i).getLastAccessTime()));
			}
		}

		if (splitsWithTs.size() == 0){
			LOG.info("Newest partition split size is 0. Can not forward the splits because the state view size would be zero.");
			return;
		}

		LOG.info("Newest partition splits size is " + splitsWithTs.size());

		// emit watermark to notify downstream operator start to construct dimension table
		long constructDimTableWatermark = System.currentTimeMillis();
		context.emitWatermark(new Watermark(constructDimTableWatermark));
		// emit data
		for (Tuple2<HiveTableInputSplit, Integer> split : splitsWithTs) {
			context.collect(new TimestampedHiveInputSplit(split.f1, split.f0));
		}
		// emit watermark to notify downstream operator stop to construct dimension table
		context.emitWatermark(new Watermark(constructDimTableWatermark + 1));
	}

	@Override
	public boolean withSameWatermarkPerBatch() {
		return scanIntervalMs != 0;
	}

	private PartitionList getNewestPartitionsList() throws Exception{

		// the first time a partition is probed, starting from the current system time.
		if (this.newestPartitionDate == null) {
			Date currentDate = new Date();
			try {
				this.newestPartitionDate = this.dateFormat.parse(this.dateFormat.format(currentDate));
			} catch (ParseException e) {
				throw new ParseException("Can not parse the system date time. Please check the partition pattern is correct. " +
					"Your specified date partition pattern is " + this.datePartition.f1 + " and the hour partition pattern (if exist) is " + (this.hourPartition == null ? "null" : this.hourPartition.f1) + ".", e.getErrorOffset());
			}
		}

		// construct the partition date and filter string in a range
		// only the first time a partition is probed, we probe both forward and backward
		// otherwise we only probe forward
		int lowerBounds = this.newestPartitions == null ? -this.partitionPendingRange.f1 : 0;

		String partitionFilterStr = getPartitionFilterStr(
										this.datePartition,
										this.hourPartition,
										this.partitionFilter,
										this.partitionPendingRange,
										lowerBounds,
										this.probeLevel,
										this.newestPartitionDate,
										this.cal,
										this.dateFormat);

		// hive limit the return partition number
		int probeSize = (this.partitionPendingRange.f0 - lowerBounds + 1) * this.forwardPartitionNum;
		if (probeSize > this.partitionNumLimit) {
			throw new RuntimeException("The probe partitions number has reached " + probeSize + ". " +
				"The hive server can not return partitions over " + this.partitionNumLimit + ". Please decrease the partition-pending-range or forward-partition-num.");
		}

		Log.info("The partition filter query is '{}'.", partitionFilterStr);

		// get partitions from hive
		// client would retry if it can not obtain partition and throw exceptions
		// if there is no partition found the newestPartitionsList would be empty list rather than null
		int curRetryTimes = 0;
		List<Partition> newestPartitionsList;
		while (true) {
			try {
				newestPartitionsList = this.client.listPartitionsByFilter(
					this.tablePath.getDatabaseName(),
					this.tablePath.getObjectName(),
					partitionFilterStr,
					this.partitionNumLimit);
				break;
			} catch (Exception e) {
				if (curRetryTimes == this.hiveClientRetryTimes) {
					LOG.warn("Retry {} times to obtain newest partition from hive failed. Exceed the maximum times {}.", curRetryTimes, this.hiveClientRetryTimes);
					return null;
				}
				LOG.warn("Obtain newest partition from hive failed. Please check the hive status. Now retry " + curRetryTimes + " times.", e);
				curRetryTimes++;
			}
			Thread.sleep(2000);
		}

		// partition group by date
		// the key would be like a Date type value Mon Oct 24 20:00:00 CST 2022
		Map<Date, List<Partition>> groupPartitionMap = newestPartitionsList.stream().collect(Collectors.groupingBy(partition -> {
			String groupByStr = partition.getValues().get(this.partitionKeys.indexOf(this.datePartition.f0));
			if (this.hourPartition != null) {
				groupByStr += " " + partition.getValues().get(this.partitionKeys.indexOf(this.hourPartition.f0));
			}
			try {
				return this.dateFormat.parse(groupByStr);
			} catch (ParseException e) {
				throw new RuntimeException(e);
			}
		}));

		// sort the partition date in descending chronological order
		Set<Date> sortedPartitionDate = new TreeSet<>((o1, o2) -> -o1.compareTo(o2));
		sortedPartitionDate.addAll(groupPartitionMap.keySet());

		for (Date partDate : sortedPartitionDate) {
			// key will definitely exist for it comes from groupPartitionMap key set
			if (groupPartitionMap.get(partDate).size() == this.forwardPartitionNum) {
				// update the newestPartitionDate for the next time to probe
				this.newestPartitionDate = partDate;
				return new PartitionList(groupPartitionMap.get(partDate));
			} else {
				LOG.info("Probe partition {} and discover {} partition which does not satisfied the forward-partition-num {}.", partDate, groupPartitionMap.get(partDate).size(), this.forwardPartitionNum);
			}
		}

		return null;
	}

	private class PartitionList {
		public List<Partition> partitionList;
		public int size;

		public PartitionList(List<Partition> partitionList){
			this.partitionList = partitionList;
			this.size = partitionList.size();
			// partitions in the list should be in the same order for equal method to compare two list
			this.partitionList.sort((o1, o2) -> (-o1.compareTo(o2)));
		}

		public Partition get(int index) {
			return this.partitionList.get(index);
		}

		@Override
		public boolean equals(Object obj) {
			if (obj instanceof PartitionList) {
				PartitionList tmpPartitionList = (PartitionList) obj;
				if (tmpPartitionList.partitionList.size() != this.partitionList.size()) {
					return false;
				}
				// directly compare two partitions use equal methods
				for (int i = 0; i < this.partitionList.size(); i++) {
					if (!this.partitionList.get(i).equals(tmpPartitionList.partitionList.get(i))) {
						return false;
					}
				}
				return true;
			}
			return false;
		}

		public String parNamesToString() {
			StringBuilder sb = new StringBuilder();
			sb.append("[");
			for (int i = 0; i < this.partitionList.size(); i++){
				sb.append(this.partitionList.get(i).getValues());
				if (i != this.partitionList.size() - 1) {
					sb.append(", ");
				}
			}
			sb.append("]");
			return sb.toString();
		}
	}

	// construct partition filter string according current system time
	public static String getPartitionFilterStr(
			Tuple2<String, String> datePartition,
			Tuple2<String, String> hourPartition,
			String partitionFilter,
			Tuple2<Integer, Integer> partitionPendingRange,
			int lowerBounds,
			int probeLevel,
			Date newestPartitionDate,
			Calendar cal,
			SimpleDateFormat dateFormat
			) {

		int range = lowerBounds;
		// store the partition date and construct filter string
		String probePartitionDateStr;
		String partitionFilterStr = "(";

		while (range <= partitionPendingRange.f0) {

			cal.setTime(newestPartitionDate);
			cal.add(probeLevel, range);
			probePartitionDateStr = dateFormat.format(cal.getTime());

			// construct the filter string
			if (range == lowerBounds) {
				partitionFilterStr += "(" + datePartition.f0 + "='" + probePartitionDateStr.split(" ")[0] + "'";
				if (hourPartition != null) {
					partitionFilterStr += " and " + hourPartition.f0 + ">='" + probePartitionDateStr.split(" ")[1] + "'";
				}
				partitionFilterStr += ") or ('" + probePartitionDateStr.split(" ")[0] + "'<" + datePartition.f0;
			} else if (range == partitionPendingRange.f0) {
				partitionFilterStr += " and " + datePartition.f0 + "<'" + probePartitionDateStr.split(" ")[0] + "')";
				partitionFilterStr += " or (" + datePartition.f0 + "='" + probePartitionDateStr.split(" ")[0] + "'";
				if (hourPartition != null) {
					partitionFilterStr += " and " + hourPartition.f0 + "<='" + probePartitionDateStr.split(" ")[1] + "'";
				}
				partitionFilterStr += ")";
			}

			range += partitionPendingRange.f0 - lowerBounds;
		}
		partitionFilterStr += ")";

		if (partitionFilter != null) {
			partitionFilterStr += " and (" + partitionFilter + ")";
		}

		return partitionFilterStr;
	}

	@Override
	public void close() {
		cancel();
	}

	@Override
	public void cancel() {
		if (this.client != null) {
			this.client.close();
			this.client = null;
		}
	}

	protected HiveTablePartition toHiveTablePartition(Partition p) {
		return HiveTableSource.toHiveTablePartition(
			partitionKeys, fieldNames, fieldTypes, hiveShim, tableProps, defaultPartitionName, p);
	}
}
