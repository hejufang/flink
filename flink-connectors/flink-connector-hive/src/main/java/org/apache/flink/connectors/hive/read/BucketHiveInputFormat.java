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

import org.apache.flink.api.common.io.BucketInputFormat;
import org.apache.flink.connectors.hive.HiveBucketInputSplitAssigner;
import org.apache.flink.connectors.hive.HiveTablePartition;
import org.apache.flink.core.io.InputSplitAssigner;
import org.apache.flink.table.catalog.CatalogTable;
import org.apache.flink.table.planner.plan.utils.HiveUtils$;
import org.apache.flink.util.FlinkRuntimeException;

import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputSplit;
import org.apache.hadoop.mapred.JobConf;

import java.io.IOException;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;
import java.util.Arrays;
import java.util.Comparator;
import java.util.List;

/**
 * BucketHiveInputFormat.
 */
public class BucketHiveInputFormat extends HiveTableInputFormat implements BucketInputFormat {
	private static final long serialVersionUID = 1L;
	private int bucketNum;
	private int nextOperatorParallelism = -1;

	public BucketHiveInputFormat(
			JobConf jobConf,
			CatalogTable catalogTable,
			List<HiveTablePartition> partitions,
			int[] projectedFields,
			long limit,
			String hiveVersion,
			boolean useMapRedReader,
			boolean createSplitInParallel) {
		super(jobConf, catalogTable, partitions, projectedFields, limit,
			hiveVersion, useMapRedReader, createSplitInParallel, true);
		this.bucketNum = HiveUtils$.MODULE$.getBucketNum(catalogTable.getOptions());
	}

	@Override
	public void open(HiveTableInputSplit split) throws IOException {
		int totalTasks = getRuntimeContext().getNumberOfParallelSubtasks();
		assert split.getSplitNumber() % totalTasks == getRuntimeContext().getIndexOfThisSubtask();
		super.open(split);
	}

	@Override
	public HiveTableInputSplit[] createInputSplits(int minNumSplits) throws IOException {
		return createInputSplits(bucketNum, splits -> {
			Arrays.sort(splits, new Comparator<InputSplit>() {
				@Override
				public int compare(InputSplit o1, InputSplit o2) {
					return ((FileSplit) o1).getPath().toString().compareTo(
						((FileSplit) o2).getPath().toString());
				}
			});
			return splits;
		});
	}

	@Override
	public int getBucketNum() {
		return bucketNum;
	}

	@Override
	public InputSplitAssigner getInputSplitAssigner(HiveTableInputSplit[] inputSplits) {
		if (nextOperatorParallelism <= 0) {
			throw new FlinkRuntimeException(
				"nextOperatorParallelism can't be -1, this can't happened");
		}
		return new HiveBucketInputSplitAssigner(inputSplits, nextOperatorParallelism);
	}

	@Override
	public void setOperatorParallelism(int parallelism) {
		this.nextOperatorParallelism = parallelism;
	}

	@Override
	protected void writeExtraField(ObjectOutputStream out) throws IOException {
		out.writeInt(bucketNum);
		out.writeInt(nextOperatorParallelism);
	}

	@Override
	protected void readExtraField(ObjectInputStream in) throws IOException {
		bucketNum = in.readInt();
		nextOperatorParallelism = in.readInt();
	}
}
