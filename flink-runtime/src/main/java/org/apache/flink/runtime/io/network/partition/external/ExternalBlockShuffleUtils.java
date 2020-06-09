/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.	See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.	The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.	You may obtain a copy of the License at
 *
 *		 http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.runtime.io.network.partition.external;

import org.apache.flink.runtime.executiongraph.ExecutionAttemptID;
import org.apache.flink.runtime.io.network.partition.ResultPartitionID;
import org.apache.flink.runtime.jobgraph.IntermediateDataSetID;
import org.apache.flink.runtime.jobgraph.IntermediateResultPartitionID;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * The utils of external block shuffle.
 */
public class ExternalBlockShuffleUtils {
	private static final Logger LOG = LoggerFactory.getLogger(ExternalBlockShuffleUtils.class);

	/** FINISHED_FILE is used to identify whether a result partition is finished and ready to be fetched. */
	private static final String FINISHED_FILE = "finished";

	/** The prefix of result partition's relative directory path. */
	private static final String PARTITION_DIR_PREFIX = "partition_";

	/** The splitter to separate producerId and partitionId in constructing partition director. */
	private static final String SPLITTER = "_";

	/** Generate result partition's directory based on the given root directory. */
	public static String generatePartitionRootPath(String prefix, ResultPartitionID partitionId) {
		return prefix + "/" + PARTITION_DIR_PREFIX +
				partitionId.getPartitionId().getLowerPart() + SPLITTER +
				partitionId.getPartitionId().getUpperPart() + SPLITTER +
				partitionId.getPartitionId().getPartitionNum() + SPLITTER +
				partitionId.getProducerId().getLowerPart() + SPLITTER +
				partitionId.getProducerId().getUpperPart() + "/";
	}

	/** Convert relative partition directory to ResultPartitionID according to generatePartitionRootPath() method. */
	public static ResultPartitionID convertRelativeDirToResultPartitionID(String relativeDir) {
		if (!relativeDir.startsWith(PARTITION_DIR_PREFIX)) {
			return null;
		}
		String[] segments = relativeDir.substring(PARTITION_DIR_PREFIX.length()).split(SPLITTER);
		if (segments.length != 5) {
			return null;
		}
		try {
			return new ResultPartitionID(
						new IntermediateResultPartitionID(new IntermediateDataSetID(
								Long.parseLong(segments[0]),
								Long.parseLong(segments[1])),
								Integer.parseInt(segments[2])),
						new ExecutionAttemptID(Long.parseLong(segments[2]), Long.parseLong(segments[3])));
		} catch (Exception e) {
			return null;
		}
	}

	public static String generateSubPartitionFile(String partitionDir, Integer subPartitionIndex) {
		return partitionDir + subPartitionIndex + SPLITTER + FINISHED_FILE;
	}

	/** Convert sub partition file name to SubPartition index according to generateSubPartitionFile() method. */
	public static Integer convertFileNameToSubPartitionIndex(String subPartitionFile) {
		if (!subPartitionFile.endsWith(FINISHED_FILE)) {
			return null;
		}
		String[] segments = subPartitionFile.split(SPLITTER);
		if (segments == null || segments.length != 2) {
			return null;
		}
		try {
			return Integer.valueOf(segments[0]);
		} catch (Exception e) {
			return null;
		}
	}

}
