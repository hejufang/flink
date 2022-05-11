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

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.Internal;
import org.apache.flink.core.io.GenericInputSplit;

import com.bytedance.htap.metaclient.partition.PartitionID;

import java.util.Arrays;

/**
 HtapInputSplit.
 */
@Internal
public class HtapInputSplit extends GenericInputSplit {

	private final byte[] scanToken;
	private final PartitionID partitionID;

	/**
	 * Creates a new HtapInputSplit.
	 *
	 * @param partitionNumber The number of the split's partition.
	 * @param totalNumberOfPartitions The total number of the splits (partitions).
	 */
	public HtapInputSplit(
			byte[] scanToken,
			final PartitionID partitionID,
			final int partitionNumber,
			final int totalNumberOfPartitions) {
		super(partitionNumber, totalNumberOfPartitions);
		this.partitionID = partitionID;
		this.scanToken = scanToken;
	}

	public byte[] getScanToken() {
		return scanToken;
	}

	public PartitionID getPartitionID() {
		return partitionID;
	}

	@Override
	public String toString() {
		return "HtapInputSplit{" +
			"scanToken=" + Arrays.toString(scanToken) +
			", partitionID=" + partitionID +
			", partitionNumber=" + getSplitNumber() +
			", totalNumberOfPartitions=" + getTotalNumberOfSplits() +
			'}';
	}
}
