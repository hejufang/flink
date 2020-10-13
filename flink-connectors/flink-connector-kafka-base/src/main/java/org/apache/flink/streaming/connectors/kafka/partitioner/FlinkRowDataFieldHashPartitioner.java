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

package org.apache.flink.streaming.connectors.kafka.partitioner;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.types.DataType;
import org.apache.flink.util.Preconditions;

import java.util.Arrays;

/**
 * Row fields hash partitioner.
 */
@PublicEvolving
public class FlinkRowDataFieldHashPartitioner extends FlinkKafkaPartitioner<RowData> {
	private static final long serialVersionUID = 1L;

	private final int[] hashFieldIndexArray;
	private final DataType[] dataTypes;
	private final Object[] reusableKeybyFields;

	public FlinkRowDataFieldHashPartitioner(int[] hashFieldIndexArray, DataType[] dataTypes) {
		this.hashFieldIndexArray = hashFieldIndexArray;
		this.dataTypes = dataTypes;
		reusableKeybyFields = new Object[hashFieldIndexArray.length];
	}

	@Override
	public int partition(RowData record, byte[] key, byte[] value, String targetTopic, int[] partitions) {
		Preconditions.checkArgument(
			partitions != null && partitions.length > 0,
			"Partitions of the target topic is empty!");

		// we send null values to the first partition.
		if (record == null) {
			return partitions[0];
		}

		for (int i = 0; i < hashFieldIndexArray.length; i++) {
			reusableKeybyFields[i] = RowData.get(record, hashFieldIndexArray[i], dataTypes[i].getLogicalType());
		}

		return partitions[(Arrays.deepHashCode(reusableKeybyFields) & 0x7FFFFFFF) % partitions.length];
	}
}
