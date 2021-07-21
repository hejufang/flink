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

package org.apache.flink.connector.bytetable.util;

import org.apache.flink.table.data.RowData;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.sql.Timestamp;
import java.util.Map;

/**
 * ByteTable Mutate Reduce Util.
 */
public class ByteTableReduceUtil {

	private static final Logger LOG = LoggerFactory.getLogger(ByteTableReduceUtil.class);

	public static boolean mutateReduce(
			int cellVersionIndex,
			ByteArrayWrapper rowkey,
			RowData newRow,
			Map<ByteArrayWrapper, RowData> mutationInfoMap) {
		// check and solve cellVersion reduce.
		boolean reduceFlag = false;
		RowData oldRow = null;
		if (cellVersionIndex != -1 && mutationInfoMap.containsKey(rowkey)) {
			oldRow = mutationInfoMap.get(rowkey);
			Timestamp newVersion = newRow.getTimestamp(cellVersionIndex, BConstants.MAX_TIMESTAMP_PRECISION).toTimestamp();
			Timestamp oldVersion = oldRow.getTimestamp(cellVersionIndex, BConstants.MAX_TIMESTAMP_PRECISION).toTimestamp();
			if (newVersion.getTime() < oldVersion.getTime()) {
				return true;
			}
		}
		if (mutationInfoMap.containsKey(rowkey)) {
			reduceFlag = true;
		}
		mutationInfoMap.put(rowkey, newRow);
		return reduceFlag;
	}
}
