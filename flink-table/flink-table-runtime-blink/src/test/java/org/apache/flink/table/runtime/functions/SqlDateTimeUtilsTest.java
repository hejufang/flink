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

package org.apache.flink.table.runtime.functions;

import org.apache.flink.table.data.TimestampData;

import org.apache.calcite.avatica.util.DateTimeUtils;
import org.junit.Assert;
import org.junit.Test;

import java.sql.Timestamp;

/**
 * Unit test for {@link SqlDateTimeUtils}.
 */
public class SqlDateTimeUtilsTest {
	private static final String TIMESTAMP_STR_1 = "0000-00-00 00:00:00";
	private static final String TIMESTAMP_STR_2 = "0001-01-01 00:00:00";
	private static final TimestampData TIMESTAMP_DATA_1 =
		TimestampData.fromTimestamp(Timestamp.valueOf(TIMESTAMP_STR_2));

	private static final String DATE_STR_1 = "0000-00-00";
	private static final Integer DATE_1 = DateTimeUtils.ymdToUnixDate(1, 1, 1);

	@Test
	public void testToTimestampData() {
		// `0000-00-00 00:00:00` will be transform to `0001-01-01 00:00:00` when
		// compatibleWithMySQL is true.
		TimestampData timestampData1 = SqlDateTimeUtils.toTimestampData(TIMESTAMP_STR_1, true);
		Assert.assertEquals(timestampData1, TIMESTAMP_DATA_1);
		TimestampData timestampData2 = SqlDateTimeUtils.toTimestampData(TIMESTAMP_STR_1, false);
		Assert.assertNull(timestampData2);
		TimestampData timestampData3 = SqlDateTimeUtils.toTimestampData(TIMESTAMP_STR_1);
		Assert.assertNull(timestampData3);
	}

	@Test
	public void testDateStringToUnixDate() {
		// `0000-00-00` will be transform to `0001-01-01` when compatibleWithMySQL is true.
		Integer date1 = SqlDateTimeUtils.dateStringToUnixDate(DATE_STR_1, true);
		Assert.assertEquals(date1, DATE_1);
		Integer date2 = SqlDateTimeUtils.dateStringToUnixDate(DATE_STR_1, false);
		Assert.assertNull(date2);
		Integer date3 = SqlDateTimeUtils.dateStringToUnixDate(DATE_STR_1);
		Assert.assertNull(date3);
	}
}
