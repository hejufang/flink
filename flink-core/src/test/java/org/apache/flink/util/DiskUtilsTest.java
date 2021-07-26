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

package org.apache.flink.util;

import org.junit.Assert;
import org.junit.Test;

import java.util.Optional;

/**
 * Test for {@link DiskUtils}.
 */
public class DiskUtilsTest {

	@Test
	public void testParseLine() {
		final String testLine1 = "NAME=\"sdf\" ROTA=\"1\" SIZE=\"5.5T\" MOUNTPOINT=\"\"";
		final String testLine2 = "NAME=\"sdf1\" ROTA=\"1\" SIZE=\"5.5T\" MOUNTPOINT=\"/data04\"";
		final String testLine3 = "NAME=\"sda1\" ROTA=\"0\" SIZE=\"116.5G\" MOUNTPOINT=\"/\"";

		Assert.assertEquals(Optional.empty(), DiskUtils.parseLine(testLine1));
		Assert.assertEquals(new DiskUtils.DiskInfo("sdf1", "1", "5.5T", "/data04"),
				DiskUtils.parseLine(testLine2).orElse(null));
		Assert.assertEquals(Optional.empty(), DiskUtils.parseLine(testLine3));
	}

	@Test
	public void testParseDataDirectory() {
		Assert.assertEquals("/data24", DiskUtils.parseDataDirectory("/data24/yarn/userlogs"));
		Assert.assertEquals("/data24", DiskUtils.parseDataDirectory("/data24"));
		Assert.assertEquals("/opt/sss", DiskUtils.parseDataDirectory("/opt/sss"));
	}
}
