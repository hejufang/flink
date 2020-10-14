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

package org.apache.flink.connectors.loghouse;

import org.apache.flink.shaded.guava18.com.google.common.io.ByteStreams;

import org.junit.Assert;
import org.junit.Test;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.util.zip.GZIPInputStream;

/**
 * Test for GzipCompressor.
 */
public class GzipCompressorTest {

	private static final String[] TEST_DATA = new String[]{
		"这是一段中文, 包含了一些特殊字符:\uD83C\uDF3A☘️",
		"this is a ascii only string",
		"{\"key1\": \"value1\", \"key2\": 123}"
	};

	@Test
	public void testCompress() throws IOException {
		GzipCompressor gzip = new GzipCompressor();
		gzip.open();

		for (String data : TEST_DATA) {
			byte[] compressResult = gzip.compress(data.getBytes());
			ByteArrayInputStream bais = new ByteArrayInputStream(compressResult);
			GZIPInputStream gzipInputStream = new GZIPInputStream(bais);
			byte[] result = ByteStreams.toByteArray(gzipInputStream);
			Assert.assertArrayEquals(result, data.getBytes());
		}

		gzip.close();
	}
}
