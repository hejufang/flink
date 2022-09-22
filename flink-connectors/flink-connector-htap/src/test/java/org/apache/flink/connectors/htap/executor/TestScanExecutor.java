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

package org.apache.flink.connectors.htap.executor;

import org.apache.flink.connectors.htap.connector.executor.ScanExecutor;
import org.apache.flink.connectors.htap.connector.reader.HtapResultIterator;

import org.junit.Assert;
import org.junit.Test;

/**
 * TestScanExecutor.
 */
public class TestScanExecutor {
	@Test
	public void test() throws Exception {
		// One scanner simple test
		HtapResultIterator htapResultIterator = new HtapMockReaderIterator(1, 10, "dummy");

		Assert.assertFalse(htapResultIterator.isFinish());
		Thread.sleep(100);
		Assert.assertTrue(htapResultIterator.isFinish());

		// Small scanner after big scanner can start quickly and will be finished before big scanner
		HtapResultIterator htapResultIteratorBig = new HtapMockReaderIterator(100, 100, "dummy");
		Thread.sleep(500);
		HtapResultIterator htapResultIteratorSmall = new HtapMockReaderIterator(1, 5, "dummy");
		Assert.assertFalse(htapResultIteratorSmall.isFinish());
		Thread.sleep(100);
		Assert.assertTrue(htapResultIteratorSmall.isFinish());
		htapResultIteratorBig.close();

		// Big scanner after lots of small scanners can't be starved, but will be a little slow
		for (int i = 0; i < 50; i++) {
			new HtapMockReaderIterator(100, 5, "dummy");
		}
		htapResultIteratorBig = new HtapMockReaderIterator(1000, 2, "dummy");
		Assert.assertFalse(htapResultIteratorBig.isFinish());
		Thread.sleep(1000 * 3);
		// Still no finish
		Assert.assertFalse(htapResultIteratorBig.isFinish());
		// Wait for finish
		while (!htapResultIteratorBig.isFinish()) {
			Thread.sleep(100);
		}
		// Still have small scanners running
		Assert.assertNotEquals(0, ScanExecutor.getCreatedInstance().waitingScannerSize());
		htapResultIteratorBig.close();
		ScanExecutor.getCreatedInstance().stop();
	}
}
