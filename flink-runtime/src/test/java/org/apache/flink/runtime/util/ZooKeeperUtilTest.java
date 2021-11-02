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

package org.apache.flink.runtime.util;

import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.HighAvailabilityOptions;
import org.apache.flink.runtime.testutils.ZooKeeperTestUtils;
import org.apache.flink.runtime.zookeeper.ZooKeeperTestEnvironment;
import org.apache.flink.util.TestLogger;

import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the {@link ZooKeeperUtils}.
 */
public class ZooKeeperUtilTest extends TestLogger {
	private static ZooKeeperTestEnvironment zooKeeper;

	@Rule
	public final TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void setup() {
		zooKeeper = new ZooKeeperTestEnvironment(1);
	}

	@Before
	public void cleanUp() throws Exception {
		zooKeeper.deleteAll();
	}

	@AfterClass
	public static void tearDown() throws Exception {
		if (zooKeeper != null) {
			zooKeeper.shutdown();
		}
	}

	@Test
	public void testZooKeeperEnsembleConnectStringConfiguration() throws Exception {
		// ZooKeeper does not like whitespace in the quorum connect String.
		String actual, expected;
		Configuration conf = new Configuration();

		{
			expected = "localhost:2891";

			setQuorum(conf, expected);
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);

			setQuorum(conf, " localhost:2891 "); // with leading and trailing whitespace
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);

			setQuorum(conf, "localhost :2891"); // whitespace after port
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);
		}

		{
			expected = "localhost:2891,localhost:2891";

			setQuorum(conf, "localhost:2891,localhost:2891");
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);

			setQuorum(conf, "localhost:2891, localhost:2891");
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);

			setQuorum(conf, "localhost :2891, localhost:2891");
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);

			setQuorum(conf, " localhost:2891, localhost:2891 ");
			actual = ZooKeeperUtils.getZooKeeperEnsemble(conf);
			assertEquals(expected, actual);
		}
	}

	@Test
	public void testCreateZKNamespace() throws Exception{
		String snapshotNamespace = "snapshot_namespace";
		String jobUID = "juid";
		String fsStateHandlePath = tmp.newFolder().getPath();
		Configuration configuration = ZooKeeperTestUtils.createZooKeeperHAConfig(zooKeeper.getConnectString(), fsStateHandlePath);
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, snapshotNamespace);
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, snapshotNamespace);
		String path = ZooKeeperUtils.generateCheckpointsPath(configuration, jobUID);
		assertTrue(path.contains(snapshotNamespace));
	}

	private Configuration setQuorum(Configuration conf, String quorum) {
		conf.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM, quorum);
		return conf;
	}
}
