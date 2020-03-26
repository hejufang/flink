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

package org.apache.flink.configuration;

import org.apache.flink.util.TestLogger;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Properties;
import java.util.UUID;

import static org.apache.flink.configuration.GlobalConfiguration.reloadConfigWithDynamicProperties;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;

/**
 * This class contains tests for the global configuration (parsing configuration directory information).
 */
public class GlobalConfigurationTest extends TestLogger {

	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testConfigurationYAML() {
		File tmpDir = tempFolder.getRoot();
		File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);

		try {
			try (final PrintWriter pw = new PrintWriter(confFile)) {

				pw.println("###########################"); // should be skipped
				pw.println("# Some : comments : to skip"); // should be skipped
				pw.println("###########################"); // should be skipped
				pw.println("common:");
				pw.println("  mykey1: myvalue1"); // OK, simple correct case
				pw.println("  mykey2       : myvalue2"); // OK, whitespace before colon is correct
				pw.println("  mykey3: myvalue3"); // OK
				pw.println("   "); // SKIP (silently)
				pw.println(" "); // SKIP (silently)
				pw.println("  mykey4: myvalue4 # some comments"); // OK, skip comments only
				pw.println("  mykey5    :    myvalue5    "); // OK, trim unnecessary whitespace

				pw.println("  mykey6: myvalue6"); // OK
				pw.println("  mykey6: myvalue7"); // OK, overwrite last value
				pw.println("  mykey8: myvalue8"); // OK
				pw.println("  # mykey9: myvalue9"); // SKIP
				pw.println("flink:");
				pw.println("  mykey8: myvalue9"); // OK, overwrite last value

			} catch (FileNotFoundException e) {
				e.printStackTrace();
			}

			Configuration conf = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());

			// all distinct keys from confFile1 + confFile2 key
			assertEquals(7, conf.keySet().size());

			// keys 1, 2, 4, 5, 6, 7, 8 should be OK and match the expected values
			assertEquals("myvalue1", conf.getString("mykey1", null));
			assertEquals("myvalue2", conf.getString("mykey2", null));
			assertEquals("myvalue3", conf.getString("mykey3", "null"));
			assertEquals("myvalue4", conf.getString("mykey4", null));
			assertEquals("myvalue5", conf.getString("mykey5", null));
			assertEquals("myvalue7", conf.getString("mykey6", null));
			assertEquals("myvalue9", conf.getString("mykey8", null));
			assertNull(conf.getString("mykey9", null));
		} finally {
			confFile.delete();
			tmpDir.delete();
		}
	}

	@Test(expected = IllegalArgumentException.class)
	public void testFailIfNull() {
		GlobalConfiguration.loadConfiguration((String) null);
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testFailIfNotLoaded() {
		GlobalConfiguration.loadConfiguration("/some/path/" + UUID.randomUUID());
	}

	@Test(expected = IllegalConfigurationException.class)
	public void testInvalidConfiguration() throws IOException {
		GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath());
	}

	@Test(expected = Exception.class)
	public void testInvalidYamlFile() throws IOException {
		final File confFile = tempFolder.newFile(GlobalConfiguration.FLINK_CONF_FILENAME);

		try (PrintWriter pw = new PrintWriter(confFile)) {
			pw.append("invalid");
		}

		GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath());
	}

	@Test
	public void testReloadConfigWithDynamicProperties() throws IOException {
		final File confFile = tempFolder.newFile(GlobalConfiguration.FLINK_CONF_FILENAME);

		try (PrintWriter pw = new PrintWriter(confFile)) {
			pw.println("common:\n" +
				"  state.checkpoints.dir: ${hdfs.prefix}/${dc}/${clusterName}/1.9/flink/fs_checkpoint_dir");
			pw.println("flink:\n" +
				"  dc: cn\n" +
				"  clusterName: flink\n" +
				"  hdfs.prefix: hdfs://haruna/flink_lf");
		}
		Configuration config = GlobalConfiguration.loadConfiguration(tempFolder.getRoot().getAbsolutePath());
		assertEquals(
			config.getString("state.checkpoints.dir", null),
			"hdfs://haruna/flink_lf/cn/flink/1.9/flink/fs_checkpoint_dir");

		Properties properties = new Properties();
		properties.setProperty("dc", "test");

		reloadConfigWithDynamicProperties(config, properties);
		assertEquals(
			config.getString("state.checkpoints.dir", null),
			"hdfs://haruna/flink_lf/test/flink/1.9/flink/fs_checkpoint_dir");
	}

	@Test
	public void testHiddenKey() {
		assertTrue(GlobalConfiguration.isSensitive("password123"));
		assertTrue(GlobalConfiguration.isSensitive("123pasSword"));
		assertTrue(GlobalConfiguration.isSensitive("PasSword"));
		assertTrue(GlobalConfiguration.isSensitive("Secret"));
		assertFalse(GlobalConfiguration.isSensitive("Hello"));
	}
}
