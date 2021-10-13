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

package org.apache.flink.client.cli;

import org.apache.flink.client.deployment.application.ApplicationConfiguration;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.GlobalConfiguration;
import org.apache.flink.runtime.clusterframework.BootstrapTools;

import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;
import org.junit.runner.RunWith;
import org.junit.runners.JUnit4;

import java.io.File;
import java.io.IOException;

import static org.junit.Assert.assertArrayEquals;
import static org.junit.Assert.assertEquals;

/**
 * unit test for Application Configuration.
 */
@RunWith(JUnit4.class)
public class ApplicationConfigurationTest {
	@Rule
	public TemporaryFolder tempFolder = new TemporaryFolder();

	@Test
	public void testGetApplicationConfiguration() throws IOException {
		Configuration configuration = new Configuration();
		String[] originArgs = new String[]{"--foo", "bar", "--json_conf", "{\"foo\": \"bar\", \"list\": [1,2,\"3\"], \"map\": {\"inner_foo\": \"inner_bar\"}}"};
		String className = "com.foo.bar";
		ApplicationConfiguration applicationConfiguration = new ApplicationConfiguration(originArgs, className);
		applicationConfiguration.applyToConfiguration(configuration);

		File tmpDir = tempFolder.getRoot();
		File confFile = new File(tmpDir, GlobalConfiguration.FLINK_CONF_FILENAME);
		try {
			BootstrapTools.writeConfiguration(configuration, confFile);
			Configuration newConfig = GlobalConfiguration.loadConfiguration(tmpDir.getAbsolutePath());
			ApplicationConfiguration applicationConfiguration1 = ApplicationConfiguration.fromConfiguration(newConfig);
			assertEquals(applicationConfiguration1.getApplicationClassName(), applicationConfiguration.getApplicationClassName());
			assertArrayEquals(applicationConfiguration1.getProgramArguments(), applicationConfiguration.getProgramArguments());
		} finally {
			confFile.delete();
			tmpDir.delete();
		}
	}
}
