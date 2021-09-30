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

import org.apache.flink.configuration.Configuration;

import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.io.IOException;
import java.util.Collections;

import static org.junit.Assert.assertEquals;

/**
 * test download options for CliFronted.
 */
public class CliFrontedDownloadTest extends CliFrontendTestBase {

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Test(expected = IOException.class)
	public void testDownload() throws Exception {
		// test configure parallelism with overflow integer value
		String[] parameters = {
			"-src",
			"hdfs:///file1;hdfs:///file2",
			"-dest",
			"/opt/tiger/workdir"
		};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		try {
			testFrontend.download(parameters);
		} catch (IOException e) {
			assertEquals("Hadoop is not in the classpath/dependencies.", e.getCause().getMessage());
			throw new IOException();
		}
	}

	@Test(expected = CliArgsException.class)
	public void testDownloadSavepath() throws Exception {
		// test configure parallelism with overflow integer value
		String[] parameters = {
			"-src",
			"hdfs:///file1;hdfs:///file2",
			"-dest",
			"/opt/tiger/workdir/"
		};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		testFrontend.download(parameters);
	}

	@Test(expected = CliArgsException.class)
	public void testNoSavepath() throws Exception {
		// test configure parallelism with overflow integer value
		String[] parameters = {
			"-src",
			"hdfs:///file1;hdfs:///file2",
			"-dest"
		};
		Configuration configuration = new Configuration();
		CliFrontend testFrontend = new CliFrontend(
			configuration,
			Collections.singletonList(getCli(configuration)));
		testFrontend.download(parameters);
	}
}
