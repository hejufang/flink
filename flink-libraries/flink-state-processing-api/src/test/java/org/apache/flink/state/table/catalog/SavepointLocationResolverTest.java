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

package org.apache.flink.state.table.catalog;

import org.apache.flink.core.fs.Path;
import org.apache.flink.state.table.catalog.resolver.SavepointLocationResolver;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.io.IOException;

/**
 * Test for savepoint location resolver, mainly for savepoint path backward compatibility.
 */
public class SavepointLocationResolverTest {
	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	private SavepointLocationResolver resolver;
	private String jobName = "jobName";
	private String namespace1 = "ns1";
	private String namespace2 = "ns2";
	private Path savepointJobNamePath;
	private String savepointId = "savepoint-1313cc-27346252ff76";

	@Before
	public void setup() throws IOException {
		this.resolver = new SavepointLocationResolver();
		this.savepointJobNamePath = new Path(tmp.newFolder(jobName).toURI());
		tmp.newFolder(jobName, namespace1, "savepoint-1313cc-091a46e28080");
		tmp.newFolder(jobName, namespace1, "savepoint-1313cc-091a46e28081");
		tmp.newFolder(jobName, namespace1, "savepoint-1313cc-091a46e28082");
		tmp.newFolder(jobName, namespace2, "savepoint-1313cc-4494f00c85c4");
		tmp.newFolder(jobName, namespace2, "savepoint-1313cc-4494f00c85c5");
		tmp.newFolder(jobName, "savepoint-1313cc-27364910ff32");
		tmp.newFolder(jobName, "savepoint-1313cc-27364910ff33");
		tmp.newFolder(jobName, "savepoint-1313cc-27364910ff34");
		tmp.newFolder(jobName, "savepoint-1313cc-27364910ff35");
	}

	@Test
	public void testOldSavepointPath() throws IOException {
		Path expectedSavepointPath = new Path(tmp.newFolder(jobName, savepointId).toURI());
		Path foundSavepointPath = resolver.findEffectiveSavepointPath(savepointJobNamePath, savepointId);
		Assert.assertEquals(expectedSavepointPath.getPath(), foundSavepointPath.getPath() + "/");
	}

	@Test
	public void testNewSavepointPath() throws IOException {
		Path expectedSavepointPath = new Path(tmp.newFolder(jobName, namespace2, savepointId).toURI());
		Path foundSavepointPath = resolver.findEffectiveSavepointPath(savepointJobNamePath, savepointId);
		Assert.assertEquals(expectedSavepointPath.getPath(), foundSavepointPath.getPath() + "/");
	}

	@Test
	public void testNotExistSavepointPath() throws IOException {
		Path foundSavepointPath = resolver.findEffectiveSavepointPath(savepointJobNamePath, savepointId);
		Assert.assertEquals(foundSavepointPath, null);
	}
}
