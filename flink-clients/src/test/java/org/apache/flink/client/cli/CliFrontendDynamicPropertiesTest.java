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

import org.apache.flink.api.common.JobID;
import org.apache.flink.client.program.PackagedProgram;
import org.apache.flink.configuration.CheckpointingOptions;
import org.apache.flink.configuration.ConfigConstants;
import org.apache.flink.configuration.Configuration;
import org.apache.flink.configuration.CoreOptions;
import org.apache.flink.configuration.PipelineOptions;
import org.apache.flink.core.fs.FileSystem;
import org.apache.flink.core.fs.Path;
import org.apache.flink.runtime.checkpoint.Checkpoints;
import org.apache.flink.runtime.checkpoint.metadata.CheckpointMetadata;
import org.apache.flink.runtime.execution.librarycache.FlinkUserCodeClassLoaders.ParentFirstClassLoader;
import org.apache.flink.runtime.metrics.MetricRegistryImpl;
import org.apache.flink.runtime.state.CheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.CheckpointStorage;
import org.apache.flink.runtime.state.CheckpointStorageLocation;
import org.apache.flink.runtime.state.StateBackend;
import org.apache.flink.runtime.state.filesystem.FsCheckpointMetadataOutputStream;
import org.apache.flink.runtime.state.filesystem.FsCheckpointStorage;
import org.apache.flink.util.ChildFirstClassLoader;

import org.apache.flink.shaded.org.apache.commons.cli.Options;

import org.junit.After;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TemporaryFolder;

import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.UUID;
import java.util.stream.Collectors;

import static org.apache.flink.client.cli.CliFrontendTestUtils.TEST_JAR_MAIN_CLASS;
import static org.apache.flink.client.cli.CliFrontendTestUtils.getTestJarPath;
import static org.apache.flink.runtime.state.filesystem.AbstractFsCheckpointStorage.METADATA_FILE_NAME;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

/**
 * Tests for the RUN command with Dynamic Properties.
 */
public class CliFrontendDynamicPropertiesTest extends CliFrontendTestBase {

	private GenericCLI cliUnderTest;
	private Configuration configuration;

	@Rule
	public TemporaryFolder tmp = new TemporaryFolder();

	@BeforeClass
	public static void init() {
		CliFrontendTestUtils.pipeSystemOutToNull();
	}

	@AfterClass
	public static void shutdown() {
		CliFrontendTestUtils.restoreSystemOut();
	}

	@Before
	public void setup() {
		Options testOptions = new Options();
		configuration = new Configuration();

		cliUnderTest = new GenericCLI(
			configuration,
			tmp.getRoot().getAbsolutePath());

		cliUnderTest.addGeneralOptions(testOptions);
	}

	@After
	public void cleanup() {
		System.clearProperty(ConfigConstants.JOB_NAME_KEY);
	}

	@Test
	public void testRestoreFromSavepoint() throws Exception {
		final String jobName = "testRestoreFromSavepoint";
		final String jobUID = jobName;
		final String namespace = "testNS";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-e", "test-executor",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());

		// verify the sp-1 exists
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobUID), namespace)).length);
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobUID), namespace), "sp-1")));
	}

	@Test(expected = IllegalStateException.class)
	public void testRestoreFromSavepointOnExistingNamespace() throws Exception {
		final String jobName = "testRestoreFromSavepointOnExistingNamespace";
		final String jobUID = jobName;
		final String namespace = "testNS";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE.key(), namespace);
		configuration.setString(CheckpointingOptions.STATE_BACKEND.key(), "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), "file://" + checkpointFolder);

		// create a savepoint here
		final ClassLoader classLoader = Thread.currentThread().getContextClassLoader();
		StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, classLoader, null);
		CheckpointStorage checkpointStorage = stateBackend.createCheckpointStorage(JobID.generate(), jobUID);
		CheckpointStorageLocation savepointMetaInCheckpointDirLocation = checkpointStorage.initializeLocationForSavepointMetaInCheckpointDir(1L);

		try (CheckpointMetadataOutputStream out = savepointMetaInCheckpointDirLocation.createMetadataOutputStream()) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-e", "test-executor",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
	}

//	@Test
//	public void testRestoreFromSavepoint() throws Exception {
//		final String jobName = "testRestoreFromSavepoint";
//		final String namespace = "testNS";
//		System.setProperty(ConfigConstants.JOB_NAME_KEY, jobName);
//
//		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
//		final String zkHAfolder = tmp.newFolder().getAbsolutePath();
//		FileSystem fs = new Path(checkpointFolder).getFileSystem();
//
//		configuration.setString(HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key(), zooKeeperResource.getConnectString());
//		configuration.setString(HighAvailabilityOptions.HA_STORAGE_PATH.key(), zkHAfolder);
//		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE.key(), namespace);
//
//		// create zk node and namespace directory
//		try (CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration)) {
//			CuratorFramework newClient = client.usingNamespace(ZooKeeperUtils.ensureNamespace(client, ZooKeeperUtils.generateCheckpointsPath(configuration, jobName)));
//			newClient.create().creatingParentsIfNeeded()
//				.withMode(CreateMode.PERSISTENT).forPath("/test");
//		}
//		fs.create(new Path(new Path(checkpointFolder, jobName), namespace), true);
//
//		// make sure zk node and namespace directory exist
//		try (CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration)) {
//			CuratorFramework newClient = client.usingNamespace(ZooKeeperUtils.ensureNamespace(client, ZooKeeperUtils.generateCheckpointsPath(configuration, jobName)));
//			Assert.assertNotNull(newClient.checkExists().forPath("/test"));
//		}
//		Assert.assertTrue(fs.exists(new Path(new Path(checkpointFolder, jobName), namespace)));
//
//		String[] args = {
//			"-cn", "test",
//			"-e", "test-executor",
//			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=/tmp/savepoint",
//			"-D" + HighAvailabilityOptions.HA_ZOOKEEPER_QUORUM.key() + "=" + zooKeeperResource.getConnectString(),
//			"-D" + HighAvailabilityOptions.HA_STORAGE_PATH.key() + "=" + zkHAfolder,
//			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
//			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
//			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
//			"-Dclassloader.resolve-order=parent-first",
//			"-Dmetrics.reporter.opentsdb_reporter.jobname=test-job-name",
//			"-DclusterName=flink",
//			getTestJarPath()};
//
//		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
//
//		// make sure the zk is empty and the directory is renamed
//		try (CuratorFramework client = ZooKeeperUtils.startCuratorFramework(configuration)) {
//			CuratorFramework newClient = client.usingNamespace(ZooKeeperUtils.ensureNamespace(client, ZooKeeperUtils.generateCheckpointsPath(configuration, jobName)));
//			Assert.assertNull(newClient.checkExists().forPath("/test"));
//		}
//
//		FileStatus[] statuses = fs.listStatus(new Path(checkpointFolder, jobName));
//		Assert.assertEquals(1, statuses.length);
//		Assert.assertNotEquals(namespace, statuses[0].getPath().getName());
//	}

	@Test
	public void testRestoreFromSavepointWithApplicationMode() throws Exception {
		final String jobName = "testRestoreFromSavepointWithApplicationMode";
		final String jobUID = jobName;
		final String namespace = "testNS";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			verifyCliFrontendWithApplicationMode(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}

		// verify the sp-1 exists
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobUID), namespace)).length);
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobUID), namespace), "sp-1")));
	}

	@Test
	public void testMutipleExternalFileWithApplicationMode() throws Exception {
		final String jobName = "testRestoreFromSavepointWithApplicationMode";
		final String namespace = "testNS";
		System.setProperty(ConfigConstants.JOB_NAME_KEY, jobName);

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();

		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE.key(), namespace);
		configuration.setString(CheckpointingOptions.STATE_BACKEND.key(), "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY.key(), "file://" + checkpointFolder);

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.EXTERNAL_RESOURCES.key() + "=" + "file://file1.jar;file://file2.jar;file://file3.jar",
			"-D" + PipelineOptions.UPLOAD_REMOTE_DIR.key() + "=" + "hdfs:///tmp",
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			"local:///job.jar"};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			// just check whether there will be any exception.
			verifyCliFrontendWithApplicationMode(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}
	}

	@Test
	public void testDynamicPropertiesWithParentFirstClassloader() throws Exception {
		String[] args = {
			"-cn", "flink",
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-Dclassloader.resolve-order=parent-first",
			"-Dmetrics.reporter.opentsdb_reporter.jobname=test-job-name",
			"-DclusterName=flink",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
	}

	@Test
	public void testDynamicPropertiesWithDefaultChildFirstClassloader() throws Exception {

		String[] args = {
			"-cn", "flink",
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-Dmetrics.reporter.opentsdb_reporter.jobname=test-job-name",
			"-DclusterName=flink",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest, "child-first", ChildFirstClassLoader.class.getName());
	}

	@Test
	public void testDynamicPropertiesWithChildFirstClassloader() throws Exception {

		String[] args = {
			"-cn", "flink",
			"-e", "test-executor",
			"-D" + CoreOptions.DEFAULT_PARALLELISM.key() + "=5",
			"-Dclassloader.resolve-order=child-first",
			"-Dmetrics.reporter.opentsdb_reporter.jobname=test-job-name",
			"-DclusterName=flink",
			getTestJarPath(), "-a", "--debug", "true", "arg1", "arg2"
		};

		verifyCliFrontend(configuration, args, cliUnderTest, "child-first", ChildFirstClassLoader.class.getName());
	}

	@Test
	public void testRestoreFromSavepointWithInconsistentJobUIDAndJobName() throws Exception {
		final String jobUID = "juid";
		final String jobName = "jName";
		final String namespace = "ns";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + PipelineOptions.JOB_UID.key() + "=" + jobUID,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		// verify there are 3 status in jobUID folder
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobUID), namespace)).length);
		// verify the sp-1 exists
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobUID), namespace), "sp-1")));
		// verify the jobName folder doesn't exists when the inconsistent jobUID was set
		Assert.assertFalse(fs.exists(new Path(checkpointFolder, jobName)));
	}

	@Test
	public void testRestoreFromSavepointWithOnlyJobNameSet() throws Exception {
		final String jobName = "jName";
		final String namespace = "ns";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		// verify there are 3 status in jobUID folder
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobName), namespace)).length);
		// verify the sp-1 exists
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobName), namespace), "sp-1")));
	}

	@Test
	public void testInconsistentJobUIDAndJobNameInApplicationMode() throws Exception {
		final String jobUID = "juid";
		final String jobName = "jName";
		final String namespace = "ns";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + PipelineOptions.JOB_UID.key() + "=" + jobUID,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			verifyCliFrontendWithApplicationMode(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		// verify there are 3 status in jobUID folder
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobUID), namespace)).length);
		// verify the sp-1 exists
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobUID), namespace), "sp-1")));
		// verify the jobName folder doesn't exists when the inconsistent jobUID was set
		Assert.assertFalse(fs.exists(new Path(checkpointFolder, jobName)));
	}

	@Test
	public void testOnlyJobNameSetInApplicationMode() throws Exception {
		final String jobName = "jName";
		final String namespace = "ns";

		final String checkpointFolder = tmp.newFolder().getAbsolutePath();
		final String savepointPath = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepointPath).getFileSystem(),
			new Path(savepointPath, METADATA_FILE_NAME),
			new Path(savepointPath))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}

		String[] args = {
			"-cn", "test",
			"-t", "remote",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=" + "file://" + savepointPath,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointFolder,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + namespace,
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		final String errorMsg = "Application Mode not supported by standalone deployments.";

		try {
			verifyCliFrontendWithApplicationMode(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		} catch (Exception e) {
			//expected
			assertTrue(e instanceof UnsupportedOperationException);
			assertEquals(errorMsg, e.getMessage());
		}
		FileSystem fs = new Path(checkpointFolder).getFileSystem();
		// verify there are 3 status in jobUID folder
		Assert.assertEquals(3, fs.listStatus(new Path(new Path(checkpointFolder, jobName), namespace)).length);
		// verify the sp-1 exists
		Assert.assertTrue(fs.exists(new Path(new Path(new Path(checkpointFolder, jobName), namespace), "sp-1")));
	}

	@Test
	public void testRestoreFromSavepointWithNewSnapshotNamespace() throws Exception {
		String snapshotNamespace = "snapshot_namespace";
		String checkpointNamespace = "checkpoint_namespace";
		String jobName = "jname";
		String jobUID = jobName;

		String checkpointDir = tmp.newFolder().getAbsolutePath();
		String savepoint = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();
		try (CheckpointMetadataOutputStream out = new FsCheckpointMetadataOutputStream(
			new Path(savepoint).getFileSystem(),
			new Path(savepoint, METADATA_FILE_NAME),
			new Path(savepoint))) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}
		String[] args = {
			"-cn", "test",
			"-e", "test-executor",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.SNAPSHOT_NAMESPACE.key() + "=" + snapshotNamespace,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + checkpointNamespace,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=file://" + savepoint,
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointDir,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-Dclassloader.resolve-order=parent-first",
			"-DclusterName=flink",
			getTestJarPath()};

		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		// Savepoint metadata is copied to checkpoint dir with folder-name sp-1
		assertTrue(Files.exists(Paths.get(checkpointDir, jobUID, snapshotNamespace, "sp-1", METADATA_FILE_NAME)));
	}

	@Test
	public void testRestoreFromSavepointWithExistingSnapshotNamespace() throws Exception {
		String snapshotNamespace = "snapshot_namespace";
		String checkpointNamespace = "checkpoint_namespace";
		String jobName = "testRestoreFromSavepointWithExistingSnapshotNamespace";
		String jobUID = jobName;

		String checkpointDir = tmp.newFolder().getAbsolutePath();
		String savepoint = tmp.newFolder().getAbsolutePath() + "/" + UUID.randomUUID();

		configuration.setString(CheckpointingOptions.STATE_BACKEND, "filesystem");
		configuration.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, Paths.get(checkpointDir).toUri().toString());
		configuration.setString(CheckpointingOptions.SNAPSHOT_NAMESPACE, snapshotNamespace);
		configuration.setString(CheckpointingOptions.CHECKPOINTS_NAMESPACE, snapshotNamespace);

		StateBackend stateBackend = Checkpoints.loadStateBackend(configuration, Thread.currentThread().getContextClassLoader(), null);
		FsCheckpointStorage storage = (FsCheckpointStorage) stateBackend.createCheckpointStorage(new JobID(), jobUID);
		CheckpointStorageLocation location = storage.initializeLocationForCheckpoint(1L);

		try (CheckpointMetadataOutputStream out = location.createMetadataOutputStream()) {
			Checkpoints.storeCheckpointMetadata(new CheckpointMetadata(1L, Collections.emptyList(), Collections.emptyList()), out);
			out.closeAndFinalizeCheckpoint();
		}
		String[] args = new String[]{
			"-cn", "test",
			"-t", "remote",
			"-DclusterName=flink",
			"-Dclassloader.resolve-order=parent-first",
			"-D" + PipelineOptions.NAME.key() + "=" + jobName,
			"-D" + CheckpointingOptions.STATE_BACKEND.key() + "=filesystem",
			"-D" + CheckpointingOptions.CHECKPOINTS_DIRECTORY.key() + "=file://" + checkpointDir,
			"-D" + CheckpointingOptions.CHECKPOINTS_NAMESPACE.key() + "=" + checkpointNamespace,
			"-D" + CheckpointingOptions.SNAPSHOT_NAMESPACE.key() + "=" + snapshotNamespace,
			"-D" + CheckpointingOptions.RESTORE_SAVEPOINT_PATH.key() + "=file://" + savepoint,
			getTestJarPath()
		};
		verifyCliFrontend(configuration, args, cliUnderTest, "parent-first", ParentFirstClassLoader.class.getName());
		// No savepoint will be copied to checkpoint dir, and client didn't throw IllegalStateException
		assertTrue(Files.notExists(Paths.get(checkpointDir, jobUID, snapshotNamespace, "sp-1")));
		assertTrue(Files.exists(Paths.get(checkpointDir, jobUID, snapshotNamespace, "chk-1", METADATA_FILE_NAME)));
		assertEquals(3, Files.list(Paths.get(checkpointDir, jobUID, snapshotNamespace)).collect(Collectors.toList()).size());
	}

	// --------------------------------------------------------------------------------------------

	public static void verifyCliFrontend(
			Configuration configuration,
			String[] parameters,
			GenericCLI cliUnderTest,
			String expectedResolveOrderOption,
			String userCodeClassLoaderClassName) throws Exception {
		TestingCliFrontend testFrontend =
			new TestingCliFrontend(configuration, cliUnderTest, expectedResolveOrderOption, userCodeClassLoaderClassName);
		testFrontend.run(parameters); // verifies the expected values (see below)
	}

	public static void verifyCliFrontendWithApplicationMode(
		Configuration configuration,
		String[] parameters,
		GenericCLI cliUnderTest,
		String expectedResolveOrderOption,
		String userCodeClassLoaderClassName) throws Exception {
		TestingCliFrontend testFrontend =
			new TestingCliFrontend(configuration, cliUnderTest, expectedResolveOrderOption, userCodeClassLoaderClassName);
		testFrontend.runApplication(parameters); // verifies the expected values (see below)
	}

	private static final class TestingCliFrontend extends CliFrontend {

		private final String expectedResolveOrder;

		private final String userCodeClassLoaderClassName;

		private TestingCliFrontend(
				Configuration configuration,
				GenericCLI cliUnderTest,
				String expectedResolveOrderOption,
				String userCodeClassLoaderClassName) {
			super(
				configuration,
				Collections.singletonList(cliUnderTest));
			this.expectedResolveOrder = expectedResolveOrderOption;
			this.userCodeClassLoaderClassName = userCodeClassLoaderClassName;
		}

		@Override
		protected void executeProgram(Configuration configuration, PackagedProgram program) {
			assertEquals(TEST_JAR_MAIN_CLASS, program.getMainClassName());
			assertEquals(expectedResolveOrder, configuration.get(CoreOptions.CLASSLOADER_RESOLVE_ORDER));
			assertEquals(userCodeClassLoaderClassName, program.getUserCodeClassLoader().getClass().getName());
		}

		@Override
		public MetricRegistryImpl createMetricRegistry(Configuration config) {
			return null;
		}
	}
}
