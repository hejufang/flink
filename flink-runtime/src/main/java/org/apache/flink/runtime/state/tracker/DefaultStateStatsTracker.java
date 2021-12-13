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

package org.apache.flink.runtime.state.tracker;

import org.apache.flink.metrics.Message;
import org.apache.flink.metrics.MessageSet;
import org.apache.flink.metrics.MessageType;
import org.apache.flink.metrics.MetricGroup;
import org.apache.flink.metrics.TagGauge;
import org.apache.flink.metrics.TagGaugeStore;
import org.apache.flink.runtime.checkpoint.WarehouseRestoreMessage;
import org.apache.flink.runtime.checkpoint.WarehouseSnapshotMessage;

import java.util.concurrent.atomic.AtomicInteger;

/**
 * StateStatsTracker implemented by default.
 * The snapshot and restore metrics will be reported to the warehouse and opentsdb.
 */
public class DefaultStateStatsTracker implements StateStatsTracker {
	// warehouse
	private static final String WAREHOUSE_STATE_BACKEND_SNAPSHOTS = "warehouseSnapshotsStats";
	private static final String WAREHOUSE_STATE_BACKEND_RESTORES = "warehouseRestoresStats";
	private static final String WAREHOUSE_STATE_BACKEND_STATE_FILE_BATCHING = "warehouseStateBatchingStats";

	// snapshot metrics
	private static final String STATE_SNAPSHOT_DURATION = "syncDuration";
	private static final String STATE_UPLOAD_DURATION = "uploadDuration";
	private static final String STATE_UPLOAD_SIZE = "uploadSizeInBytes";
	private static final String CHECKPOINT_HDFS_RETRY_COUNT = "numberOfHdfsRetries";

	// restore metrics
	private static final String STATE_RECOVER_DURATION = "stateRecoverTime";
	private static final String STATE_DOWNLOAD_DURATION = "downloadDuration";
	private static final String STATE_WRITE_KEY_DURATION = "writeKeyDuration";
	private static final String STATE_DOWNLOAD_SIZE = "downloadSizeInBytes";
	private static final String NUMBER_OF_RECOVER_SUCCESS = "numberOfRecoverSuccess";
	private static final String NUMBER_OF_RECOVER_FAILED = "numberOfRecoverFailed";

	// warehouse
	private static final MessageSet<WarehouseSnapshotMessage> snapshotMessageSet = new MessageSet<>(MessageType.SNAPSHOT);
	private static final MessageSet<WarehouseRestoreMessage> restoreMessageSet = new MessageSet<>(MessageType.RESTORE);
	private static final MessageSet<WarehouseStateFileBatchingMessage> stateBatchingMessageSet = new MessageSet<>(MessageType.SNAPSHOT);

	// opentsdb
	private final TagGauge syncDurationGauge = createTagGauge();
	private final TagGauge uploadDurationGauge = createTagGauge();
	private final TagGauge uploadSizeGauge = createTagGauge();
	private final TagGauge stateRecoverTimeGauge = createTagGauge();
	private final TagGauge downloadDurationGauge = createTagGauge();
	private final TagGauge writeKeyDurationGauge = createTagGauge();
	private final TagGauge downloadSizeInByGauge = createTagGauge();
	private final TagGauge recoverFailedGauge = createTagGauge();
	private final TagGauge recoverSuccessGauge = createTagGauge();

	// incremental checkpoint specific metrics
	private static final String PRE_RAW_TOTAL_STATE_SIZE = "preRawTotalStateSize";
	private static final String POST_RAW_TOTAL_STATE_SIZE = "postRawTotalStateSize";
	private static final String PRE_SST_FILE_NUM = "preSstFileNum";
	private static final String POST_SST_FILE_NUM = "postSstFileNum";
	private static final String PRE_UPLOAD_FILE_NUM = "preUploadFileNum";
	private static final String POST_UPLOAD_FILE_NUM = "postUploadFileNum";

	private final TagGauge preRawTotalStateSizeGauge = createTagGauge();
	private final TagGauge postRawTotalStateSizeGauge = createTagGauge();
	private final TagGauge preSstFileNumGauge = createTagGauge();
	private final TagGauge postSstFileNumGauge = createTagGauge();
	private final TagGauge preUploadFileNumGauge = createTagGauge();
	private final TagGauge postUploadFileNumGauge = createTagGauge();

	private final AtomicInteger hdfsRetryCounter;

	public DefaultStateStatsTracker(MetricGroup metricGroup) {
		hdfsRetryCounter = new AtomicInteger(0);
		registerMetrics(metricGroup);
	}

	/**
	 * Callback when a state backend snapshot completes.
	 *
	 * @param message Message of snapshot metrics.
	 */
	@Override
	public void reportCompletedSnapshot(WarehouseSnapshotMessage message) {
		snapshotMessageSet.addMessage(new Message<>(message));
		updateSnapshotStatistic(message);
	}

	/**
	 * Callback when a state backend restore completes.
	 *
	 * @param message Message of restore metrics.
	 */
	@Override
	public void reportCompletedRestore(WarehouseRestoreMessage message) {
		restoreMessageSet.addMessage(new Message<>(message));
		updateRecoverStatistic(message);
	}

	@Override
	public void updateRetryCounter(int retryCount) {
		hdfsRetryCounter.getAndAdd(retryCount);
	}

	@Override
	public void reportFailedRestore(WarehouseRestoreMessage message) {
		restoreMessageSet.addMessage(new Message<>(message));
		updateRecoverFailedStatistic(message);
	}

	/**
	 * Register the exposed metrics.
	 *
	 * @param metricGroup Metric group to use for the metrics.
	 */
	private void registerMetrics(MetricGroup metricGroup) {
		// warehouse
		metricGroup.gauge(WAREHOUSE_STATE_BACKEND_SNAPSHOTS, snapshotMessageSet);
		metricGroup.gauge(WAREHOUSE_STATE_BACKEND_RESTORES, restoreMessageSet);
		metricGroup.gauge(WAREHOUSE_STATE_BACKEND_STATE_FILE_BATCHING, stateBatchingMessageSet);

		// snapshot metrics
		metricGroup.gauge(CHECKPOINT_HDFS_RETRY_COUNT, () -> hdfsRetryCounter.getAndSet(0));
		metricGroup.gauge(STATE_SNAPSHOT_DURATION, syncDurationGauge);
		metricGroup.gauge(STATE_UPLOAD_DURATION, uploadDurationGauge);
		metricGroup.gauge(STATE_UPLOAD_SIZE, uploadSizeGauge);

		// restore metrics
		metricGroup.gauge(STATE_RECOVER_DURATION, stateRecoverTimeGauge);
		metricGroup.gauge(STATE_DOWNLOAD_DURATION, downloadDurationGauge);
		metricGroup.gauge(STATE_WRITE_KEY_DURATION, writeKeyDurationGauge);
		metricGroup.gauge(STATE_DOWNLOAD_SIZE, downloadSizeInByGauge);
		metricGroup.gauge(NUMBER_OF_RECOVER_SUCCESS, recoverSuccessGauge);
		metricGroup.gauge(NUMBER_OF_RECOVER_FAILED, recoverFailedGauge);

		// state file batching metrics
		metricGroup.gauge(PRE_RAW_TOTAL_STATE_SIZE, preRawTotalStateSizeGauge);
		metricGroup.gauge(POST_RAW_TOTAL_STATE_SIZE, postRawTotalStateSizeGauge);
		metricGroup.gauge(PRE_SST_FILE_NUM, preSstFileNumGauge);
		metricGroup.gauge(POST_SST_FILE_NUM, postSstFileNumGauge);
		metricGroup.gauge(PRE_UPLOAD_FILE_NUM, preUploadFileNumGauge);
		metricGroup.gauge(POST_UPLOAD_FILE_NUM, postUploadFileNumGauge);
	}

	private void updateSnapshotStatistic(WarehouseSnapshotMessage snapshotMessage) {
		TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
			.addTagValue("backend", snapshotMessage.getBackendType())
			.build();
		syncDurationGauge.addMetric(snapshotMessage.getSyncDuration(), tagValues);
		uploadDurationGauge.addMetric(snapshotMessage.getUploadDuration(), tagValues);
		uploadSizeGauge.addMetric(snapshotMessage.getUploadSizeInBytes(), tagValues);
	}

	private void updateRecoverStatistic(WarehouseRestoreMessage restoreMessage) {
		TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
			.addTagValue("backend", restoreMessage.getBackendType())
			.addTagValue("restoreMode", restoreMessage.getRestoreMode())
			.build();
		stateRecoverTimeGauge.addMetric(restoreMessage.getStateRecoverTime(), tagValues);
		downloadDurationGauge.addMetric(restoreMessage.getDownloadDuration(), tagValues);
		writeKeyDurationGauge.addMetric(restoreMessage.getWriteKeyDuration(), tagValues);
		downloadSizeInByGauge.addMetric(restoreMessage.getDownloadSizeInBytes(), tagValues);
		recoverSuccessGauge.addMetric(1, tagValues);
	}

	public void updateIncrementalBatchingStatistics(WarehouseStateFileBatchingMessage batchingMessage) {
		stateBatchingMessageSet.addMessage(new Message<>(batchingMessage));
		TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder().build();
		preRawTotalStateSizeGauge.addMetric(batchingMessage.getPreRawTotalStateSize(), tagValues);
		postRawTotalStateSizeGauge.addMetric(batchingMessage.getPostRawTotalStateSize(), tagValues);
		preSstFileNumGauge.addMetric(batchingMessage.getPreSstFileNum(), tagValues);
		postSstFileNumGauge.addMetric(batchingMessage.getPostSstFileNum(), tagValues);
		preUploadFileNumGauge.addMetric(batchingMessage.getPreUploadFileNum(), tagValues);
		postUploadFileNumGauge.addMetric(batchingMessage.getPostUploadFileNum(), tagValues);
	}

	private void updateRecoverFailedStatistic(WarehouseRestoreMessage restoreMessage) {
		TagGaugeStore.TagValues tagValues = new TagGaugeStore.TagValuesBuilder()
			.addTagValue("backend", restoreMessage.getBackendType())
			.addTagValue("restoreMode", restoreMessage.getRestoreMode())
			.addTagValue("errMsg", restoreMessage.getErrMsg())
			.build();
		recoverFailedGauge.addMetric(1, tagValues);
	}

	private TagGauge createTagGauge() {
		return new TagGauge.TagGaugeBuilder().setClearAfterReport(true).build();
	}
}
