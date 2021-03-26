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

package org.apache.flink.connector.doris;

import org.apache.flink.util.Preconditions;

import com.bytedance.inf.compute.hsap.doris.DataFormat;
import com.bytedance.inf.compute.hsap.doris.TableModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;

import static org.apache.flink.connector.doris.Constant.COLUMN_SEPARATOR_DEFAULT;
import static org.apache.flink.connector.doris.Constant.DATA_FORMAT_DEFAULT;
import static org.apache.flink.connector.doris.Constant.FE_UPDATE_INTERVAL_MS_DEFAULT;
import static org.apache.flink.connector.doris.Constant.MAX_BYTES_PER_BATCH_DEFAULT;
import static org.apache.flink.connector.doris.Constant.MAX_FILTER_RATIO_DEFAULT;
import static org.apache.flink.connector.doris.Constant.MAX_PENDING_BATCH_NUM_DEFAULT;
import static org.apache.flink.connector.doris.Constant.MAX_PENDING_TIME_MS_DEFAULT;
import static org.apache.flink.connector.doris.Constant.MAX_RETRY_NUM_DEFAULT;
import static org.apache.flink.connector.doris.Constant.RETRY_INTERVAL_MS_DEFAULT;
import static org.apache.flink.connector.doris.Constant.TABLE_MODEL_DEFAULT;
import static org.apache.flink.connector.doris.Constant.TIMEOUT_MS_DEFAULT;

/**
 * DorisOptions.
 */
public class DorisOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	private final List<Pair<String, Integer>> dorisFEList;
	private final String cluster;
	private final String dataCenter;
	private final String dorisFEPsm;
	private final String user;
	private final String password;
	private final String dbName;
	private final String tableName;
	private final String[] columns;
	private final String[] keys;
	private final TableModel tableModel;
	private final DataFormat dataFormat;
	private final String columnSeparator;
	private final int maxBytesPerBatch;
	private final int maxPendingBatchNum;
	private final int maxPendingTimeMs;
	private final float maxFilterRatio;
	private final int retryIntervalMs;
	private final int maxRetryNum;
	private final int feUpdateIntervalMs;
	private final int parallelism;
	private final String sequenceColumn;
	private final int timeoutMs;

	private DorisOptions(
			List<Pair<String, Integer>> dorisFEList,
			String cluster,
			String dataCenter,
			String dorisFEPsm,
			String user,
			String password,
			String dbName,
			String tableName,
			String[] columns,
			String[] keys,
			TableModel tableModel,
			DataFormat dataFormat,
			String columnSeparator,
			int maxBytesPerBatch,
			int maxPendingBatchNum,
			int maxPendingTimeMs,
			float maxFilterRatio,
			int retryIntervalMs,
			int maxRetryNum,
			int feUpdateIntervalMs,
			int parallelism,
			String sequenceColumn,
			int timeoutMs) {
		this.dorisFEList = dorisFEList;
		this.cluster = cluster;
		this.dataCenter = dataCenter;
		this.dorisFEPsm = dorisFEPsm;
		this.user = user;
		this.password = password;
		this.dbName = dbName;
		this.tableName = tableName;
		this.columns = columns;
		this.keys = keys;
		this.tableModel = tableModel;
		this.dataFormat = dataFormat;
		this.columnSeparator = columnSeparator;
		this.maxBytesPerBatch = maxBytesPerBatch;
		this.maxPendingBatchNum = maxPendingBatchNum;
		this.maxPendingTimeMs = maxPendingTimeMs;
		this.maxFilterRatio = maxFilterRatio;
		this.retryIntervalMs = retryIntervalMs;
		this.maxRetryNum = maxRetryNum;
		this.feUpdateIntervalMs = feUpdateIntervalMs;
		this.parallelism = parallelism;
		this.sequenceColumn = sequenceColumn;
		this.timeoutMs = timeoutMs;
	}

	public List<Pair<String, Integer>> getDorisFEList() {
		return dorisFEList;
	}

	public String getCluster() {
		return cluster;
	}

	public String getDataCenter() {
		return dataCenter;
	}

	public String getDorisFEPsm() {
		return dorisFEPsm;
	}

	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}

	public String getDbname() {
		return dbName;
	}

	public String getTableName() {
		return tableName;
	}

	public String[] getColumns() {
		return columns;
	}

	public String[] getKeys() {
		return keys;
	}

	public TableModel getTableModel() {
		return tableModel;
	}

	public DataFormat getDataFormat() {
		return dataFormat;
	}

	public String getColumnSeparator() {
		return columnSeparator;
	}

	public int getMaxBytesPerBatch() {
		return maxBytesPerBatch;
	}

	public int getMaxPendingBatchNum() {
		return maxPendingBatchNum;
	}

	public int getMaxPendingTimeMs() {
		return maxPendingTimeMs;
	}

	public float getMaxFilterRatio() {
		return maxFilterRatio;
	}

	public int getRetryIntervalMs() {
		return retryIntervalMs;
	}

	public int getMaxRetryNum() {
		return maxRetryNum;
	}

	public int getFeUpdateIntervalMs() {
		return feUpdateIntervalMs;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String getSequenceColumn() {
		return sequenceColumn;
	}

	public int getTimeoutMs() {
		return timeoutMs;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link DorisOptions}.
	 */
	public static class Builder {

		private List<Pair<String, Integer>> dorisFEList;
		private String cluster;
		private String dataCenter;
		private String dorisFEPsm;
		private String user;
		private String password;
		private String dbName;
		private String tableName;
		private String[] columns;
		private String[] keys;
		private TableModel tableModel = TABLE_MODEL_DEFAULT;
		private DataFormat dataFormat = DATA_FORMAT_DEFAULT;
		private String columnSeparator = COLUMN_SEPARATOR_DEFAULT;
		private int maxBytesPerBatch = MAX_BYTES_PER_BATCH_DEFAULT;
		private int maxPendingBatchNum = MAX_PENDING_BATCH_NUM_DEFAULT;
		private int maxPendingTimeMs = MAX_PENDING_TIME_MS_DEFAULT;
		private float maxFilterRatio = MAX_FILTER_RATIO_DEFAULT;
		private int retryIntervalMs = RETRY_INTERVAL_MS_DEFAULT;
		private int maxRetryNum = MAX_RETRY_NUM_DEFAULT;
		private int feUpdateIntervalMs = FE_UPDATE_INTERVAL_MS_DEFAULT;
		private int parallelism = 1;
		private String sequenceColumn;
		private int timeoutMs = TIMEOUT_MS_DEFAULT;

		private Builder() {
		}

		public Builder setDorisFEList(List<Pair<String, Integer>> dorisFEList) {
			this.dorisFEList = dorisFEList;
			return this;
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setDataCenter(String dataCenter) {
			this.dataCenter = dataCenter;
			return this;
		}

		public Builder setDorisFEPsm(String dorisFEPsm) {
			this.dorisFEPsm = dorisFEPsm;
			return this;
		}

		public Builder setUser(String user) {
			this.user = user;
			return this;
		}

		public Builder setPassword(String password) {
			this.password = password;
			return this;
		}

		public Builder setDbname(String dbName) {
			this.dbName = dbName;
			return this;
		}

		public Builder setTableName(String tableName) {
			this.tableName = tableName;
			return this;
		}

		public Builder setColumns(String[] columns) {
			this.columns = columns;
			return this;
		}

		public Builder setKeys(String[] keys) {
			this.keys = keys;
			return this;
		}

		public Builder setTableModel(TableModel tableModel) {
			this.tableModel = tableModel;
			return this;
		}

		public Builder setDataFormat(DataFormat dataFormat) {
			this.dataFormat = dataFormat;
			return this;
		}

		public Builder setColumnSeparator(String columnSeparator) {
			this.columnSeparator = columnSeparator;
			return this;
		}

		public Builder setMaxBytesPerBatch(int maxBytesPerBatch) {
			this.maxBytesPerBatch = maxBytesPerBatch;
			return this;
		}

		public Builder setMaxPendingBatchNum(int maxPendingBatchNum) {
			this.maxPendingBatchNum = maxPendingBatchNum;
			return this;
		}

		public Builder setMaxPendingTimeMs(int maxPendingTimeMs) {
			this.maxPendingTimeMs = maxPendingTimeMs;
			return this;
		}

		public Builder setMaxFilterRatio(float maxFilterRatio) {
			this.maxFilterRatio = maxFilterRatio;
			return this;
		}

		public Builder setRetryIntervalMs(int retryIntervalMs) {
			this.retryIntervalMs = retryIntervalMs;
			return this;
		}

		public Builder setMaxRetryNum(int maxRetryNum) {
			this.maxRetryNum = maxRetryNum;
			return this;
		}

		public Builder setFeUpdateIntervalMs(int feUpdateIntervalMs) {
			this.feUpdateIntervalMs = feUpdateIntervalMs;
			return this;
		}

		public Builder setParallelism(int parallelism) {
			this.parallelism = parallelism;
			return this;
		}

		public Builder setSequenceColumn(String sequenceColumn) {
			this.sequenceColumn = sequenceColumn;
			return this;
		}

		public Builder setTimeoutMs(int timeoutMs) {
			this.timeoutMs = timeoutMs;
			return this;
		}

		public DorisOptions build() {
			// There are two ways to get connection, through DORIS_FE_LIST or DORIS_FE_PSM + DATA_CENTER,
			// and the second way has higher priority if they are both set.
			Preconditions.checkArgument((dorisFEList != null && dorisFEList.size() != 0) ||
				(!StringUtils.isEmpty(dorisFEPsm) && !StringUtils.isEmpty(dataCenter)),
				"You must set either 'doris-fe-list' or 'doris-fe-psm' and 'data-center', please check your params");
			Preconditions.checkNotNull(cluster, "cluster can not be null");
			Preconditions.checkNotNull(user, "user can not be null");
			Preconditions.checkNotNull(password, "password can not be null");
			Preconditions.checkNotNull(dbName, "db-name can not be null");
			Preconditions.checkNotNull(tableName, "table-name can not be null");
			Preconditions.checkNotNull(columns, "columns can not be null");
			Preconditions.checkNotNull(keys, "keys can not be null");

			Preconditions.checkArgument(maxBytesPerBatch > 0, "maxBytesPerBatch must > 0");
			Preconditions.checkArgument(maxPendingBatchNum > 0, "maxPendingBatchNum must > 0");
			Preconditions.checkArgument(maxPendingTimeMs > 0, "maxPendingTimeMs must > 0");
			Preconditions.checkArgument(maxFilterRatio >= 0, "maxFilterRatio must >= 0");
			Preconditions.checkArgument(retryIntervalMs > 0, "retryIntervalMs must > 0");
			Preconditions.checkArgument(feUpdateIntervalMs > 0, "feUpdateIntervalMs must > 0");
			Preconditions.checkArgument(timeoutMs > 0, "timeoutMs must > 0");

			return new DorisOptions(
				dorisFEList,
				cluster,
				dataCenter,
				dorisFEPsm,
				user,
				password,
				dbName,
				tableName,
				columns,
				keys,
				tableModel,
				dataFormat,
				columnSeparator,
				maxBytesPerBatch,
				maxPendingBatchNum,
				maxPendingTimeMs,
				maxFilterRatio,
				retryIntervalMs,
				maxRetryNum,
				feUpdateIntervalMs,
				parallelism,
				sequenceColumn,
				timeoutMs);
		}
	}
}
