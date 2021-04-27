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

import org.apache.flink.util.FlinkRuntimeException;
import org.apache.flink.util.Preconditions;

import com.bytedance.inf.compute.hsap.doris.DataFormat;
import com.bytedance.inf.compute.hsap.doris.TableModel;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.util.List;

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
	private final long maxPendingTimeMs;
	private final float maxFilterRatio;
	private final long retryIntervalMs;
	private final int maxRetryNum;
	private final long feUpdateIntervalMs;
	private final int parallelism;
	private final String sequenceColumn;

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
			long maxPendingTimeMs,
			float maxFilterRatio,
			long retryIntervalMs,
			int maxRetryNum,
			long feUpdateIntervalMs,
			int parallelism,
			String sequenceColumn) {
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

	public long getMaxPendingTimeMs() {
		return maxPendingTimeMs;
	}

	public float getMaxFilterRatio() {
		return maxFilterRatio;
	}

	public long getRetryIntervalMs() {
		return retryIntervalMs;
	}

	public int getMaxRetryNum() {
		return maxRetryNum;
	}

	public long getFeUpdateIntervalMs() {
		return feUpdateIntervalMs;
	}

	public int getParallelism() {
		return parallelism;
	}

	public String getSequenceColumn() {
		return sequenceColumn;
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
		private TableModel tableModel;
		private DataFormat dataFormat;
		private String columnSeparator;
		private int maxBytesPerBatch;
		private int maxPendingBatchNum;
		private long maxPendingTimeMs;
		private float maxFilterRatio;
		private long retryIntervalMs;
		private int maxRetryNum;
		private long feUpdateIntervalMs;
		private int parallelism;
		private String sequenceColumn;

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

		public Builder setMaxPendingTimeMs(long maxPendingTimeMs) {
			this.maxPendingTimeMs = maxPendingTimeMs;
			return this;
		}

		public Builder setMaxFilterRatio(float maxFilterRatio) {
			this.maxFilterRatio = maxFilterRatio;
			return this;
		}

		public Builder setRetryIntervalMs(long retryIntervalMs) {
			this.retryIntervalMs = retryIntervalMs;
			return this;
		}

		public Builder setMaxRetryNum(int maxRetryNum) {
			this.maxRetryNum = maxRetryNum;
			return this;
		}

		public Builder setFeUpdateIntervalMs(long feUpdateIntervalMs) {
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

		public DorisOptions build() {
			// There are two ways to get connection, through DORIS_FE_LIST or DORIS_FE_PSM + DATA_CENTER,
			// and the second way has higher priority if they are both set.
			if ((dorisFEList == null || dorisFEList.size() == 0) && (StringUtils.isEmpty(dorisFEPsm)
					|| StringUtils.isEmpty(dataCenter))) {
				throw new FlinkRuntimeException("You must set either 'doris-fe-list' or "
					+ "'doris-fe-psm' and 'data-center', please check your params");
			}
			Preconditions.checkNotNull(cluster, "cluster can not be null");
			Preconditions.checkNotNull(user, "user can not be null");
			Preconditions.checkNotNull(password, "dataCenter can not be null");
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
				sequenceColumn);
		}
	}
}
