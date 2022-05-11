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

package org.apache.flink.connectors.htap.connector.reader;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.connectors.htap.batch.HtapRowInputFormat;

import com.bytedance.htap.metaclient.catalog.Snapshot;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration used by {@link HtapRowInputFormat}. Specifies connection and other necessary properties.
 */
@PublicEvolving
public class HtapReaderConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String metaSvcRegion;
	private final String metaSvcCluster;
	private final String dbCluster;

	private final String byteStoreLogPath;
	private final String byteStoreDataPath;
	private final String logStoreLogDir;
	private final String pageStoreLogDir;
	private final int batchSizeBytes;

	private final Snapshot snapshot;
	private boolean compatibleWithMySQL = false;

	public HtapReaderConfig(
			String metaSvcRegion,
			String metaSvcCluster,
			String dbCluster,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir,
			int batchSizeBytes,
			Snapshot snapshot) {
		this.metaSvcRegion = checkNotNull(metaSvcRegion, "Htap MetaService region cannot be null");
		this.metaSvcCluster = checkNotNull(metaSvcCluster, "Htap MetaService cluster cannot be null");
		this.dbCluster = checkNotNull(dbCluster, "Htap dbCluster cannot be null");
		this.byteStoreLogPath = checkNotNull(byteStoreLogPath, "ByteStore LogPath cannot be null");
		this.byteStoreDataPath = checkNotNull(byteStoreDataPath, "ByteStore DataPath cannot be null");
		this.logStoreLogDir = checkNotNull(logStoreLogDir, "LogStore LogDir cannot be null");
		this.pageStoreLogDir = checkNotNull(pageStoreLogDir, "PageStore LogDir cannot be null");
		this.batchSizeBytes = checkNotNull(batchSizeBytes, "BatchSizeBytes cannot be null");
		this.snapshot = checkNotNull(snapshot, "snapshot cannot be null");
	}

	public String getMetaSvcRegion() {
		return metaSvcRegion;
	}

	public String getMetaSvcCluster() {
		return metaSvcCluster;
	}

	public String getDbCluster() {
		return dbCluster;
	}

	public String getByteStoreLogPath() {
		return byteStoreLogPath;
	}

	public String getByteStoreDataPath() {
		return byteStoreDataPath;
	}

	public String getLogStoreLogDir() {
		return logStoreLogDir;
	}

	public String getPageStoreLogDir() {
		return pageStoreLogDir;
	}

	public int getBatchSizeBytes() {
		return batchSizeBytes;
	}

	public Snapshot getSnapshot() {
		return snapshot;
	}

	public boolean isCompatibleWithMySQL() {
		return compatibleWithMySQL;
	}

	public void setCompatibleWithMySQL(boolean compatibleWithMySQL) {
		this.compatibleWithMySQL = compatibleWithMySQL;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this)
				.append("meta service region", metaSvcRegion)
				.append("meta service cluster", metaSvcCluster)
				.append("dbCluster", dbCluster)
				.append("byteStoreLogPath", byteStoreLogPath)
				.append("byteStoreDataPath", byteStoreDataPath)
				.append("logStoreLogDir", logStoreLogDir)
				.append("pageStoreLogDir", pageStoreLogDir)
				.append("batchSizeBytes", batchSizeBytes)
				.append("snapshot", snapshot)
				.append("compatibleWithMySQL", compatibleWithMySQL)
				.toString();
	}
}
