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

import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;

import static org.apache.flink.util.Preconditions.checkNotNull;

/**
 * Configuration used by {@link HtapRowInputFormat}. Specifies connection and other necessary properties.
 */
@PublicEvolving
public class HtapReaderConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private final String metaHost;
	private final int metaPort;
	private final String instanceId;

	private final String byteStoreLogPath;
	private final String byteStoreDataPath;
	private final String logStoreLogDir;
	private final String pageStoreLogDir;

	private final long checkPointLSN;

	public HtapReaderConfig(
			String metaHost,
			int metaPort,
			String instanceId,
			String byteStoreLogPath,
			String byteStoreDataPath,
			String logStoreLogDir,
			String pageStoreLogDir,
			long checkPointLSN) {
		this.metaHost = checkNotNull(metaHost, "Htap meta host cannot be null");
		this.metaPort = checkNotNull(metaPort, "Htap meta port cannot be null");
		this.instanceId = checkNotNull(instanceId, "Htap instanceId cannot be null");
		this.byteStoreLogPath = checkNotNull(byteStoreLogPath, "ByteStore LogPath cannot be null");
		this.byteStoreDataPath = checkNotNull(byteStoreDataPath, "ByteStore DataPath cannot be null");
		this.logStoreLogDir = checkNotNull(logStoreLogDir, "LogStore LogDir cannot be null");
		this.pageStoreLogDir = checkNotNull(pageStoreLogDir, "PageStore LogDir cannot be null");
		this.checkPointLSN = checkNotNull(checkPointLSN, "CheckPointLSN cannot be null");
	}

	public String getMetaHosts() {
		return metaHost;
	}

	public int getMetaPort() {
		return metaPort;
	}

	public String getInstanceId() {
		return instanceId;
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

	public long getCheckPointLSN() {
		return checkPointLSN;
	}

	@Override
	public String toString() {
		return new ToStringBuilder(this)
				.append("metaHost", metaHost)
				.append("metaPort", metaPort)
				.append("instanceId", instanceId)
				.append("byteStoreLogPath", byteStoreLogPath)
				.append("byteStoreDataPath", byteStoreDataPath)
				.append("logStoreLogDir", logStoreLogDir)
				.append("pageStoreLogDir", pageStoreLogDir)
				.append("checkPointLSN", checkPointLSN)
				.toString();
	}
}
