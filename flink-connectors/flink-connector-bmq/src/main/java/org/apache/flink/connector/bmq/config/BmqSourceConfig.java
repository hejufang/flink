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

package org.apache.flink.connector.bmq.config;

import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * BmqSourceConfig.
 */
public class BmqSourceConfig implements Serializable {

	private static final long serialVersionUID = 1L;

	private String version;
	private String cluster;
	private String topic;
	private Long scanStartTimeMs;
	private Long scanEndTimeMs;
	private Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;
	private DataType withoutMetaDataType;
	private boolean ignoreUnhealthySegment;

	public String getVersion() {
		return version;
	}

	public void setVersion(String version) {
		this.version = version;
	}

	public String getCluster() {
		return cluster;
	}

	public void setCluster(String cluster) {
		this.cluster = cluster;
	}

	public String getTopic() {
		return topic;
	}

	public void setTopic(String topic) {
		this.topic = topic;
	}

	public Long getScanStartTimeMs() {
		return scanStartTimeMs;
	}

	public void setScanStartTimeMs(Long scanStartTimeMs) {
		this.scanStartTimeMs = scanStartTimeMs;
	}

	public Long getScanEndTimeMs() {
		return scanEndTimeMs;
	}

	public void setScanEndTimeMs(Long scanEndTimeMs) {
		this.scanEndTimeMs = scanEndTimeMs;
	}

	public Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> getMetadataMap() {
		return metadataMap;
	}

	public void setMetadataMap(Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
		this.metadataMap = metadataMap;
	}

	public void setWithoutMetaDataType(DataType withoutMetaDataType) {
		this.withoutMetaDataType = withoutMetaDataType;
	}

	public boolean isIgnoreUnhealthySegment() {
		return ignoreUnhealthySegment;
	}

	public void setIgnoreUnhealthySegment(boolean ignoreUnhealthySegment) {
		this.ignoreUnhealthySegment = ignoreUnhealthySegment;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		BmqSourceConfig that = (BmqSourceConfig) o;
		return ignoreUnhealthySegment == that.ignoreUnhealthySegment &&
			Objects.equals(version, that.version) &&
			Objects.equals(cluster, that.cluster) &&
			Objects.equals(topic, that.topic) &&
			Objects.equals(scanStartTimeMs, that.scanStartTimeMs) &&
			Objects.equals(scanEndTimeMs, that.scanEndTimeMs) &&
			Objects.equals(withoutMetaDataType, that.withoutMetaDataType) &&
			Objects.equals(metadataMap, that.metadataMap);
	}

	@Override
	public int hashCode() {
		return Objects.hash(version, cluster, topic, scanStartTimeMs, scanEndTimeMs,
			withoutMetaDataType, metadataMap, ignoreUnhealthySegment);
	}

	@Override
	public String toString() {
		return "BmqSourceConfig{" +
			"version='" + version + '\'' +
			", cluster='" + cluster + '\'' +
			", topic='" + topic + '\'' +
			", scanStartTimeMs=" + scanStartTimeMs +
			", scanEndTimeMs=" + scanEndTimeMs +
			", metadataMap=" + metadataMap +
			", withoutMetaDataType=" + withoutMetaDataType +
			", ignoreUnhealthySegment=" + ignoreUnhealthySegment +
			'}';
	}
}
