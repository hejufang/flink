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

package org.apache.flink.streaming.connectors.kafka.config;

import org.apache.flink.api.common.io.ratelimiting.RateLimitingUnit;
import org.apache.flink.table.factories.DynamicSourceMetadataFactory;
import org.apache.flink.table.types.DataType;

import java.io.Serializable;
import java.util.Map;
import java.util.Objects;

/**
 * KafkaSourceConf.
 */
public class KafkaSourceConfig implements Serializable  {
	private long rateLimitNumber = -1;
	private RateLimitingUnit rateLimitingUnit;
	private Long scanSampleInterval;
	private Long scanSampleNum;
	private String partitionTopicList;
	private Boolean kafkaResetNewPartition;
	private DataType withoutMetaDataType;
	private Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap;
	private long manualCommitInterval;

	public long getRateLimitNumber() {
		return rateLimitNumber;
	}

	public void setRateLimitNumber(long rateLimitNumber) {
		this.rateLimitNumber = rateLimitNumber;
	}

	public RateLimitingUnit getRateLimitingUnit() {
		return rateLimitingUnit;
	}

	public void setRateLimitingUnit(RateLimitingUnit rateLimitingUnit) {
		this.rateLimitingUnit = rateLimitingUnit;
	}

	public Long getScanSampleInterval() {
		return scanSampleInterval;
	}

	public void setScanSampleInterval(Long scanSampleInterval) {
		this.scanSampleInterval = scanSampleInterval;
	}

	public Long getScanSampleNum() {
		return scanSampleNum;
	}

	public void setScanSampleNum(Long scanSampleNum) {
		this.scanSampleNum = scanSampleNum;
	}

	public String getPartitionTopicList() {
		return partitionTopicList;
	}

	public void setPartitionTopicList(String partitionTopicList) {
		this.partitionTopicList = partitionTopicList;
	}

	public Boolean getKafkaResetNewPartition() {
		return kafkaResetNewPartition;
	}

	public void setKafkaResetNewPartition(Boolean kafkaResetNewPartition) {
		this.kafkaResetNewPartition = kafkaResetNewPartition;
	}

	public DataType getWithoutMetaDataType() {
		return withoutMetaDataType;
	}

	public void setWithoutMetaDataType(DataType withoutMetaDataType) {
		this.withoutMetaDataType = withoutMetaDataType;
	}

	public Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> getMetadataMap() {
		return metadataMap;
	}

	public void setMetadataMap(Map<Integer, DynamicSourceMetadataFactory.DynamicSourceMetadata> metadataMap) {
		this.metadataMap = metadataMap;
	}

	public long getManualCommitInterval() {
		return manualCommitInterval;
	}

	public void setManualCommitInterval(long manualCommitInterval) {
		this.manualCommitInterval = manualCommitInterval;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		KafkaSourceConfig that = (KafkaSourceConfig) o;
		return rateLimitNumber == that.rateLimitNumber &&
			rateLimitingUnit == that.rateLimitingUnit &&
			Objects.equals(scanSampleInterval, that.scanSampleInterval) &&
			Objects.equals(scanSampleNum, that.scanSampleNum) &&
			Objects.equals(partitionTopicList, that.partitionTopicList) &&
			Objects.equals(kafkaResetNewPartition, that.kafkaResetNewPartition) &&
			Objects.equals(withoutMetaDataType, that.withoutMetaDataType) &&
			Objects.equals(metadataMap, that.metadataMap) &&
			Objects.equals(manualCommitInterval, that.manualCommitInterval);
	}

	@Override
	public int hashCode() {
		return Objects.hash(rateLimitNumber, rateLimitingUnit, scanSampleInterval, scanSampleNum,
			partitionTopicList, kafkaResetNewPartition, withoutMetaDataType, metadataMap, manualCommitInterval);
	}
}
