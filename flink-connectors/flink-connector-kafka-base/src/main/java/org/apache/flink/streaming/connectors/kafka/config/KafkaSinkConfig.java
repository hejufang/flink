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

import org.apache.flink.table.data.RowData;
import org.apache.flink.table.functions.DeleteNormalizer;
import org.apache.flink.table.types.logical.LogicalType;

/**
 * KafkaSinkConf.
 */
public class KafkaSinkConfig {
	private final DeleteNormalizer<RowData> deleteNormalizer;

	private final Integer sinkMsgKeyIndex;

	private final LogicalType sinkMsgKeyLogicalType;

	public Integer getSinkMsgKeyIndex() {
		return sinkMsgKeyIndex;
	}

	public LogicalType getSinkMsgKeyLogicalType() {
		return sinkMsgKeyLogicalType;
	}

	private KafkaSinkConfig(
			DeleteNormalizer<RowData> deleteNormalizer,
			Integer sinkMsgKeyIndex,
			LogicalType sinkMsgKeyLogicalType) {
		this.deleteNormalizer = deleteNormalizer;
		this.sinkMsgKeyIndex = sinkMsgKeyIndex;
		this.sinkMsgKeyLogicalType = sinkMsgKeyLogicalType;
	}

	public DeleteNormalizer<RowData> getDeleteNormalizer() {
		return deleteNormalizer;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * The builder for {@link KafkaSinkConfig}.
	 */
	public static class Builder {
		private DeleteNormalizer<RowData> deleteNormalizer;

		private Integer sinkMsgKeyIndex;

		private LogicalType sinkMsgKeyLogicalType;

		private Builder() {
		}

		public Builder withDeleteNormalizer(DeleteNormalizer<RowData> deleteNormalizer) {
			this.deleteNormalizer = deleteNormalizer;
			return this;
		}

		public Builder withSinkMsgKeyInformation(Integer sinkMsgKeyIndex, LogicalType sinkMsgKeyLogicalType) {
			this.sinkMsgKeyIndex = sinkMsgKeyIndex;
			this.sinkMsgKeyLogicalType = sinkMsgKeyLogicalType;
			return this;
		}

		public KafkaSinkConfig build() {
			return new KafkaSinkConfig(
					deleteNormalizer,
					sinkMsgKeyIndex,
					sinkMsgKeyLogicalType);
		}
	}
}
