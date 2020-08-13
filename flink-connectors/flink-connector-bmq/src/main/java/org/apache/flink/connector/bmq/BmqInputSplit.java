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

package org.apache.flink.connector.bmq;

import org.apache.flink.core.io.InputSplit;

import org.apache.kafka.common.TopicPartition;

/**
 * BMQ InputSplit.
 */
public class BmqInputSplit implements InputSplit {

	private final int splitNumber;
	private final int indexInSameTopicPartition;
	private final TopicPartition topicPartition;
	private final long expectedStartOffset;
	private final long expectedEndOffset;
	private final String health;
	private final String filePath;
	private final String messageFormat;
	private final String codec;
	private final long startOffset;
	private final long messageBytes;

	private BmqInputSplit(
			int splitNumber,
			int indexInSameTopicPartition,
			TopicPartition topicPartition,
			long expectedStartOffset,
			long expectedEndOffset,
			String health,
			String filePath,
			String messageFormat,
			String codec,
			long startOffset,
			long messageBytes) {
		this.splitNumber = splitNumber;
		this.indexInSameTopicPartition = indexInSameTopicPartition;
		this.topicPartition = topicPartition;
		this.expectedStartOffset = expectedStartOffset;
		this.expectedEndOffset = expectedEndOffset;
		this.health = health;
		this.filePath = filePath;
		this.messageFormat = messageFormat;
		this.codec = codec;
		this.startOffset = startOffset;
		this.messageBytes = messageBytes;
	}

	@Override
	public int getSplitNumber() {
		return splitNumber;
	}

	public int getIndexInSameTopicPartition() {
		return indexInSameTopicPartition;
	}

	public TopicPartition getTopicPartition() {
		return topicPartition;
	}

	public long getExpectedStartOffset() {
		return expectedStartOffset;
	}

	public long getExpectedEndOffset() {
		return expectedEndOffset;
	}

	public String getHealth() {
		return health;
	}

	public String getFilePath() {
		return filePath;
	}

	public String getMessageFormat() {
		return messageFormat;
	}

	public String getCodec() {
		return codec;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public long getMessageBytes() {
		return messageBytes;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String toString() {
		return String.format("BmqInputSplit{splitNumber=%d, indexInSameTopicPartition=%d, topicPartition=%s, " +
				"expectedStartOffset=%d, expectedEndOffset=%d, health=%s, filePath=%s, messageFormat=%s, codec=%s, " +
				"startOffset=%d, messageBytes=%d}",
			splitNumber,
			indexInSameTopicPartition,
			topicPartition,
			expectedStartOffset,
			expectedEndOffset,
			health,
			filePath,
			messageFormat,
			codec,
			startOffset,
			messageBytes
		);
	}

	/**
	 * Builder for {@link org.apache.flink.connector.bmq.BmqInputSplit}.
	 */
	public static class Builder {

		private int splitNumber;
		private int indexInSameTopicPartition;
		private TopicPartition topicPartition;
		private long expectedStartOffset;
		private long expectedEndOffset;
		private String health;
		private String filePath;
		private String messageFormat;
		private String codec;
		private long startOffset;
		private long messageBytes;

		private Builder() {
			// private constructor
		}

		public Builder setSplitNumber(int splitNumber) {
			this.splitNumber = splitNumber;
			return this;
		}

		public Builder setIndexInSameTopicPartition(int indexInSameTopicPartition) {
			this.indexInSameTopicPartition = indexInSameTopicPartition;
			return this;
		}

		public Builder setTopicPartition(TopicPartition topicPartition) {
			this.topicPartition = topicPartition;
			return this;
		}

		public Builder setExpectedStartOffset(long expectedStartOffset) {
			this.expectedStartOffset = expectedStartOffset;
			return this;
		}

		public Builder setExpectedEndOffset(long expectedEndOffset) {
			this.expectedEndOffset = expectedEndOffset;
			return this;
		}

		public Builder setHealth(String health) {
			this.health = health;
			return this;
		}

		public Builder setFilePath(String filePath) {
			this.filePath = filePath;
			return this;
		}

		public Builder setMessageFormat(String messageFormat) {
			this.messageFormat = messageFormat;
			return this;
		}

		public Builder setCodec(String codec) {
			this.codec = codec;
			return this;
		}

		public Builder setStartOffset(long startOffset) {
			this.startOffset = startOffset;
			return this;
		}

		public Builder setMessageBytes(long messageBytes) {
			this.messageBytes = messageBytes;
			return this;
		}

		public BmqInputSplit build() {
			return new BmqInputSplit(
				splitNumber,
				indexInSameTopicPartition,
				topicPartition,
				expectedStartOffset,
				expectedEndOffset,
				health,
				filePath,
				messageFormat,
				codec,
				startOffset,
				messageBytes);
		}
	}

}
