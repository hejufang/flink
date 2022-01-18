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

import org.apache.flink.core.fs.FileInputSplit;
import org.apache.flink.core.fs.Path;

/**
 * BmqFileInputSplit.
 */
public class BmqFileInputSplit extends FileInputSplit {

	private static final long serialVersionUID = 1L;

	private final long startOffset;
	private final long endOffset;
	private final long expectedStartOffset;
	private final long expectedEndOffset;

	private BmqFileInputSplit(
			int splitNumber,
			Path file,
			long start,
			long length,
			String[] hosts,
			long startOffset,
			long endOffset,
			long expectedStartOffset,
			long expectedEndOffset) {
		super(splitNumber, file, start, length, hosts);
		this.startOffset = startOffset;
		this.endOffset = endOffset;
		this.expectedStartOffset = expectedStartOffset;
		this.expectedEndOffset = expectedEndOffset;
	}

	public long getStartOffset() {
		return startOffset;
	}

	public long getEndOffset() {
		return endOffset;
	}

	public long getExpectedStartOffset() {
		return expectedStartOffset;
	}

	public long getExpectedEndOffset() {
		return expectedEndOffset;
	}

	public static Builder builder() {
		return new Builder();
	}

	@Override
	public String toString() {
		return "BmqFileInputSplit{" +
			super.toString() +
			", startOffset=" + startOffset +
			", endOffset=" + endOffset +
			", expectedStartOffset=" + expectedStartOffset +
			", expectedEndOffset=" + expectedEndOffset +
			'}';
	}

	/**
	 * Builder for {@link org.apache.flink.connector.bmq.BmqFileInputSplit}.
	 */
	public static class Builder {
		private int splitNumber;
		private Path file;
		private long start;
		private long length;
		private String[] hosts;
		private long startOffset;
		private long endOffset;
		private long expectedStartOffset;
		private long expectedEndOffset;

		private Builder() {
			// private constructor
		}

		public BmqFileInputSplit.Builder setSplitNumber(int splitNumber) {
			this.splitNumber = splitNumber;
			return this;
		}

		public BmqFileInputSplit.Builder setPath(Path file) {
			this.file = file;
			return this;
		}

		public BmqFileInputSplit.Builder setStart(long start) {
			this.start = start;
			return this;
		}

		public BmqFileInputSplit.Builder setLength(long length) {
			this.length = length;
			return this;
		}

		public BmqFileInputSplit.Builder setHosts(String[] hosts) {
			this.hosts = hosts;
			return this;
		}

		public BmqFileInputSplit.Builder setStartOffset(long startOffset) {
			this.startOffset = startOffset;
			return this;
		}

		public BmqFileInputSplit.Builder setEndOffset(long endOffset) {
			this.endOffset = endOffset;
			return this;
		}

		public BmqFileInputSplit.Builder setExpectedStartOffset(long expectedStartOffset) {
			this.expectedStartOffset = expectedStartOffset;
			return this;
		}

		public BmqFileInputSplit.Builder setExpectedEndOffset(long expectedEndOffset) {
			this.expectedEndOffset = expectedEndOffset;
			return this;
		}

		public BmqFileInputSplit build() {
			return new BmqFileInputSplit(
				splitNumber,
				file,
				start,
				length,
				hosts,
				startOffset,
				endOffset,
				expectedStartOffset,
				expectedEndOffset);
		}
	}
}
