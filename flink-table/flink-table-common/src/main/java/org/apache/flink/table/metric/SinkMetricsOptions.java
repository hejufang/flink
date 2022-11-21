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

package org.apache.flink.table.metric;

import org.apache.flink.util.Preconditions;
import org.apache.flink.util.StringUtils;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * common metrics options of sink connector.
 */
public class SinkMetricsOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	// whether report metrics
	private final boolean collected;

	// the list of quantiles to be calculated
	private final List<Double> percentiles;

	// The column which marks the timestamp(13-digits) of data
	private final String eventTsColName;

	// The index of eventTsColName
	private final int eventTsColIndex;

	// whether the column is written to abase
	private final boolean eventTsWriteable;

	// the list of columns that are regarded as tags of metrics
	private final List<String> tagNames;

	// the indices of tagNames
	private final List<Integer> tagNameIndices;

	// whether the columns marked as tags are written to abase
	private final boolean tagWriteable;

	// the extra properties to be reported with metrics
	private final Map<String, String> props;

	// the bucket size of histogram
	private final long bucketsSize;

	// the number of buckets
	private final int bucketsNum;

	// the series of buckets division points
	private final List<Long> buckets;

	private final long logErrorInterval;

	public boolean isCollected() {
		return collected;
	}

	public List<Double> getPercentiles() {
		return percentiles;
	}

	public String getEventTsColName() {
		return eventTsColName;
	}

	public int getEventTsColIndex() {
		return eventTsColIndex;
	}

	public boolean isEventTsWriteable() {
		return eventTsWriteable;
	}

	public List<String> getTagNames() {
		return tagNames;
	}

	public List<Integer> getTagNameIndices() {
		return tagNameIndices;
	}

	public boolean isTagWriteable() {
		return tagWriteable;
	}

	public Map<String, String> getProps() {
		return props;
	}

	public long getBucketsSize() {
		return bucketsSize;
	}

	public int getBucketsNum() {
		return bucketsNum;
	}

	public List<Long> getBuckets() {
		return buckets;
	}

	public long getLogErrorInterval() {
		return logErrorInterval;
	}

	private SinkMetricsOptions(
			List<Double> percentiles,
			String eventTsColName,
			int eventTsColIndex,
			boolean eventTsWriteable,
			List<String> tagNames,
			List<Integer> tagNameIndices,
			boolean tagWriteable,
			Map<String, String> props,
			long bucketsSize,
			int bucketsNum,
			List<Long> buckets,
			long logErrorInterval) {
		this.collected = !StringUtils.isNullOrWhitespaceOnly(eventTsColName);
		this.percentiles = percentiles;
		this.eventTsColName = eventTsColName;
		this.eventTsColIndex = eventTsColIndex;
		this.eventTsWriteable = eventTsWriteable;
		this.tagNames = tagNames;
		this.tagNameIndices = tagNameIndices;
		this.tagWriteable = tagWriteable;
		this.props = props;
		this.bucketsSize = bucketsSize;
		this.bucketsNum = bucketsNum;
		this.buckets = buckets;
		this.logErrorInterval = logErrorInterval;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * builder class of {@link SinkMetricsOptions}.
	 */
	public static class Builder {
		private List<Double> percentiles = null;
		private String eventTsColName = null;
		private int eventTsColIndex = -1;
		private boolean eventTsWriteable = false;
		private List<String> tagNames = null;
		private List<Integer> tagNameIndices = null;
		private boolean tagWriteable = false;
		private Map<String, String> props = null;
		private long bucketsSize = 0;
		private int bucketsNum = 0;
		private List<Long> buckets = null;
		private long logErrorInterval;

		public Builder setPercentiles(List<Double> percentiles) {
			Preconditions.checkArgument(percentiles != null && percentiles.size() > 0,
				"The percentiles can't be configured empty.");
			Preconditions.checkArgument(isValidPercentiles(percentiles),
				"Invalid percentile number, should be an integer greater than 0 and less than 100 and "
					+ "no duplicate number is allowed");
			this.percentiles = new ArrayList<>(percentiles);
			Collections.sort(this.percentiles);
			return this;
		}

		public Builder setEventTsColName(String eventTsColName) {
			this.eventTsColName = eventTsColName;
			return this;
		}

		public Builder setEventTsColIndex(int eventTsColIndex) {
			this.eventTsColIndex = eventTsColIndex;
			return this;
		}

		public Builder setEventTsWriteable(boolean eventTsWriteable) {
			this.eventTsWriteable = eventTsWriteable;
			return this;
		}

		public Builder setTagNames(List<String> tagNames) {
			Preconditions.checkArgument(isDistinct(tagNames), "Duplicate tag names are not allowed!");
			this.tagNames = tagNames;
			return this;
		}

		public Builder setTagNameIndices(List<Integer> tagNameIndices) {
			this.tagNameIndices = tagNameIndices;
			return this;
		}

		public Builder setTagWriteable(boolean tagWriteable) {
			this.tagWriteable = tagWriteable;
			return this;
		}

		public Builder setProps(Map<String, String> props) {
			this.props = props;
			return this;
		}

		public Builder setBucketsSize(long bucketsSize) {
			this.bucketsSize = bucketsSize;
			return this;
		}

		public Builder setBucketsNum(int bucketsNum) {
			this.bucketsNum = bucketsNum;
			return this;
		}

		public Builder setBuckets(List<Long> buckets) {
			Preconditions.checkArgument(buckets != null && buckets.size() > 0,
				"The buckets can't be configured empty.");
			Preconditions.checkArgument(isValidBuckets(buckets),
				"Negative or duplicate number found in buckets!");
			this.buckets = buckets;
			Collections.sort(this.buckets);
			return this;
		}

		public Builder setLogErrorInterval(long logErrorInterval) {
			Preconditions.checkArgument(logErrorInterval >= 0,
				"The sink.metrics.log.error.interval can't be configured less than zero.");
			this.logErrorInterval = logErrorInterval;
			return this;
		}

		public SinkMetricsOptions build() {
			Preconditions.checkArgument((bucketsSize == 0 && bucketsNum == 0) || (bucketsSize > 0 && bucketsNum > 0),
				"bucketsSize and bucketsNum should be configured simultaneously.");
			Preconditions.checkArgument(bucketsNum < 1000, "bucketsNum should be less than 1000");

			return new SinkMetricsOptions(
				percentiles,
				eventTsColName,
				eventTsColIndex,
				eventTsWriteable,
				tagNames,
				tagNameIndices,
				tagWriteable,
				props,
				bucketsSize,
				bucketsNum,
				buckets,
				logErrorInterval);
		}

		private static boolean isDistinct(List<String> list) {
			return new HashSet<>(list).size() == list.size();
		}

		private static boolean isValidPercentiles(List<Double> percentiles) {
			if (percentiles == null) {
				return true;
			}
			if (new HashSet<>(percentiles).size() != percentiles.size()) {
				return false;
			}
			for (double percentile : percentiles) {
				if (percentile < 0 || percentile > 1) {
					return false;
				}
			}
			return true;
		}

		private static boolean isValidBuckets(List<Long> buckets) {
			if (buckets == null) {
				return true;
			}

			// check duplicates
			Set<Long> bucketSet = new HashSet<>(buckets);
			if (bucketSet.size() < buckets.size()) {
				return false;
			}

			// check negative numbers
			for (long bucket : buckets) {
				if (bucket < 0) {
					return false;
				}
			}
			return true;
		}

		@Override
		public String toString() {
			return "AbaseSinkMetricsOptionsBuilder{" +
				"percentiles=" + percentiles +
				", eventTsColName='" + eventTsColName + '\'' +
				", eventTsColIndex='" + eventTsColIndex + '\'' +
				", eventTsWriteable=" + eventTsWriteable +
				", tagNames=" + tagNames +
				", tagNameIndices=" + tagNameIndices +
				", tagWriteable=" + tagWriteable +
				", props=" + props +
				", bucketsSize=" + bucketsSize +
				", bucketsNum=" + bucketsNum +
				", buckets=" + buckets +
				'}';
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (!(o instanceof SinkMetricsOptions)) {
			return false;
		}
		SinkMetricsOptions that = (SinkMetricsOptions) o;
		return eventTsWriteable == that.eventTsWriteable &&
			tagWriteable == that.tagWriteable &&
			bucketsSize == that.bucketsSize &&
			bucketsNum == that.bucketsNum &&
			logErrorInterval == that.logErrorInterval &&
			Objects.equals(percentiles, that.percentiles) &&
			eventTsColName.equals(that.eventTsColName) &&
			eventTsColIndex == that.eventTsColIndex &&
			Objects.equals(tagNames, that.tagNames) &&
			Objects.equals(tagNameIndices, that.tagNameIndices) &&
			props.equals(that.props) &&
			Objects.equals(buckets, that.buckets);
	}

	@Override
	public int hashCode() {
		return Objects.hash(
			percentiles,
			eventTsColName,
			eventTsColIndex,
			eventTsWriteable,
			tagNames,
			tagNameIndices,
			tagWriteable,
			props,
			bucketsSize,
			bucketsNum,
			buckets,
			logErrorInterval);
	}

	@Override
	public String toString() {
		return "SinkMetricsOptions{" +
			"collected=" + collected +
			", percentiles=" + percentiles +
			", eventTsColName='" + eventTsColName + '\'' +
			", eventTsColIndex=" + eventTsColIndex +
			", eventTsWriteable=" + eventTsWriteable +
			", tagNames=" + tagNames +
			", tagNameIndices=" + tagNameIndices +
			", tagWriteable=" + tagWriteable +
			", props=" + props +
			", bucketsSize=" + bucketsSize +
			", bucketsNum=" + bucketsNum +
			", buckets=" + buckets +
			'}';
	}
}