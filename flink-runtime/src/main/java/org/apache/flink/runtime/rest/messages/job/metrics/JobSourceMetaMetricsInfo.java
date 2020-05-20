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

package org.apache.flink.runtime.rest.messages.job.metrics;

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;

/**
 * Metadata for source (job level).
 */
public class JobSourceMetaMetricsInfo implements ResponseBody {

	private static final String FIELD_NAME_METAS = "metas";

	@JsonProperty(FIELD_NAME_METAS)
	private final List<SourceMetaMetricsInfo> metas;

	@JsonCreator
	public JobSourceMetaMetricsInfo(@JsonProperty(FIELD_NAME_METAS) List<SourceMetaMetricsInfo> metas) {
		this.metas = metas;
	}

	public List<SourceMetaMetricsInfo> getMetas() {
		return metas;
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		JobSourceMetaMetricsInfo that = (JobSourceMetaMetricsInfo) o;
		return metas.equals(that.metas);
	}

	@Override
	public int hashCode() {
		return Objects.hash(metas);
	}
}
