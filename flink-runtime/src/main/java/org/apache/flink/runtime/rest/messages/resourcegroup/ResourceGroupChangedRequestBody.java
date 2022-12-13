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

package org.apache.flink.runtime.rest.messages.resourcegroup;

import org.apache.flink.runtime.resourcemanager.resourcegroup.client.apass.APaaSResourceGroupInfo;
import org.apache.flink.runtime.rest.messages.RequestBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import javax.annotation.Nullable;

import java.util.Collection;
import java.util.Collections;
import java.util.Objects;

/**
 * Request for on rg changed callback.
 *
 * <p>This request only contains the names of files that must be present on the server, and defines how these files are
 * interpreted.
 */
public class ResourceGroupChangedRequestBody implements RequestBody {

	private static final String FIELD_INSTANCE_NAME = "instance_name";

	private static final String FIELD_VERSION = "version";

	private static final String FIELD_TYPE = "type";

	private static final String FIELD_RG = "rg";

	@JsonProperty(FIELD_INSTANCE_NAME)
	public final String instanceName;

	@JsonProperty(FIELD_VERSION)
	public final int version;

	@JsonProperty(FIELD_TYPE)
	public final String type;

	@JsonProperty(FIELD_RG)
	@Nullable
	public final Collection<APaaSResourceGroupInfo> rgInfos;

	@JsonCreator
	public ResourceGroupChangedRequestBody(
		@JsonProperty(FIELD_INSTANCE_NAME) String instanceName,
		@JsonProperty(FIELD_VERSION) int version,
		@JsonProperty(FIELD_TYPE) String type,
		@Nullable @JsonProperty(FIELD_RG) Collection<APaaSResourceGroupInfo> rgInfos) {

		this.instanceName = instanceName;
		this.version = version;
		this.type = type;

		if (rgInfos == null) {
			this.rgInfos = Collections.emptyList();
		} else {
			this.rgInfos = rgInfos;
		}
	}

	@Override
	public boolean equals(Object o) {
		if (this == o) {
			return true;
		}
		if (o == null || getClass() != o.getClass()) {
			return false;
		}
		ResourceGroupChangedRequestBody that = (ResourceGroupChangedRequestBody) o;
		return Objects.equals(instanceName, that.instanceName) &&
			Objects.equals(version, that.version) &&
			Objects.equals(type, that.type) &&
			Objects.equals(rgInfos, that.rgInfos);
	}

	@Override
	public int hashCode() {
		return Objects.hash(instanceName, version, type, rgInfos);
	}

	@Override
	public String toString() {
		return "ResourceGroupChangedRequestBody{" +
			"instanceName='" + instanceName +
			", version=" + version +
			", type=" + type +
			", rgInfos=" + rgInfos +
			'}';
	}
}
