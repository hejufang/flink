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

import org.apache.flink.runtime.rest.messages.ResponseBody;

import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonCreator;
import org.apache.flink.shaded.jackson2.com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

/**
 * Response to the on resource group changed handler, containing a version cronstructed from the reqeust version.
 */
public class ResourceGroupChangedResponseBody implements ResponseBody {

	private static final String FIELD_CODE = "code";

	private static final String FIELD_VERSION = "version";

	private static final String FIELD_REQUEST_ID = "request_id";

	@JsonProperty(FIELD_CODE)
	public final String code;

	@JsonProperty(FIELD_VERSION)
	public final int version;

	@JsonProperty(FIELD_REQUEST_ID)
	public final String requestID;

	@JsonCreator
	public ResourceGroupChangedResponseBody(
		@JsonProperty(FIELD_CODE) String code,
		@JsonProperty(FIELD_VERSION) int version,
		@JsonProperty(FIELD_REQUEST_ID) String requestID) {
		this.code = code;
		this.version = version;
		this.requestID = requestID;
	}

	@Override
	public boolean equals(Object object) {
		if (object instanceof ResourceGroupChangedResponseBody) {
			ResourceGroupChangedResponseBody other = (ResourceGroupChangedResponseBody) object;
			return Objects.equals(this.code, other.code) &&
				Objects.equals(this.version, other.version) &&
				Objects.equals(this.requestID, other.requestID);
		}
		return false;
	}

}
