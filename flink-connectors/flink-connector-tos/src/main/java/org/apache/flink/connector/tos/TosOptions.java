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

package org.apache.flink.connector.tos;

import org.apache.flink.util.Preconditions;

import java.io.Serializable;

/**
 * TosOptions.
 */
public class TosOptions implements Serializable {

	private static final long serialVersionUID = 1L;

	// must
	private final String bucket;
	private final String accessKey;
	private final String objectKey;
	// optional
	private final String cluster;
	private final int tosTimeoutSeconds;
	private final String psm;

	private TosOptions(
			String bucket,
			String accessKey,
			String objectKey,
			String cluster,
			int tosTimeoutSeconds,
			String psm) {
		this.bucket = bucket;
		this.accessKey = accessKey;
		this.objectKey = objectKey;
		this.cluster = cluster;
		this.tosTimeoutSeconds = tosTimeoutSeconds;
		this.psm = psm;
	}

	public String getBucket() {
		return bucket;
	}

	public String getAccessKey() {
		return accessKey;
	}

	public String getObjectKey() {
		return objectKey;
	}

	public String getCluster() {
		return cluster;
	}

	public int getTosTimeoutSeconds() {
		return tosTimeoutSeconds;
	}

	public String getPsm() {
		return psm;
	}

	public static Builder builder() {
		return new Builder();
	}

	/**
	 * Builder for {@link TosOptions}.
	 */
	public static class Builder {

		private String bucket;
		private String accessKey;
		private String objectKey;
		private String cluster;
		private int tosTimeoutSeconds;
		private String psm;

		private Builder() {
		}

		public Builder setBucket(String bucket) {
			this.bucket = bucket;
			return this;
		}

		public Builder setAccessKey(String accessKey) {
			this.accessKey = accessKey;
			return this;
		}

		public Builder setObjectKey(String objectKey) {
			this.objectKey = objectKey;
			return this;
		}

		public Builder setCluster(String cluster) {
			this.cluster = cluster;
			return this;
		}

		public Builder setTosTimeoutSeconds(int tosTimeoutSeconds) {
			this.tosTimeoutSeconds = tosTimeoutSeconds;
			return this;
		}

		public Builder setPsm(String psm) {
			this.psm = psm;
			return this;
		}

		public TosOptions build() {
			Preconditions.checkNotNull(bucket, "bucket can not be null");
			Preconditions.checkNotNull(accessKey, "accessKey can not be null");
			Preconditions.checkNotNull(objectKey, "objectKey can not be null");
			Preconditions.checkArgument(tosTimeoutSeconds > 0, "tosTimeout must > 0");
			return new TosOptions(
				bucket,
				accessKey,
				objectKey,
				cluster,
				tosTimeoutSeconds,
				psm);
		}
	}
}
