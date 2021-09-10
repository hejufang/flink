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

package org.apache.flink.runtime.state.tracker;

/**
 * The enumerated type of backend.
 */
public enum BackendType {
	OPERATOR_STATE_BACKEND("OPERATOR_STATE_BACKEND"),
	HEAP_STATE_BACKEND("HEAP_STATE_BACKEND"),
	INCREMENTAL_ROCKSDB_STATE_BACKEND("INCREMENTAL_ROCKSDB_STATE_BACKEND"),
	FULL_ROCKSDB_STATE_BACKEND("FULL_ROCKSDB_STATE_BACKEND"),
	NO_STATE_WITH_ROCKSDB_BACKEND("NO_STATE_WITH_ROCKSDB_BACKEND"),
	MOCK_STATE_BACKEND("MOCK_STATE_BACKEND"),
	UNKOWN("UNKOWN");


	private final String backendType;

	BackendType(String backendType) {
		this.backendType = backendType;
	}

	public String getBackendType() {
		return backendType;
	}
}
