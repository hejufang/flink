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

package org.apache.flink.client.cli;

/**
 * Checkpoint verification result at client end.
 */
public enum CheckpointVerifyResult {
	SUCCESS("success"),
	FAIL_MISS_OPERATOR_ID("miss OperatorID"),
	FAIL_MISMATCH_PARALLELISM("mismatch parallelism"),
	SKIP("skip"),
	ZOOKEEPER_RETRIEVE_FAIL("Zookeeper retrieve fail"),
	HDFS_RETRIEVE_FAIL("HDFS retrieve fail"),
	INVALID_SAVEPOINT_PATH("invalid savepoint path");

	private final String name;

	CheckpointVerifyResult(String name) {
		this.name = name;
	}
}
