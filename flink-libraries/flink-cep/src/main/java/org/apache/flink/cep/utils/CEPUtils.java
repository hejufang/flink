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

package org.apache.flink.cep.utils;

import org.apache.flink.api.common.state.StateTtlConfig;
import org.apache.flink.api.common.time.Time;

import java.util.concurrent.TimeUnit;

/**
 * CepUtils.
 */
public class CEPUtils {

	public static final String TTL_KEY = "cep.ttl.milliseconds";
	public static final String TTL_DEFAULT_VALUE = "86400000";

	public static StateTtlConfig defaultTtlConfig() {
		return StateTtlConfig.newBuilder(Time.of(1, TimeUnit.DAYS))
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
				.build();
	}

	public static StateTtlConfig defaultTtlConfig(long millionseconds) {
		return StateTtlConfig.newBuilder(Time.of(millionseconds, TimeUnit.MILLISECONDS))
				.cleanupIncrementally(5, false)
				.setUpdateType(StateTtlConfig.UpdateType.OnCreateAndWrite)
				.setStateVisibility(StateTtlConfig.StateVisibility.NeverReturnExpired)
				.build();
	}

	public static String generateUniqueId(String patternId, int hash) {
		return patternId + "_" + String.valueOf(hash);
	}
}
