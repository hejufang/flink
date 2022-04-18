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

package org.apache.flink.table.plan;

import org.apache.flink.table.api.TableConfig;

import org.apache.flink.shaded.guava18.com.google.common.cache.Cache;
import org.apache.flink.shaded.guava18.com.google.common.cache.CacheBuilder;

import org.apache.commons.lang3.StringUtils;

import java.util.Map;
import java.util.Objects;
import java.util.Optional;

import static org.apache.flink.util.Preconditions.checkArgument;

/**
 * Cache the plan for given sql.
 */
public class PlanCacheManager<V> {
	private final Cache<PlanKey, V> planCache;

	public PlanCacheManager(long maximumCapacity) {
		planCache = CacheBuilder.newBuilder()
			.maximumSize(maximumCapacity)
			.build();
	}

	public Optional<V> getPlan(String sql, Map<String, Long> lsn, TableConfig tableConfig) {
		checkArgument(StringUtils.isNotEmpty(sql));
		V value = planCache.getIfPresent(new PlanKey(sql, lsn, tableConfig.hashCode()));
		return value == null ? Optional.empty() : Optional.of(value);
	}

	public void putPlan(String sql, Map<String, Long> lsn, TableConfig tableConfig, V value) {
		checkArgument(sql != null && value != null);
		planCache.put(new PlanKey(sql, lsn, tableConfig.hashCode()), value);
	}

	private static class PlanKey {
		// The given query sql.
		private final String sql;
		// The given lsn.
		private final Map<String, Long> lsn;
		// The hash code of the table config.
		private final int configCode;

		public PlanKey(String sql, Map<String, Long> lsn, int configCode) {
			this.sql = sql;
			this.lsn = lsn;
			this.configCode = configCode;
		}

		@Override
		public boolean equals(Object o) {
			if (this == o) {
				return true;
			}
			if (o == null || getClass() != o.getClass()) {
				return false;
			}
			PlanKey that = (PlanKey) o;
			return Objects.equals(lsn, that.lsn) && configCode == that.configCode && Objects.equals(sql, that.sql);
		}

		@Override
		public int hashCode() {
			return Objects.hash(sql, lsn, configCode);
		}
	}
}
