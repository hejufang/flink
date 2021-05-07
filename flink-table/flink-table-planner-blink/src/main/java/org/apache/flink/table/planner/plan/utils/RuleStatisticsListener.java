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

package org.apache.flink.table.planner.plan.utils;

import org.apache.flink.api.java.tuple.Tuple3;

import org.apache.calcite.plan.RelOptListener;

import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Listener for counting the statistics of each rule. Only enabled under DEBUG level.
 */
public class RuleStatisticsListener implements RelOptListener {
	private long beforeTimestamp;
	// key: rule name, value.f0: production succeeded, value.f1: attempt counts, value.f2: time elapsed
	private final Map<String, Tuple3<Long, Long, Long>> ruleStatistics;

	public RuleStatisticsListener() {
		ruleStatistics = new HashMap<>();
	}

	@Override
	public void relEquivalenceFound(RelEquivalenceEvent event) {}

	@Override
	public void ruleAttempted(RuleAttemptedEvent event) {
		if (event.isBefore()) {
			this.beforeTimestamp = System.nanoTime();
		} else {
			long elapsed = (System.nanoTime() - this.beforeTimestamp) / 1000;
			String rule = event.getRuleCall().getRule().toString();
			if (ruleStatistics.containsKey(rule)) {
				Tuple3<Long, Long, Long> p = ruleStatistics.get(rule);
				ruleStatistics.put(rule, Tuple3.of(p.f0, p.f1 + 1, p.f2 + elapsed));
			} else {
				ruleStatistics.put(rule, Tuple3.of(0L, 1L, elapsed));
			}
		}
	}

	@Override
	public void ruleProductionSucceeded(RuleProductionEvent event) {
		if (!event.isBefore()) {
			String rule = event.getRuleCall().getRule().toString();
			if (ruleStatistics.containsKey(rule)) {
				Tuple3<Long, Long, Long> p = ruleStatistics.get(rule);
				ruleStatistics.put(rule, Tuple3.of(p.f0 + 1, p.f1, p.f2));
			} else {
				ruleStatistics.put(rule, Tuple3.of(1L, 0L, 0L));
			}
		}
	}

	@Override
	public void relDiscarded(RelDiscardedEvent event) {}

	@Override
	public void relChosen(RelChosenEvent event) {}

	public String dumpStatistic() {
		// Sort rules by number of attempts descending, then by rule elapsed time descending,
		// then by rule name ascending.
		List<Map.Entry<String, Tuple3<Long, Long, Long>>> list =
				new ArrayList<>(this.ruleStatistics.entrySet());
		Collections.sort(list,
				(left, right) -> {
					int res = right.getValue().f1.compareTo(left.getValue().f1);
					if (res == 0) {
						res = right.getValue().f2.compareTo(left.getValue().f2);
					}
					if (res == 0) {
						res = left.getKey().compareTo(right.getKey());
					}
					return res;
				});

		// Print out rule attempts and time
		StringBuilder sb = new StringBuilder();
		sb.append(String.format(Locale.ROOT, "%n%-70s%20s%20s%20s%n",
				"Rules", "Production Succeeded", "Attempts", "Time (us)"));
		NumberFormat usFormat = NumberFormat.getNumberInstance(Locale.US);
		for (Map.Entry<String, Tuple3<Long, Long, Long>> entry : list) {
			sb.append(
					String.format(Locale.ROOT, "%-70s%20s%20s%20s%n",
							entry.getKey(),
							usFormat.format(entry.getValue().f0),
							usFormat.format(entry.getValue().f1),
							usFormat.format(entry.getValue().f2)));
		}
		return sb.toString();
	}
}
