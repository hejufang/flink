/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.table.planner.hint;

import org.apache.flink.table.api.ValidationException;
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase;

import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.RelShuttleImpl;
import org.apache.calcite.rel.SingleRel;
import org.apache.calcite.rel.hint.HintPredicate;
import org.apache.calcite.rel.hint.HintPredicates;
import org.apache.calcite.rel.hint.HintStrategy;
import org.apache.calcite.rel.hint.HintStrategyTable;
import org.apache.calcite.rel.hint.RelHint;
import org.apache.calcite.rel.logical.LogicalJoin;
import org.apache.calcite.sql.SqlHint;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.util.Litmus;
import org.apache.calcite.util.Util;

import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

/**
 * A collection of Flink style {@link HintStrategy}s.
 */
public abstract class FlinkHintStrategies {

	private static String getTableName(RelNode relNode) {
		if (relNode.getTable() != null) {
			if (relNode.getTable() instanceof FlinkPreparingTableBase) {
				return Util.last(((FlinkPreparingTableBase) relNode.getTable()).getNames());
			}
			return Util.last(relNode.getTable().getQualifiedName());
		}
		if (relNode instanceof SingleRel) {
			return getTableName(((SingleRel) relNode).getInput());
		}
		return null;
	}

	private static HintPredicate joinWithFixedTableName() {
		return (hint, rel) -> {
			if (!(rel instanceof LogicalJoin)) {
				return false;
			}
			LogicalJoin join = (LogicalJoin) rel;
			final String hintTableName = hint.kvOptions.get(FlinkHints.HINT_OPTION_TABLE_NAME);
			if (hintTableName == null) {
				throw new ValidationException("Must specific broadcast table or view name");
			}

			final String leftTable = getTableName(join.getLeft());
			if (hintTableName.equals(leftTable)) {
				throw new ValidationException("Current not support broadcast left side of table");
			}

			final String rightTable = getTableName(join.getRight());
			return hintTableName.equals(rightTable);
		};
	}

	/** Customize the {@link HintStrategyTable} which
	 * contains hint strategies supported by Flink. */
	public static HintStrategyTable createHintStrategyTable() {
		return HintStrategyTable.builder()
				// Configure to always throw when we encounter any hint errors
				// (either the non-registered hint or the hint format).
				.errorHandler(Litmus.THROW)
				.hintStrategy(
						FlinkHints.HINT_NAME_OPTIONS,
						HintStrategy.builder(HintPredicates.TABLE_SCAN)
								.optionChecker((hint, errorHandler) ->
										errorHandler.check(hint.kvOptions.size() > 0,
										"Hint [{}] only support non empty key value options",
										hint.hintName))
								.build())
				.hintStrategy(FlinkHints.HINT_NAME_USE_BROADCAST_JOIN,
					HintPredicates.and(HintPredicates.JOIN, joinWithFixedTableName()))
				.build();
	}

	public static RelNode validateAndReplaceFlinkHint(RelNode relNode, SqlNode sqlNode) {
		if (!(sqlNode instanceof SqlSelect)) {
			return relNode;
		}

		List<SqlHint> sqlHints = ((SqlSelect) sqlNode).getHints().getList().stream()
			.filter(n -> n instanceof SqlHint)
			.map(n -> (SqlHint) n)
			.collect(Collectors.toList());
		final Set<String> broadcastTables = sqlHints.stream()
			.filter(hint -> FlinkHints.HINT_NAME_USE_BROADCAST_JOIN.equals(hint.getName()))
			.map(hint -> hint.getOptionKVPairs().get(FlinkHints.HINT_OPTION_TABLE_NAME))
			.collect(Collectors.toSet());
		if (broadcastTables.isEmpty()) {
			return relNode;
		}

		final Set<String> resolvedBroadcastTables = new HashSet<>();
		RelShuttleImpl hintRelShuttle = new RelShuttleImpl() {
			@Override
			public RelNode visit(LogicalJoin join) {
				join.getInputs().forEach(node -> node.accept(this));
				List<RelHint> relHints = join.getHints().stream().map(
					hint -> {
						if (FlinkHints.HINT_NAME_USE_BROADCAST_JOIN.equals(hint.hintName)) {
							resolvedBroadcastTables.add(
								hint.kvOptions.get(FlinkHints.HINT_OPTION_TABLE_NAME));
							return hint.copy(Collections.emptyList());
						}
						return hint;
					}
				).collect(Collectors.toList());

				return relHints.size() > 0 ? join.withHints(relHints) : join;
			}
		};
		relNode = relNode.accept(hintRelShuttle);
		if (!broadcastTables.equals(resolvedBroadcastTables)) {
			throw new ValidationException(String.format(
				"Hints not resolved, all hints is %s, resolved is %s",
				broadcastTables, resolvedBroadcastTables));
		}
		return relNode;
	}
}
