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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecExchange, BatchExecLegacyTableSourceScan, BatchExecLocalSortAggregate, BatchExecSort}
import org.apache.flink.table.sources.AggregatableTableSource

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.RelOptRuleCall

/**
 * A [[PushLocalAggIntoLegacyTableSourceScanRuleBase]] tries to push a local sort aggregator
 * with grouping into an [[AggregatableTableSource]].
 *
 * for example: select count(*) from t group by a
 * The physical plan
 *
 * {{{
 * SortAggregate(isMerge=[true], groupBy=[a], select=[a, Final_COUNT(count1$0)])
 * +- Sort(orderBy=[a ASC])
 *    +- Exchange(distribution=[hash[a]])
 *       +- LocalSortAggregate(groupBy=[a], select=[a, Partial_COUNT(*) AS count1$0])
 *          +- Sort(orderBy=[a ASC])
 *             +- BatchExecLegacyTableSourceScan
 * }}}
 *
 * will be rewritten to
 *
 * {{{
 * SortAggregate(isMerge=[true], groupBy=[a], select=[a, Final_COUNT(count1$0)])
 * +- Sort(orderBy=[a ASC])
 *    +- Exchange(distribution=[hash[a]])
 *       +- BatchExecLegacyTableSourceScan
 * }}}
 */
class PushLocalAggWithSortIntoLegacyTableSourceScanRule
  extends PushLocalAggIntoLegacyTableSourceScanRuleBase(
    operand(classOf[BatchExecExchange],
      operand(classOf[BatchExecLocalSortAggregate],
        operand(classOf[BatchExecSort],
          operand(classOf[BatchExecLegacyTableSourceScan], none)))),
    "PushLocalAggWithSortIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    call.rels(1) match {
      case _: BatchExecLocalSortAggregate =>
      case _ => return false
    }
    val aggregate = call.rels(1).asInstanceOf[BatchExecLocalSortAggregate]
    val scan = call.rels(3).asInstanceOf[BatchExecLegacyTableSourceScan]
    isMatch(call, aggregate, scan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val localSortAgg = call.rels(1).asInstanceOf[BatchExecLocalSortAggregate]
    val oldScan = call.rels(3).asInstanceOf[BatchExecLegacyTableSourceScan]
    pushLocalAggregateIntoScan(call, localSortAgg, oldScan)
  }
}

object PushLocalAggWithSortIntoLegacyTableSourceScanRule {
  val INSTANCE = new PushLocalAggWithSortIntoLegacyTableSourceScanRule
}
