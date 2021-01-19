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

import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecExchange, BatchExecGroupAggregateBase, BatchExecLegacyTableSourceScan, BatchExecLocalHashAggregate, BatchExecLocalSortAggregate}
import org.apache.flink.table.sources.AggregatableTableSource

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.RelOptRuleCall

/**
 * A [[PushLocalAggIntoLegacyTableSourceScanRuleBase]] tries to push a local hash aggregator
 * or a local sort aggregator without grouping into an [[AggregatableTableSource]].
 *
 * for example: select count(*) from t
 * The physical plan
 *
 * {{{
 * HashAggregate(isMerge=[true], select=[Final_COUNT(count1$0)])
 * +- Exchange(distribution=[single])
 *    +- LocalHashAggregate(select=[Partial_COUNT(*) AS count1$0])
 *       +- BatchExecLegacyTableSourceScan
 * }}}
 *
 * will be rewritten to
 *
 * {{{
 * HashAggregate(isMerge=[true], select=[Final_COUNT(count1$0)])
 * +- Exchange(distribution=[single])
 *    +- BatchExecLegacyTableSourceScan
 * }}}
 **/
class PushLocalAggWithoutSortIntoLegacyTableSourceScanRule
  extends PushLocalAggIntoLegacyTableSourceScanRuleBase(
    operand(classOf[BatchExecExchange],
      operand(classOf[BatchExecGroupAggregateBase],
        operand(classOf[BatchExecLegacyTableSourceScan], none))),
    "PushLocalAggWithoutSortIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    call.rels(1) match {
      case _: BatchExecLocalHashAggregate =>
      case _: BatchExecLocalSortAggregate =>
      case _ => return false
    }
    val aggregate = call.rels(1).asInstanceOf[BatchExecGroupAggregateBase]
    val scan = call.rels(2).asInstanceOf[BatchExecLegacyTableSourceScan]
    isMatch(call, aggregate, scan)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val localHashAgg = call.rels(1).asInstanceOf[BatchExecGroupAggregateBase]
    val oldScan = call.rels(2).asInstanceOf[BatchExecLegacyTableSourceScan]
    pushLocalAggregateIntoScan(call, localHashAgg, oldScan)
  }
}

object PushLocalAggWithoutSortIntoLegacyTableSourceScanRule {
  val INSTANCE = new PushLocalAggWithoutSortIntoLegacyTableSourceScanRule
}
