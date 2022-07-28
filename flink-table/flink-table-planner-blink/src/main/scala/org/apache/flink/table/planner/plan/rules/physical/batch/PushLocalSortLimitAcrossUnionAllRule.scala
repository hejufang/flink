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

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.Sort
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLimit, BatchExecSortLimit, BatchExecUnion}

/**
  * A [[PushLocalSortLimitAcrossUnionAllRule]] tries to push a local SortLimit across the union
  * all operator so that we can reduce records on both of union's input subsets.  If a local
  * SortLimit operator is immediately above a Union-All operator, this rule transforms the query
  * tree so that the SortLimit operator moves to a position immediately below the Union-All
  * operator.
  *
  * for example:
  * SELECT * FROM (
  *   SELECT * FROM MyTable1
  *   UNION ALL
  *   SELECT * FROM MyTable2
  * ) as T
  * ORDER BY id LIMIT 4
  *
  * The physical plan
  *
  * {{{
  * SortLimit(orderBy=[id ASC], offset=[0], fetch=[4], global=[true])
  * +- Exchange(distribution=[single])
  *   +- SortLimit(orderBy=[id ASC], offset=[0], fetch=[4], global=[false])
  *     +- Union(all=[true], union=[name, id, amount, price])
  *        :- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, .....
  *        +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, .....
  *
  * will be rewritten to
  *
  * {{{
  *SortLimit(orderBy=[id ASC], offset=[0], fetch=[4], global=[true])
  *+- Exchange(distribution=[single])
  *   +- Union(all=[true], union=[name, id, amount, price])
  *      :- SortLimit(orderBy=[id ASC], offset=[0], fetch=[4], global=[false])
  *      :  +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, .....
  *      +- SortLimit(orderBy=[id ASC], offset=[0], fetch=[4], global=[false])
  *         +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, .....
  * }}}
  */
class PushLocalSortLimitAcrossUnionAllRule
  extends RelOptRule(
    operand(classOf[RelNode],
      operand(classOf[BatchExecSortLimit],
        operand(classOf[BatchExecUnion], any))),
  "PushLocalSortLimitAcrossUnionAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
        OptimizerConfigOptions.TABLE_OPTIMIZER_PUSH_ACROSS_UNION_ALL_ENABLED)) {
      return false
    }

    call.rels(1) match {
      case _: BatchExecSortLimit =>
        // make sure it is a local SortLimit operator
        val sortLimit: BatchExecSortLimit = call.rel(1)
        if (sortLimit.getIsGlobal) {
          return false
        }

      case _ => return false
    }

    // must be UNION ALL in order to match with this rule.
    val union: BatchExecUnion = call.rel(2)
    union.all
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    // the first node will have the union-all node as its input
    val newUnionAllNode = pushDownLocalSortLimit(call)
    val firstNode = call.rel(0).asInstanceOf[RelNode]
    firstNode.replaceInput(0, newUnionAllNode)
    call.transformTo(firstNode)
  }

  private def pushDownLocalSortLimit(call: RelOptRuleCall): RelNode = {
    val sortLimit: BatchExecSortLimit = call.rel(1)
    val union: BatchExecUnion = call.rel(2)

    // need to create two new local BatchExecSortLimit nodes for the inputs of union.
    // Each of the new SortLimit nodes will have union's old input as its input respectively.
    val sortLimitOffset = sortLimit.asInstanceOf[Sort].offset
    val sortLimitFetch = sortLimit.asInstanceOf[Sort].fetch
    val newSortLimit1 = sortLimit.copy(
      sortLimit.getTraitSet,
      union.getInput(0),
      sortLimit.getCollation,
      sortLimitOffset,
      sortLimitFetch
    )
    val newSortLimit2 = sortLimit.copy(
      sortLimit.getTraitSet,
      union.getInput(1),
      sortLimit.getCollation,
      sortLimitOffset,
      sortLimitFetch
    )

    // union node will have the two new SortLimit nodes as its input.
    union.replaceInputNode(0, newSortLimit1.asInstanceOf[BatchExecSortLimit])
    union.replaceInputNode(1, newSortLimit2.asInstanceOf[BatchExecSortLimit])

    // SortLimit node has been pushed down.  Union node has been pushed up.
    union
  }

}

object PushLocalSortLimitAcrossUnionAllRule {
  val INSTANCE = new PushLocalSortLimitAcrossUnionAllRule
}
