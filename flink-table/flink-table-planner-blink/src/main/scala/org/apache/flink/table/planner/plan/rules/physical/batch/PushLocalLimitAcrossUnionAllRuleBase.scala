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

import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptRuleOperand}
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLimit, BatchExecSortLimit, BatchExecUnion}

/**
  * Planner rule that tries to push a local SortLimit or limit operator below Union All node.
  * 
  * Note: When we push a SortLimit or Limit operator, we need to adjust both its parent node and
  * and child node (or its inpput node).
  */
abstract class PushLocalLimitAcrossUnionAllRuleBase(
    operand: RelOptRuleOperand,
    description: String)
extends RelOptRule(operand, description) {

  protected def isMatch(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_PUSH_ACROSS_UNION_ALL_ENABLED)) {
      return false
    }

    call.rels(1) match {
      case _: BatchExecLimit =>
        // make sure it is a local Limit operator
        val limitNode: BatchExecLimit = call.rel(1)
        if (limitNode.getIsGlobal) {
          return false
        }
      case _: BatchExecSortLimit => {
        // make sure it is a local SortLimit operator
        val sortLimit: BatchExecSortLimit = call.rel(1)
        if (sortLimit.getIsGlobal) {
          return false
        }
      }
      case _ => return false
    }

    // must be UNION-ALL in order to match with this rule.
    val union: BatchExecUnion = call.rel(2)
    union.all
  }

  protected def pushDownLocalLimit(call: RelOptRuleCall): Unit = {
    // this method transforms the query tree so that the Limit (or SortLimit) operator moves to a
    // position immediately below the Union-All operator.
    val unionNode: BatchExecUnion = call.rel(2)

    // need to create two new local BatchExecLimit (or BatchExecSortLimit) nodes for the inputs of
    // union. Each of the new limit nodes will have union's old input as its input respectively.
    val limitNode = call.rel(1).asInstanceOf[Sort]
    val limitOffset = limitNode.offset
    val limitFetch = limitNode.fetch
    val newLimit1 = limitNode.copy(
      limitNode.getTraitSet,
      unionNode.asInstanceOf[RelNode].getInput(0),
      limitNode.getCollation,
      limitOffset,
      limitFetch
    )
    val newLimit2 = limitNode.copy(
      limitNode.getTraitSet,
      unionNode.asInstanceOf[RelNode].getInput(1),
      limitNode.getCollation,
      limitOffset,
      limitFetch
    )

    // union node will have the two new Limit (or SortLimit) nodes as its input.
    unionNode.asInstanceOf[RelNode].replaceInput(0, newLimit1)
    unionNode.asInstanceOf[RelNode].replaceInput(1, newLimit2)

    // BatchExecLimit (or BatchExecSortLimit) node has been pushed down.
    // The Union node has been pushed up.
    // The first node will have the union-all node as its input
    val firstNode = call.rel(0).asInstanceOf[RelNode]
    firstNode.replaceInput(0, unionNode)
    call.transformTo(firstNode)
  }

}
