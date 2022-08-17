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
import org.apache.calcite.rel.{AbstractRelNode, RelNode}
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLocalHashAggregate, BatchExecLocalSortAggregate, BatchExecUnion}

import java.util

/**
  * Planner rule that tries to push a local SortAggregate or HashAggregate operator below the
  * Union All node.
  *
  * Note: When we push a local SortAggregate or HashAggregate operator, we need to adjust both its
  * parent node and child node (or its inpput node).  A GROUP-BY clause usually generates
  * a HashAggregate operator
  */

abstract class PushLocalAggregateAcrossUnionAllRuleBase(
    operand: RelOptRuleOperand,
    description: String)
extends RelOptRule(operand, description) {

  protected def isMatch(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_PUSH_ACROSS_UNION_ALL_ENABLED)) {
      return false
    }

    // must be UNION-ALL in order to match with this rule.
    val union: BatchExecUnion = call.rel(2)
    union.all
  }

  protected def pushDownLocalAggregate(call: RelOptRuleCall): Unit = {
    // this method transforms the query tree so that a Local Aggregate node moves to
    // a position immediately below the Union node.  The local aggregate node can be either
    // BatchExecLocalSortAggregate or BatchExecLocalHashAggregate node.
    val unionNode: BatchExecUnion = call.rel(2)

    // need to create two new local aggregate nodes for the inputs of the union node.
    // Each of the new Local Aggregate nodes will have union's old input as its
    // input respectively.
    val localAggNode = call.rel(1).asInstanceOf[AbstractRelNode]

    val inputsList1 = new util.ArrayList[RelNode]
    inputsList1.add(unionNode.asInstanceOf[RelNode].getInput(0))
    val newlocalAgg1 = localAggNode.copy(localAggNode.getTraitSet, inputsList1)

    val inputsList2 = new util.ArrayList[RelNode]
    inputsList2.add(unionNode.asInstanceOf[RelNode].getInput(1))
    val newlocalAgg2 = localAggNode.copy(localAggNode.getTraitSet, inputsList2)

    // union node will have the two new local aggregate nodes as its input.
    unionNode.asInstanceOf[RelNode].replaceInput(0, newlocalAgg1)
    unionNode.asInstanceOf[RelNode].replaceInput(1, newlocalAgg2)

    // The Local Aggregate node has been pushed down.
    // The Union node has been pushed up.
    // The first node will have the union-all node as its input
    val firstNode = call.rel(0).asInstanceOf[RelNode]
    firstNode.replaceInput(0, unionNode)
    call.transformTo(firstNode)
  }

}
