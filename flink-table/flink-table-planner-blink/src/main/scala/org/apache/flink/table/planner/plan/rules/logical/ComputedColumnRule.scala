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

package org.apache.flink.table.planner.plan.rules.logical

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.rel.logical.LogicalTableScan
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.planner.catalog.ComputedColumnCatalogTableImpl
import org.apache.flink.table.planner.plan.nodes.calcite.LogicalWatermarkAssigner
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.types.Row

/**
  * Rule that deals with ComputedColumn, changes LogicalTableScan to
  * LogicalTableScan -> LogicalProject -> LogicalWatermarkAssigner
  */
class ComputedColumnRule extends RelOptRule(
    operand(classOf[LogicalTableScan], any),
    "CatalogTableToStreamTableSource") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val rel = call.rel(0).asInstanceOf[LogicalTableScan]
    val table = rel.getTable.unwrap(classOf[TableSourceTable[Row]])
    if (table != null) {
      val catalogTable = table.catalogTable
      if (catalogTable != null) {
        return catalogTable.isInstanceOf[ComputedColumnCatalogTableImpl]
      }
    }
    false
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val oldRel = call.rel(0).asInstanceOf[LogicalTableScan]
    val tableSourceTable = oldRel.getTable.unwrap(classOf[TableSourceTable[Row]])
    val catalogTable = tableSourceTable.catalogTable.asInstanceOf[ComputedColumnCatalogTableImpl]

    // null catalogTable to prevent optimization loop.
    tableSourceTable.catalogTable = null
    val logicalTableScan = LogicalTableScan.create(oldRel.getCluster, oldRel.getTable)

    val relBuilder = call.builder()
    relBuilder.push(logicalTableScan)
    val keys = new java.util.ArrayList[String]()
    val values = new java.util.ArrayList[RexNode]()
    catalogTable.getSchema.getFieldNames.foreach(key => {
      keys.add(key)
      values.add(catalogTable.getComputedColumns.get(key))
    })
    relBuilder.project(values, keys, false)

    val project = relBuilder.build()

    if (catalogTable.hasWatermark) {
      val newRel = new LogicalWatermarkAssigner(
        oldRel.getCluster,
        oldRel.getTraitSet,
        project,
        Option.apply(catalogTable.getRowtimeIndex),
        Option.apply(catalogTable.getDelayOffset)
      )
      call.transformTo(newRel)
    } else {
      call.transformTo(project)
    }
  }
}

object ComputedColumnRule {
  val INSTANCE = new ComputedColumnRule()
}
