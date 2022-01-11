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
package org.apache.flink.table.planner.plan.utils

import org.apache.flink.api.connector.source.HybridSourceInfo
import org.apache.flink.table.api.TableException
import org.apache.flink.table.catalog.CatalogTableImpl
import org.apache.flink.table.planner.catalog.{QueryOperationCatalogViewTable, SqlCatalogViewTable}
import org.apache.flink.table.planner.plan.schema.{CatalogSourceTable, LegacyCatalogSourceTable, LegacyTableSourceTable, TableSourceTable}
import com.google.common.collect.Sets
import org.apache.calcite.plan.ViewExpanders
import org.apache.calcite.rel.core.{TableFunctionScan, TableScan}
import org.apache.calcite.rel.logical._
import org.apache.calcite.rel.{RelNode, RelShuttle, RelShuttleImpl}
import org.apache.calcite.rex.{RexNode, RexShuttle, RexSubQuery}

import java.util
import java.util.Collections

import scala.collection.JavaConversions._

class DefaultRelShuttle extends RelShuttle {

  override def visit(rel: RelNode): RelNode = {
    var change = false
    val newInputs = rel.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change) {
      rel.copy(rel.getTraitSet, newInputs)
    } else {
      rel
    }
  }

  override def visit(intersect: LogicalIntersect): RelNode = visit(intersect.asInstanceOf[RelNode])

  override def visit(union: LogicalUnion): RelNode = visit(union.asInstanceOf[RelNode])

  override def visit(aggregate: LogicalAggregate): RelNode = visit(aggregate.asInstanceOf[RelNode])

  override def visit(minus: LogicalMinus): RelNode = visit(minus.asInstanceOf[RelNode])

  override def visit(sort: LogicalSort): RelNode = visit(sort.asInstanceOf[RelNode])

  override def visit(`match`: LogicalMatch): RelNode = visit(`match`.asInstanceOf[RelNode])

  override def visit(exchange: LogicalExchange): RelNode = visit(exchange.asInstanceOf[RelNode])

  override def visit(scan: TableScan): RelNode = visit(scan.asInstanceOf[RelNode])

  override def visit(scan: TableFunctionScan): RelNode = visit(scan.asInstanceOf[RelNode])

  override def visit(values: LogicalValues): RelNode = visit(values.asInstanceOf[RelNode])

  override def visit(filter: LogicalFilter): RelNode = visit(filter.asInstanceOf[RelNode])

  override def visit(project: LogicalProject): RelNode = visit(project.asInstanceOf[RelNode])

  override def visit(join: LogicalJoin): RelNode = visit(join.asInstanceOf[RelNode])

  override def visit(correlate: LogicalCorrelate): RelNode = visit(correlate.asInstanceOf[RelNode])

  override def visit(modify: LogicalTableModify): RelNode = visit(modify.asInstanceOf[RelNode])
}

/**
  * Convert all [[QueryOperationCatalogViewTable]]s (including tables in [[RexSubQuery]])
  * to to a relational expression.
  */
class ExpandTableScanShuttle extends RelShuttleImpl {

  /**
    * Override this method to use `replaceInput` method instead of `copy` method
    * if any children change. This will not change any output of LogicalTableScan
    * when LogicalTableScan is replaced with RelNode tree in its RelTable.
    */
  override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
    stack.push(parent)
    try {
      val child2 = child.accept(this)
      if (child2 ne child) {
        parent.replaceInput(i, child2)
      }
      parent
    } finally {
      stack.pop
    }
  }

  override def visit(filter: LogicalFilter): RelNode = {
    val newCondition = filter.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne filter.getCondition) {
      val newFilter = filter.copy(filter.getTraitSet, filter.getInput, newCondition)
      super.visit(newFilter)
    } else {
      super.visit(filter)
    }
  }

  override def visit(project: LogicalProject): RelNode = {
    val shuttle = new ExpandTableScanInSubQueryShuttle
    var changed = false
    val newProjects = project.getProjects.map {
      project =>
        val newProject = project.accept(shuttle)
        if (newProject ne project) {
          changed = true
        }
        newProject
    }
    if (changed) {
      val newProject = project.copy(
        project.getTraitSet, project.getInput, newProjects, project.getRowType)
      super.visit(newProject)
    } else {
      super.visit(project)
    }
  }

  override def visit(join: LogicalJoin): RelNode = {
    val newCondition = join.getCondition.accept(new ExpandTableScanInSubQueryShuttle)
    if (newCondition ne join.getCondition) {
      val newJoin = join.copy(
        join.getTraitSet, newCondition, join.getLeft, join.getRight,
        join.getJoinType, join.isSemiJoinDone)
      super.visit(newJoin)
    } else {
      super.visit(join)
    }
  }

  class ExpandTableScanInSubQueryShuttle extends RexShuttle {
    override def visitSubQuery(subQuery: RexSubQuery): RexNode = {
      val newRel = subQuery.rel.accept(ExpandTableScanShuttle.this)
      var changed = false
      val newOperands = subQuery.getOperands.map { op =>
        val newOp = op.accept(ExpandTableScanInSubQueryShuttle.this)
        if (op ne newOp) {
          changed = true
        }
        newOp
      }

      var newSubQuery = subQuery
      if (newRel ne newSubQuery.rel) {
        newSubQuery = newSubQuery.clone(newRel)
      }
      if (changed) {
        newSubQuery = newSubQuery.clone(newSubQuery.getType, newOperands)
      }
      newSubQuery
    }
  }

  /**
    * Converts [[LogicalTableScan]] the result [[RelNode]] tree
    * by calling toRel
    */
  override def visit(scan: TableScan): RelNode = {
    scan match {
      case tableScan: LogicalTableScan =>
        val table = tableScan.getTable
        table match {
          case viewTable: SqlCatalogViewTable =>
            val rel = viewTable.convertToRelReuse(
              ViewExpanders.simpleContext(tableScan.getCluster, tableScan.getHints))
            rel.accept(this)
          case _: CatalogSourceTable[_] | _: QueryOperationCatalogViewTable |
              _: LegacyCatalogSourceTable[_] =>
            val rel = table.toRel(
              ViewExpanders.simpleContext(tableScan.getCluster, tableScan.getHints))
            rel.accept(this)
          case _ => tableScan
        }
      case otherScan => otherScan
    }
  }
}

/**
  * Rewrite same rel object to different rel objects.
  *
  * <p>e.g.
  * {{{
  *      Join                       Join
  *     /    \                     /    \
  * Filter1 Filter2     =>     Filter1 Filter2
  *     \   /                     |      |
  *      Scan                  Scan1    Scan2
  * }}}
  * After rewrote, Scan1 and Scan2 are different object but have same digest.
  */
class SameRelObjectShuttle extends DefaultRelShuttle {
  private val visitedNodes = Sets.newIdentityHashSet[RelNode]()

  override def visit(node: RelNode): RelNode = {
    val visited = !visitedNodes.add(node)
    var change = false
    val newInputs = node.getInputs.map {
      input =>
        val newInput = input.accept(this)
        change = change || (input ne newInput)
        newInput
    }
    if (change || visited) {
      node.copy(node.getTraitSet, newInputs)
    } else {
      node
    }
  }
}

/**
 * This relies on that views are not expanded, and after this shuttle, views will be expanded
 * which is the same as {@link ExpandTableScanShuttle}.
 */
class PropagateHybridSourceShuttle extends RelShuttleImpl {

  override def visitChild(parent: RelNode, i: Int, child: RelNode): RelNode = {
    stack.push(parent)
    try {
      val child2 = child.accept(this)
      if (child2 ne child) {
        parent.replaceInput(i, child2)
      }
      parent
    } finally {
      stack.pop
    }
  }

  override def visit(scan: TableScan): RelNode = {
    scan.getTable.unwrap(classOf[SqlCatalogViewTable]) match {
      case viewTable: SqlCatalogViewTable =>
        val hybridSourceInfo = viewTable.getCatalogView.getViewHybridSourceInfo
        if (hybridSourceInfo.isPresent) {
          val union = viewTable.convertToRelReuse(
            ViewExpanders.simpleContext(scan.getCluster, scan.getHints)).asInstanceOf[LogicalUnion]
          val left = union.getInput(0).asInstanceOf[TableScan]
          val right = union.getInput(1).asInstanceOf[TableScan]
          val leftRel = new ExpandTableScanShuttle().visit(left)
          val rightRel = new ExpandTableScanShuttle().visit(right)

          val leftRes = replaceHybridSourceInfo(leftRel, hybridSourceInfo.get().copy(true))
          val rightRes = replaceHybridSourceInfo(rightRel, hybridSourceInfo.get().copy(false))
          LogicalUnion.create(util.Arrays.asList(leftRes, rightRes), true)
        } else {
          // TODO: give correct context.
          visit(viewTable.convertToRelReuse(
            ViewExpanders.simpleContext(scan.getCluster, scan.getHints)))
        }
      case _ => scan
    }
  }

  def replaceHybridSourceInfo(relNode: RelNode, hybridSourceInfo: HybridSourceInfo): RelNode = {
    if (relNode.isInstanceOf[TableScan]) {
      relNode.getTable match {
        case tableSourceTable: TableSourceTable =>
          val catalogTable = tableSourceTable.catalogTable
          val newCatalogTable = new CatalogTableImpl(
            catalogTable.getSchema,
            catalogTable.getPartitionKeys,
            catalogTable.getOptions,
            catalogTable.getComment,
            hybridSourceInfo
          )
          val newTableSourceTable = tableSourceTable.copy(newCatalogTable)
          LogicalTableScan.create(
            relNode.getCluster,
            newTableSourceTable,
            relNode.asInstanceOf[TableScan].getHints)
        case legacyTableSourceTable: LegacyTableSourceTable[_] =>
          val catalogTable = legacyTableSourceTable.catalogTable
          val newCatalogTable = new CatalogTableImpl(
            catalogTable.getSchema,
            catalogTable.getPartitionKeys,
            catalogTable.getOptions,
            catalogTable.getComment,
            hybridSourceInfo
          )
          val newLegacyTableSourceTable = legacyTableSourceTable.copy(newCatalogTable)
          LogicalTableScan.create(
            relNode.getCluster,
            newLegacyTableSourceTable,
            relNode.asInstanceOf[TableScan].getHints)
        case other => throw new TableException(
          String.format("Unknown RelOptTable type '%s'.", other.getClass))
      }
    } else {
      if (relNode.getInputs.size() != 1) {
        throw new TableException("Currently only one input operations are supported " +
          "in hybrid source.")
      }
      val result = replaceHybridSourceInfo(relNode.getInput(0), hybridSourceInfo)
      relNode.copy(relNode.getTraitSet, Collections.singletonList(result))
    }
  }
}
