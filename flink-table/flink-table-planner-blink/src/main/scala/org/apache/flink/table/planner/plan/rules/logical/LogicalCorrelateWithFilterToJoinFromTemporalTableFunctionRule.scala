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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.expressions.{FieldReferenceExpression, _}
import org.apache.flink.table.functions.{TemporalTableFunction, TemporalTableFunctionImpl}
import org.apache.flink.table.operations.QueryOperation
import org.apache.flink.table.planner.calcite.FlinkRelBuilder
import org.apache.flink.table.planner.functions.bridging.BridgingSqlFunction
import org.apache.flink.table.planner.functions.sql.FlinkSqlOperatorTable
import org.apache.flink.table.planner.functions.utils.TableSqlFunction
import org.apache.flink.table.planner.plan.utils.TemporalJoinUtil.{makeProcTimeTemporalJoinConditionCall, makeRowTimeTemporalJoinConditionCall}
import org.apache.flink.table.planner.plan.utils.{ExpandTableScanShuttle, RexDefaultVisitor}
import org.apache.flink.table.types.logical.LogicalTypeRoot.TIMESTAMP_WITHOUT_TIME_ZONE
import org.apache.flink.table.types.logical.utils.LogicalTypeChecks.{hasRoot, isProctimeAttribute}
import org.apache.flink.util.Preconditions.checkState

import org.apache.calcite.plan.RelOptRule.{any, none, operand, some}
import org.apache.calcite.plan.hep.HepRelVertex
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelOptSchema}
import org.apache.calcite.rel.{BiRel, RelNode, SingleRel}
import org.apache.calcite.rel.core.{SetOp, TableFunctionScan}
import org.apache.calcite.rel.logical.{LogicalCorrelate, LogicalFilter}
import org.apache.calcite.rex._
import org.apache.calcite.sql.SqlKind

import java.util


/**
 * This is copied from [[LogicalCorrelateToJoinFromTemporalTableFunctionRule]].
 * Changes are:
 * 1. We match LogicalCorrelate -> Filter -> TableFunctionScan, which supports
 *    left outer join on non true condition.
 * 2. We push the Filter into the Join condition.
 */
class LogicalCorrelateWithFilterToJoinFromTemporalTableFunctionRule
  extends RelOptRule(
    operand(classOf[LogicalCorrelate],
      some(operand(classOf[RelNode], any()),
        operand(classOf[LogicalFilter],
          operand(classOf[TableFunctionScan], none())))),
    "LogicalCorrelateWithFilterToJoinFromTemporalTableFunctionRule") {

  private def extractNameFromTimeAttribute(timeAttribute: Expression): String = {
    timeAttribute match {
      case f : FieldReferenceExpression
        if hasRoot(f.getOutputDataType.getLogicalType, TIMESTAMP_WITHOUT_TIME_ZONE) =>
        f.getName
      case _ => throw new ValidationException(
        s"Invalid timeAttribute [$timeAttribute] in TemporalTableFunction")
    }
  }

  private def isProctimeReference(temporalTableFunction: TemporalTableFunctionImpl): Boolean = {
    val fieldRef = temporalTableFunction.getTimeAttribute.asInstanceOf[FieldReferenceExpression]
    isProctimeAttribute(fieldRef.getOutputDataType.getLogicalType)
  }

  private def extractNameFromPrimaryKeyAttribute(expression: Expression): String = {
    expression match {
      case f: FieldReferenceExpression =>
        f.getName
      case _ => throw new ValidationException(
        s"Unsupported expression [$expression] as primary key. " +
          s"Only top-level (not nested) field references are supported.")
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val logicalCorrelate: LogicalCorrelate = call.rel(0)
    val leftNode: RelNode = call.rel(1)
    val filter: LogicalFilter = call.rel(2)
    val rightTableFunctionScan: TableFunctionScan = call.rel(3)

    if (!allEqualCondition(filter.getCondition)) {
      throw new ValidationException(s"Currently only equal conditions can be used as temporal " +
        s"table function join condition.")
    }

    val cluster = logicalCorrelate.getCluster

    new GetTemporalTableFunctionCall2(cluster.getRexBuilder, leftNode)
      .visit(rightTableFunctionScan.getCall) match {
      case None =>
      // Do nothing and handle standard TableFunction
      case Some(TemporalTableFunctionCall2(
      rightTemporalTableFunction: TemporalTableFunctionImpl, leftTimeAttribute)) =>

        // If TemporalTableFunction was found, rewrite LogicalCorrelate to TemporalJoin
        val underlyingHistoryTable: QueryOperation = rightTemporalTableFunction
          .getUnderlyingHistoryTable
        val rexBuilder = cluster.getRexBuilder

        val relBuilder = FlinkRelBuilder.of(cluster, getRelOptSchema(leftNode))
        val temporalTable: RelNode = relBuilder.queryOperation(underlyingHistoryTable).build()
        // expand QueryOperationCatalogViewTable in TableScan
        val shuttle = new ExpandTableScanShuttle
        val rightNode = temporalTable.accept(shuttle)

        val rightTimeIndicatorExpression = createRightExpression(
          rexBuilder,
          leftNode,
          rightNode,
          extractNameFromTimeAttribute(rightTemporalTableFunction.getTimeAttribute))

        val rightPrimaryKeyExpression = createRightExpression(
          rexBuilder,
          leftNode,
          rightNode,
          extractNameFromPrimaryKeyAttribute(rightTemporalTableFunction.getPrimaryKey))

        relBuilder.push(leftNode)
        relBuilder.push(rightNode)

        val condition =
          if (isProctimeReference(rightTemporalTableFunction)) {
            makeProcTimeTemporalJoinConditionCall(
              rexBuilder,
              leftTimeAttribute,
              rightPrimaryKeyExpression)
          } else {
            makeRowTimeTemporalJoinConditionCall(
              rexBuilder,
              leftTimeAttribute,
              rightTimeIndicatorExpression,
              rightPrimaryKeyExpression)
          }

        val filterCondition = transformCondition(
          rexBuilder, filter.getCondition, leftNode, rightNode)

        val finalCondition = rexBuilder.makeCall(FlinkSqlOperatorTable.AND,
          condition, filterCondition)
        relBuilder.join(logicalCorrelate.getJoinType, finalCondition)

        call.transformTo(relBuilder.build())
    }
  }

  private def transformCondition(
      rexBuilder: RexBuilder,
      rexNode: RexNode,
      leftNode: RelNode,
      rightNode: RelNode): RexNode = {
    rexNode match {
      case call: RexCall if call.getKind == SqlKind.AND =>
        rexBuilder.makeCall(FlinkSqlOperatorTable.AND,
          transformCondition(rexBuilder, call.getOperands.get(0), leftNode, rightNode),
          transformCondition(rexBuilder, call.getOperands.get(1), leftNode, rightNode))
      case call: RexCall if call.getKind == SqlKind.EQUALS =>
        val removeCorrelationVariable = new RexShuttle {
          override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
            val key = fieldAccess.getField.getKey
            val index = leftNode.getRowType.getField(key, true, false).getIndex
            rexBuilder.makeInputRef(leftNode.getRowType.getFieldList.get(index).getType, index)
          }

          override def visitInputRef(inputRef: RexInputRef): RexNode = {
            rexBuilder.makeInputRef(
              rightNode.getRowType.getFieldList.get(inputRef.getIndex).getType,
              leftNode.getRowType.getFieldCount + inputRef.getIndex)
          }
        }
        val newOperands = new util.ArrayList[RexNode]()
        for (i <- 0 until call.getOperands.size()) {
          val operand = call.getOperands.get(i)
          newOperands.add(operand.accept(removeCorrelationVariable))
        }
        rexBuilder.makeCall(FlinkSqlOperatorTable.EQUALS, newOperands)
      case _ => throw new ValidationException("only 'EQUALS condition connected with AND' " +
        "is supported when join with temporal table function")
    }
  }

  private def allEqualCondition(rexNode: RexNode): Boolean = {
    rexNode match {
      case call: RexCall if call.getKind == SqlKind.AND =>
        allEqualCondition(call.getOperands.get(0)) &&
          allEqualCondition(call.getOperands.get(1))
      case call: RexCall if call.getKind == SqlKind.EQUALS =>
        true
      case _ => false
    }
  }

  private def createRightExpression(
    rexBuilder: RexBuilder,
    leftNode: RelNode,
    rightNode: RelNode,
    field: String): RexNode = {
    val rightReferencesOffset = leftNode.getRowType.getFieldCount
    val rightDataTypeField = rightNode.getRowType.getField(field, false, false)
    rexBuilder.makeInputRef(
      rightDataTypeField.getType, rightReferencesOffset + rightDataTypeField.getIndex)
  }

  /**
    * Gets [[RelOptSchema]] from the leaf [[RelNode]] which holds a non-null [[RelOptSchema]].
    */
  private def getRelOptSchema(relNode: RelNode): RelOptSchema = relNode match {
    case hep: HepRelVertex => getRelOptSchema(hep.getCurrentRel)
    case single: SingleRel => getRelOptSchema(single.getInput)
    case bi: BiRel => getRelOptSchema(bi.getLeft)
    case set: SetOp => getRelOptSchema(set.getInput(0))
    case _ => relNode.getTable.getRelOptSchema
  }
}

object LogicalCorrelateWithFilterToJoinFromTemporalTableFunctionRule {
  val INSTANCE: RelOptRule = new LogicalCorrelateWithFilterToJoinFromTemporalTableFunctionRule
}

/**
  * Simple pojo class for extracted [[TemporalTableFunction]] with time attribute
  * extracted from RexNode with [[TemporalTableFunction]] call.
  */
case class TemporalTableFunctionCall2(
  var temporalTableFunction: TemporalTableFunction,
  var timeAttribute: RexNode) {
}

/**
  * Find [[TemporalTableFunction]] call and run [[CorrelatedFieldAccessRemoval2]] on it's operand.
  */
class GetTemporalTableFunctionCall2(
  var rexBuilder: RexBuilder,
  var leftSide: RelNode)
  extends RexVisitorImpl[TemporalTableFunctionCall2](false) {

  def visit(node: RexNode): Option[TemporalTableFunctionCall2] = {
    val result = node.accept(this)
    if (result == null) {
      return None
    }
    Some(result)
  }

  override def visitCall(rexCall: RexCall): TemporalTableFunctionCall2 = {
    val functionDefinition = rexCall.getOperator match {
      case tsf: TableSqlFunction => tsf.udtf
      case bsf: BridgingSqlFunction => bsf.getDefinition
      case _ => return null
    }

    if (!functionDefinition.isInstanceOf[TemporalTableFunction]) {
      return null
    }
    val temporalTableFunction =
      functionDefinition.asInstanceOf[TemporalTableFunctionImpl]

    checkState(
      rexCall.getOperands.size().equals(1),
      "TemporalTableFunction call [%s] must have exactly one argument",
      rexCall)
    val correlatedFieldAccessRemoval =
      new CorrelatedFieldAccessRemoval2(temporalTableFunction, rexBuilder, leftSide)
    TemporalTableFunctionCall2(
      temporalTableFunction,
      rexCall.getOperands.get(0).accept(correlatedFieldAccessRemoval))
  }
}

/**
  * This converts field accesses like `$cor0.o_rowtime` to valid input references
  * for join condition context without `$cor` reference.
  */
class CorrelatedFieldAccessRemoval2(
  var temporalTableFunction: TemporalTableFunctionImpl,
  var rexBuilder: RexBuilder,
  var leftSide: RelNode) extends RexDefaultVisitor[RexNode] {

  override def visitFieldAccess(fieldAccess: RexFieldAccess): RexNode = {
    val leftIndex = leftSide.getRowType.getFieldList.indexOf(fieldAccess.getField)
    if (leftIndex < 0) {
      throw new IllegalStateException(
        s"Failed to find reference to field [${fieldAccess.getField}] in node [$leftSide]")
    }
    rexBuilder.makeInputRef(leftSide, leftIndex)
  }

  override def visitInputRef(inputRef: RexInputRef): RexNode = {
    inputRef
  }

  override def visitNode(rexNode: RexNode): RexNode = {
    throw new ValidationException(
      s"Unsupported argument [$rexNode] " +
        s"in ${classOf[TemporalTableFunction].getSimpleName} call of " +
        s"[${temporalTableFunction.getUnderlyingHistoryTable}] table")
  }
}
