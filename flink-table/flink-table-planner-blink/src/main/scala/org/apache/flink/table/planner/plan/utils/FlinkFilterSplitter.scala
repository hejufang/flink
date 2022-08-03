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

import org.apache.calcite.rex.{RexBuilder, RexCall, RexInputRef, RexLiteral, RexNode, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable

import scala.collection.JavaConverters._

/**
 * FlinkFilterSplitter.
 */
object FlinkFilterSplitter {
  /**
   * Split input expressions to two input expression.
   *
   * <p>For example exprInput: (a > 0 and b > 0) and (c > 0 and a < 10) and (c < 10 or b > 10).
   * If a with index 0, b with index 1, c with index 2 in the relBuilder.
   * We want to split the expressions to two expressions as expr1 and expr2.
   * And (expr1 and expr2) is equivalent to exprInput.
   * We required expr1 don't contains any expression that index >= maxFieldIndex
   * If the maxFieldIndex is 2.
   * ExprInput will split to: (a > 0 and b > 0 and a < 10), (c > 0 and (c < 10 or b > 10))
   * </p>
   *
   * @param exprInput input expressions
   * @param maxFieldIndex returns filterMatches shouldn't contains any
   *                      expression include InputRef.index >= maxFieldIndex
   * @param relBuilder RexBuilder
   * @return (filterMatches, remainingFilters)
   */
  def splitFilter(
      exprInput: RexNode,
      maxFieldIndex: Int,
      relBuilder: RexBuilder): (Option[RexNode], Option[RexNode]) =  {
    val filterSplitter = new FlinkFilterSplitter(maxFieldIndex)
    val (filterMatches, remainingFilter) = exprInput.accept(filterSplitter)
    (createRexNode(filterMatches, relBuilder), createRexNode(remainingFilter, relBuilder))
  }

  private def createRexNode(nodes: Seq[RexNode], relBuilder: RexBuilder): Option[RexNode] = {
    if (nodes.isEmpty) {
      None
    } else {
      Some(RexUtil.composeConjunction(relBuilder, nodes.asJava))
    }
  }
}

/**
 * FlinkFilterSplitter.
 */
private class FlinkFilterSplitter(maxFieldIndex: Int)
    extends RexDefaultVisitor[(Seq[RexNode], Seq[RexNode])] {

  override def visitCall(rexCall: RexCall): (Seq[RexNode], Seq[RexNode]) = {
    rexCall.getOperator match {
      case SqlStdOperatorTable.AND =>
        rexCall.getOperands.asScala.map(
          op => op.accept(this)
        ).reduce((x, y) => {
          (x._1 ++ y._1, x._2 ++ y._2)
        })
      case _ =>
        for (op <- rexCall.getOperands.asScala) {
          val (_, remainingFilter) = op.accept(this)
          if (remainingFilter.nonEmpty) {
            return (Seq(), Seq(rexCall))
          }
        }
        (Seq(rexCall), Seq())
    }
  }

  override def visitInputRef(inputRef: RexInputRef): (Seq[RexNode], Seq[RexNode]) = {
    if (inputRef.getIndex < maxFieldIndex) {
      (Seq(inputRef), Seq())
    } else {
      (Seq(), Seq(inputRef))
    }
  }

  override def visitLiteral(literal: RexLiteral): (Seq[RexNode], Seq[RexNode]) = {
    (Seq(literal), Seq())
  }

  override def visitNode(rexNode: RexNode): (Seq[RexNode], Seq[RexNode]) = {
    (Seq(), Seq(rexNode))
  }
}
