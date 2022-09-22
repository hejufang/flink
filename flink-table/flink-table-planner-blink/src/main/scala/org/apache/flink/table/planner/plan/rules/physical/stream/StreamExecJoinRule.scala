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

package org.apache.flink.table.planner.plan.rules.physical.stream

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.hint.FlinkHints
import org.apache.flink.table.planner.hint.FlinkHints._
import org.apache.flink.table.planner.plan.`trait`.{FlinkRelDistribution, ModifyKindSetTrait}
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.{FlinkLogicalJoin, FlinkLogicalRel, FlinkLogicalSnapshot}
import org.apache.flink.table.planner.plan.nodes.physical.stream.StreamExecJoin
import org.apache.flink.table.planner.plan.utils.{IntervalJoinUtil, JoinUtil, TemporalJoinUtil}
import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.hint.RelHint
import org.apache.flink.util.{FlinkRuntimeException, TimeUtils}

import java.util
import java.util.function.Predicate
import scala.collection.JavaConversions._

/**
  * Rule that converts [[FlinkLogicalJoin]] without window bounds in join condition
  * to [[StreamExecJoin]].
  */
class StreamExecJoinRule
  extends RelOptRule(
    operand(classOf[FlinkLogicalJoin],
      operand(classOf[FlinkLogicalRel], any()),
      operand(classOf[FlinkLogicalRel], any())),
    "StreamExecJoinRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val join: FlinkLogicalJoin = call.rel(0)
    if (!join.getJoinType.projectsRight) {
      // SEMI/ANTI join always converts to StreamExecJoin now
      return true
    }
    val left: FlinkLogicalRel = call.rel(1).asInstanceOf[FlinkLogicalRel]
    val right: FlinkLogicalRel = call.rel(2).asInstanceOf[FlinkLogicalRel]
    val tableConfig = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    val joinRowType = join.getRowType

    if (left.isInstanceOf[FlinkLogicalSnapshot]) {
      throw new TableException(
        "Temporal table join only support apply FOR SYSTEM_TIME AS OF on the right table.")
    }

    // this rule shouldn't match temporal table join
    if (right.isInstanceOf[FlinkLogicalSnapshot] ||
      TemporalJoinUtil.containsTemporalJoinCondition(join.getCondition)) {
      return false
    }

    val (windowBounds, remainingPreds) = IntervalJoinUtil.extractWindowBoundsFromPredicate(
      join.getCondition,
      join.getLeft.getRowType.getFieldCount,
      joinRowType,
      join.getCluster.getRexBuilder,
      tableConfig)

    if (windowBounds.isDefined) {
      return false
    }

    // remaining predicate must not access time attributes
    val remainingPredsAccessTime = remainingPreds.isDefined &&
      IntervalJoinUtil.accessesTimeAttribute(remainingPreds.get, joinRowType)

    val isBroadcastJoin = join.getHints.size() match {
      case 1 if HINT_NAME_USE_BROADCAST_JOIN.equals(join.getHints.get(0).hintName) => true
      case _ => false
    }

    val rowTimeAttrInOutput = joinRowType.getFieldList
      .exists(f => FlinkTypeFactory.isRowtimeIndicatorType(f.getType))

    if (rowTimeAttrInOutput && !isBroadcastJoin) {
      throw new TableException(
        "Rowtime attributes must not be in the input rows of a regular join. " +
          "As a workaround you can cast the time attributes of input tables to TIMESTAMP before.")
    }

    // joins require an equality condition
    // or a conjunctive predicate with at least one equality condition
    // and disable outer joins with non-equality predicates(see FLINK-5520)
    // And do not accept a FlinkLogicalTemporalTableSourceScan as right input
    !remainingPredsAccessTime
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val join: FlinkLogicalJoin = call.rel(0)
    val left = join.getLeft
    val right = join.getRight

    def toHashTraitByColumns(
        columns: util.Collection[_ <: Number],
        inputTraitSets: RelTraitSet): RelTraitSet = {
      val distribution = if (columns.isEmpty) {
        FlinkRelDistribution.SINGLETON
      } else {
        FlinkRelDistribution.hash(columns)
      }
      inputTraitSets
        .replace(FlinkConventions.STREAM_PHYSICAL)
        .replace(distribution)
    }

    val joinConfigOption = join.getHints.size() match {
      case 0 => None
      case 1 if HINT_NAME_USE_BROADCAST_JOIN.equals(join.getHints.get(0).hintName) =>
        JoinUtil.JoinConfig.parse(join.getHints.get(0).kvOptions)
      case _ => throw new FlinkRuntimeException("Invalid hints for join")
    }

    val joinInfo = join.analyzeCondition()
    val providedTraitSet = join.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL)

    val (leftRequiredTrait, rightRequiredTrait) = joinConfigOption match {
      case Some(joinConf) if !joinConf.keyByMode =>
        (left.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL),
          right.getTraitSet.replace(FlinkConventions.STREAM_PHYSICAL))
      case _ =>
        (toHashTraitByColumns(joinInfo.leftKeys, left.getTraitSet),
          toHashTraitByColumns(joinInfo.rightKeys, right.getTraitSet))
    }

    val newLeft: RelNode = RelOptRule.convert(left, leftRequiredTrait)
    val newRight: RelNode = RelOptRule.convert(right, rightRequiredTrait)

    val newJoin = new StreamExecJoin(
      join.getCluster,
      providedTraitSet,
      newLeft,
      newRight,
      join.getCondition,
      join.getJoinType,
      joinConfigOption)

    call.transformTo(newJoin)
  }
}

object StreamExecJoinRule {
  val INSTANCE: RelOptRule = new StreamExecJoinRule
}
