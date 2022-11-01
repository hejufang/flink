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

import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, ExprCodeGenerator, FunctionCodeGenerator}
import org.apache.flink.table.runtime.generated.GeneratedJoinCondition
import org.apache.flink.table.planner.hint.FlinkHints._
import org.apache.flink.table.types.logical.LogicalType
import org.apache.flink.util.{FlinkRuntimeException, TimeUtils}

import org.apache.calcite.plan.RelOptUtil
import org.apache.calcite.rel.RelNode
import org.apache.calcite.rel.core.{Join, JoinInfo}
import org.apache.calcite.rex.{RexBuilder, RexNode}
import org.apache.calcite.util.ImmutableIntList
import org.apache.calcite.util.mapping.IntPair

import java.lang.{Long => JLong}
import java.util

import scala.collection.mutable

/**
  * Util for [[Join]]s.
  */
object JoinUtil {

  case class JoinConfig(
      table: String,
      allowLatencyMs: JLong,
      maxBuildLatencyMs: JLong,
      keyByMode: Boolean,
      timeOffset: JLong) {
    override def toString: String = {
      s"JoinConfig(table=$table, allowLatencyMs=$allowLatencyMs, " +
        s"maxBuildLatencyMs=$maxBuildLatencyMs, keyByMode=$keyByMode, timeOffset=$timeOffset)"
    }
  }

  object JoinConfig {
    def parse(config: util.Map[String, String]): Option[JoinConfig] = {
      val allowLatency: JLong =
        tryGetConfig(x => JLong.valueOf(TimeUtils.parseDuration(x).toMillis),
          HINT_OPTION_DIM_JOIN_ALLOW_LATENCY, config).orNull(null)
      val maxBuildLatencyMs = tryGetConfig(x => JLong.valueOf(TimeUtils.parseDuration(x).toMillis),
          HINT_OPTION_MAX_BUILD_LATENCY, config).get
      val tableName = tryGetConfig(x => x, HINT_OPTION_TABLE_NAME, config).get
      val useKeyBy =
        tryGetConfig(x => x.toBoolean, HINT_OPTION_KEY_BY_MODE, config).getOrElse(false)
      val timeOffset: JLong = tryGetConfig(x =>
          x.charAt(0) match {
            case '+' => JLong.valueOf(TimeUtils.parseDuration(x.substring(1)).toMillis)
            case '-' => -JLong.valueOf(TimeUtils.parseDuration(x.substring(1)).toMillis)
            case _ => JLong.valueOf(TimeUtils.parseDuration(x).toMillis)
          },
          HINT_OPTION_TIME_OFFSET,
          config).getOrElse(0L).asInstanceOf[JLong]
      Some(JoinConfig(tableName, allowLatency, maxBuildLatencyMs, useKeyBy, timeOffset))
    }

    private def tryGetConfig[T](
        func: String => T,
        optionName: String,
        config: util.Map[String, String]): Option[T] = {
      val optionValue = config.get(optionName)
      if (optionValue == null) {
        return None
      }

      try {
        Some(func(config.get(optionName)))
      } catch {
        case e =>
          throw new FlinkRuntimeException(
            s"Hint option $optionName parse string $optionValue failed", e)
      }
    }
  }


  /**
    * Check and get join left and right keys.
    */
  def checkAndGetJoinKeys(
      keyPairs: List[IntPair],
      left: RelNode,
      right: RelNode,
      allowEmptyKey: Boolean = false): (Array[Int], Array[Int]) = {
    // get the equality keys
    val leftKeys = mutable.ArrayBuffer.empty[Int]
    val rightKeys = mutable.ArrayBuffer.empty[Int]
    if (keyPairs.isEmpty) {
      if (allowEmptyKey) {
        (leftKeys.toArray, rightKeys.toArray)
      } else {
        throw new TableException(
          s"Joins should have at least one equality condition.\n" +
            s"\tleft: ${left.toString}\n\tright: ${right.toString}\n" +
            s"please re-check the join statement and make sure there's " +
            "equality condition for join.")
      }
    } else {
      // at least one equality expression
      val leftFields = left.getRowType.getFieldList
      val rightFields = right.getRowType.getFieldList

      keyPairs.foreach { pair =>
        val leftKeyType = leftFields.get(pair.source).getType.getSqlTypeName
        val rightKeyType = rightFields.get(pair.target).getType.getSqlTypeName

        // check if keys are compatible
        if (leftKeyType == rightKeyType) {
          // add key pair
          leftKeys += pair.source
          rightKeys += pair.target
        } else {
          throw new TableException(
            s"Join: Equality join predicate on incompatible types. " +
              s"\tLeft: ${left.toString}\n\tright: ${right.toString}\n" +
              "please re-check the join statement.")
        }
      }
      (leftKeys.toArray, rightKeys.toArray)
    }
  }

  /**
    * Creates a [[JoinInfo]] by analyzing a condition.
    *
    * <p>NOTES: the functionality of the method is same with [[JoinInfo#of]],
    * the only difference is that the methods could return `filterNulls`.
    */
  def createJoinInfo(
      left: RelNode,
      right: RelNode,
      condition: RexNode,
      filterNulls: util.List[java.lang.Boolean]): JoinInfo = {
    val leftKeys = new util.ArrayList[Integer]
    val rightKeys = new util.ArrayList[Integer]
    val remaining = RelOptUtil.splitJoinCondition(
      left, right, condition, leftKeys, rightKeys, filterNulls)

    if (remaining.isAlwaysTrue) {
      JoinInfo.of(ImmutableIntList.copyOf(leftKeys), ImmutableIntList.copyOf(rightKeys))
    } else {
      // TODO create NonEquiJoinInfo directly
      JoinInfo.of(left, right, condition)
    }
  }

  def generateConditionFunction(
      config: TableConfig,
      rexBuilder: RexBuilder,
      joinInfo: JoinInfo,
      leftType: LogicalType,
      rightType: LogicalType): GeneratedJoinCondition = {
    val ctx = CodeGeneratorContext(config)
    // should consider null fields
    val exprGenerator = new ExprCodeGenerator(ctx, false)
      .bindInput(leftType)
      .bindSecondInput(rightType)

    val body = if (joinInfo.isEqui) {
      // only equality condition
      "return true;"
    } else {
      val nonEquiPredicates = joinInfo.getRemaining(rexBuilder)
      val condition = exprGenerator.generateExpression(nonEquiPredicates)
      s"""
         |${condition.code}
         |return ${condition.resultTerm};
         |""".stripMargin
    }

    FunctionCodeGenerator.generateJoinCondition(
      ctx,
      "ConditionFunction",
      body)
  }
}
