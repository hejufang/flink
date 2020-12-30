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
import org.apache.flink.table.planner.plan.schema.{FlinkPreparingTableBase, LegacyTableSourceTable}
import org.apache.flink.table.sources.AggregatableTableSource
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.functions.FunctionDefinition
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.functions.aggfunctions.AvgAggFunction
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.AggregateUtil
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions

import org.apache.calcite.plan.RelOptRule._
import org.apache.calcite.plan.{RelOptRule, RelOptRuleCall}
import org.apache.calcite.sql.SqlKind

import java.util

/**
 * Planner rule that tries to push a local aggregator into a [[AggregatableTableSource]]
 * The following aggregation pattern will not be pushed down:
 *   1. If expression is involved in Aggregate or Group by,
 *      e.g. max (col1 + col2) or group by (col1 + col2), aggregate will not be pushed down.
 *   2. Push down all or nothing, that is, if there are more than one aggregate,
 *      this rule will only push down if all of the aggregates can be pushed down to data source.
 *   3. Complex agg functions which is not in (MAX/MIN/COUNT/SUM).
 *   4. Agg Function or operation specific does not support for pushing down
 *      in the underlying source connector.
 *   5. Agg Function with predicates which can not be pushed down to the underlying connector.
 *   6. WindowAggregate Function
 */
class PushLocalAggIntoLegacyTableSourceScanRule extends RelOptRule(
  operand(classOf[BatchExecExchange],
    operand(classOf[BatchExecGroupAggregateBase],
      operand(classOf[BatchExecLegacyTableSourceScan], none))),
  "PushLocalAggIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_AGGREGATE_PUSHDOWN_ENABLED)) {
      return false
    }

    call.rels(1) match {
      case _: BatchExecLocalHashAggregate =>
      case _: BatchExecLocalSortAggregate =>
      case _ => return false
    }

    val aggregate = call.rels(1).asInstanceOf[BatchExecGroupAggregateBase]
    if (aggregate.isFinal || aggregate.getAggCallList.size < 1) return false

    for (aggCall <- aggregate.getAggCallList) {
      if (aggCall.isDistinct || aggCall.isApproximate || aggCall.getArgList.size > 1) {
        return false
      }
      aggCall.getAggregation.getKind match {
        case SqlKind.MIN | SqlKind.MAX | SqlKind.SUM | SqlKind.SUM0 | SqlKind.AVG | SqlKind.COUNT =>
        case _ => return false
      }
    }

    val scan = call.rels(2).asInstanceOf[BatchExecLegacyTableSourceScan]
    scan.getTable.unwrap(classOf[LegacyTableSourceTable[_]]) match {
      case table: LegacyTableSourceTable[_] =>
        table.tableSource match {
          case source: AggregatableTableSource[_] => !source.isAggregatePushedDown
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val localAgg = call.rels(1).asInstanceOf[BatchExecGroupAggregateBase]
    val oldScan = call.rels(2).asInstanceOf[BatchExecLegacyTableSourceScan]
    pushLocalAggregateIntoScan(call, localAgg, oldScan)
  }

  private def pushLocalAggregateIntoScan(
      call: RelOptRuleCall,
      localAgg: BatchExecGroupAggregateBase,
      scan: BatchExecLegacyTableSourceScan): Unit = {

    val tableSource = scan.getTable.asInstanceOf[LegacyTableSourceTable[_]]
    val aggInfoList = AggregateUtil.transformToBatchAggregateInfoList(localAgg.getAggCallList,
      localAgg.getInput.getRowType)

    if (aggInfoList.aggInfos.isEmpty) {
      // no agg functions need to be pushed down
      return
    }

    val remainingAggFunctionList = new util.LinkedList[FunctionDefinition]()
    val aggArgFields = new util.LinkedList[Array[Int]]
    val groupSet = localAgg.getGrouping
    aggInfoList.getActualAggregateInfos.foreach(aggInfo => {
      if (aggInfo.function.isInstanceOf[AvgAggFunction]) {
        val (sumAggFunc, countAggFunc) = AggregateUtil.deriveSumAndCountFromAvg(aggInfo.function)
        remainingAggFunctionList.add(sumAggFunc)
        remainingAggFunctionList.add(countAggFunc)
        aggArgFields.add(aggInfo.argIndexes)
        aggArgFields.add(aggInfo.argIndexes)
      } else {
        remainingAggFunctionList.add(aggInfo.function)
        aggArgFields.add(aggInfo.argIndexes)
      }
    })
    val relDataType = localAgg.deriveRowType()
    val localAggOutputDataType = TypeConversions.fromLogicalToDataType(
      FlinkTypeFactory.toLogicalType(relDataType))

    val newRelOptTable = applyAggregate(
      aggArgFields, remainingAggFunctionList, groupSet, localAggOutputDataType, tableSource)
      .copy(relDataType)
    val newTableSource = newRelOptTable.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource
    val oldTableSource = tableSource.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource

    if (!newTableSource.asInstanceOf[AggregatableTableSource[_]].isAggregatePushedDown) {
      // local agg push down failed, do nothing
      return
    }

    if (newTableSource.asInstanceOf[AggregatableTableSource[_]].isAggregatePushedDown
      && newTableSource.explainSource().equals(oldTableSource.explainSource)) {
      throw new TableException("Failed to push aggregate into table source! "
        + "table source with pushdown capability must override and change "
        + "explainSource() API to explain the pushdown applied!")
    }
    val newScan = scan.copy(scan.getTraitSet, newRelOptTable)
    val exchange = call.rels(0).asInstanceOf[BatchExecExchange]
    exchange.replaceInputNode(0, newScan)
    call.transformTo(exchange)
  }

  private def applyAggregate(
      aggregateFields: util.List[Array[Int]],
      aggregateFunctions: util.List[FunctionDefinition],
      groupSet: Array[Int],
      aggOutputDataType: DataType,
      relOptTable: FlinkPreparingTableBase): LegacyTableSourceTable[_] = {
    val tableSourceTable = relOptTable.unwrap(classOf[LegacyTableSourceTable[Any]])
    val aggregatableSource = tableSourceTable.tableSource.asInstanceOf[AggregatableTableSource[Any]]
    val newTableSource = aggregatableSource.applyAggregates(
      aggregateFunctions, aggregateFields, groupSet, aggOutputDataType)
    val updatedAggregatesSize = aggregateFunctions.size()
    val statistic = tableSourceTable.getStatistic
    if (updatedAggregatesSize != 0) {
      // Keep all Statistics and original table source if not all aggregates can be pushed down
      return tableSourceTable
    }
    // Remove tableStats after all aggregates pushed down
    val newStatistic = FlinkStatistic.builder().statistic(statistic).tableStats(null).build()
    tableSourceTable.copy(newTableSource, newStatistic)
  }
}

object PushLocalAggIntoLegacyTableSourceScanRule {
  val INSTANCE = new PushLocalAggIntoLegacyTableSourceScanRule
}
