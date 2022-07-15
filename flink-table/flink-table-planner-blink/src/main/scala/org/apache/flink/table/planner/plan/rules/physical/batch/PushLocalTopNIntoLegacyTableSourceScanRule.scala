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

import java.util

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.plan.RelOptRule.none
import org.apache.calcite.plan.RelOptRule.operand
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.core.Sort
import org.apache.calcite.rex.RexLiteral
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.OptimizerConfigOptions
import org.apache.flink.table.plan.stats.ColumnStats
import org.apache.flink.table.plan.stats.TableStats
import org.apache.flink.table.planner.calcite.FlinkContext
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacyTableSourceScan
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecSortLimit
import org.apache.flink.table.planner.plan.schema.FlinkPreparingTableBase
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.planner.plan.stats.FlinkStatistic
import org.apache.flink.table.planner.plan.utils.SortUtil
import org.apache.flink.table.sources.TopNInfo
import org.apache.flink.table.sources.TopNableTableSource

import scala.collection.JavaConversions._
import scala.collection.JavaConverters._

/**
  * Planner rule that tries to push a topN into a [[TopNableTableSource]].
  */
class PushLocalTopNIntoLegacyTableSourceScanRule extends RelOptRule(
  operand(classOf[BatchExecSortLimit],
    operand(classOf[BatchExecLegacyTableSourceScan], none)),
  "PushLocalTopNIntoLegacyTableSourceScanRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    val config = call.getPlanner.getContext.unwrap(classOf[FlinkContext]).getTableConfig
    if (!config.getConfiguration.getBoolean(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_ENABLED)) {
      return false
    }

    val sortLimit: BatchExecSortLimit = call.rel(0)
    // In theory, Sort.fetch could be RexDynamicParam, but Flink do not support dynamic param
    // for now, so we simply assume Sort.fetch is a literal here.
    val fetch = RexLiteral.intValue(sortLimit.asInstanceOf[Sort].fetch)
    if (fetch > config.getConfiguration.getInteger(
      OptimizerConfigOptions.TABLE_OPTIMIZER_SOURCE_TOPN_PUSHDOWN_THRESHOLD)) {
      return false
    }

    val scan: BatchExecLegacyTableSourceScan = call.rel(1)
    scan.getTable.unwrap(classOf[LegacyTableSourceTable[_]]) match {
      case table: LegacyTableSourceTable[_] =>
        table.tableSource match {
          case source: TopNableTableSource[_] =>
            !source.isTopNPushedDown
          case _ => false
        }
      case _ => false
    }
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    val sortLimit: BatchExecSortLimit = call.rel(0)
    val scan: BatchExecLegacyTableSourceScan = call.rel(1)
    val table: LegacyTableSourceTable[_] = scan.getTable.asInstanceOf[LegacyTableSourceTable[_]]
    pushTopNIntoScan(call, sortLimit, scan, table)
  }

  private def pushTopNIntoScan(
      call: RelOptRuleCall,
      sortLimit: BatchExecSortLimit,
      scan: BatchExecLegacyTableSourceScan,
      relOptTable: FlinkPreparingTableBase): Unit = {
    val fetch = RexLiteral.intValue(sortLimit.asInstanceOf[Sort].fetch)

    val (keys, orders, nullsIsLast) = SortUtil.getKeysAndOrders(
      sortLimit.asInstanceOf[Sort].collation.getFieldCollations)
    val cols = new util.ArrayList[java.lang.String]()
    keys.foreach(i => cols.add(sortLimit.getInputs.get(0).getRowType.getFieldNames.get(i)))


    val sortDirections = orders.map(b => new java.lang.Boolean(b))
    val nullsIsLastArray = nullsIsLast.map(b => new java.lang.Boolean(b))

    val topNInfo =
      new TopNInfo(cols, sortDirections.toList.asJava, nullsIsLastArray.toList.asJava, fetch)

    val newRelOptTable = applyTopN(relOptTable, topNInfo)

    val newTableSource = newRelOptTable.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource
    val oldTableSource = relOptTable.unwrap(classOf[LegacyTableSourceTable[_]]).tableSource

    if (newTableSource.asInstanceOf[TopNableTableSource[_]].isTopNPushedDown
      && newTableSource.explainSource().equals(oldTableSource.explainSource)) {
      throw new TableException("Failed to push TopN into table source! "
        + "table source with pushdown capability must override and change "
        + "explainSource() API to explain the pushdown applied!")
    }

    val newScan = scan.copy(scan.getTraitSet, newRelOptTable)
    call.transformTo(newScan)
  }

  private def applyTopN (
      relOptTable: FlinkPreparingTableBase,
      topNInfo: TopNInfo): LegacyTableSourceTable[_] = {

    val tableSourceTable = relOptTable.unwrap(classOf[LegacyTableSourceTable[_]])
    val topNSource = tableSourceTable.tableSource.asInstanceOf[TopNableTableSource[_]]
    val newTableSource = topNSource.applyTopN(topNInfo)

    val limit = topNInfo.getLimit.toLong
    val statistic = relOptTable.getStatistic
    val newTableStats = if (statistic == FlinkStatistic.UNKNOWN) {
      new TableStats(limit)
    } else {
      val newRowCount = if (statistic.getRowCount != null) {
        Math.min(limit, statistic.getRowCount.toLong)
      } else {
        limit
      }
      val newColStatsMap = new java.util.HashMap[String, ColumnStats]
      val oldColStatsMap: java.util.Map[String, ColumnStats]
        = statistic.getTableStats.getColumnStats
      for (aColumnName <- oldColStatsMap.keySet()) {
        val aColumnStats = oldColStatsMap.get(aColumnName)
        val newNdv: java.lang.Long =
        if (limit < aColumnStats.getNdv) limit else aColumnStats.getNdv

        val newNullCount: java.lang.Long =
          if (limit < aColumnStats.getNullCount) limit else aColumnStats.getNullCount

        val newColumnStats = new ColumnStats(
          newNdv,
          newNullCount,
          aColumnStats.getAvgLen,
          aColumnStats.getMaxLen,
          aColumnStats.getMaxValue,
          aColumnStats.getMinValue
        )

        newColStatsMap.put(aColumnName, newColumnStats)
      }
      new TableStats(newRowCount, newColStatsMap)
    }
    // Update TableStats after topN push down
    val newStatistic = FlinkStatistic.builder()
      .statistic(statistic)
      .tableStats(newTableStats)
      .build()
    tableSourceTable.copy(newTableSource, newStatistic)
  }
}

object PushLocalTopNIntoLegacyTableSourceScanRule {
  val INSTANCE: RelOptRule = new PushLocalTopNIntoLegacyTableSourceScanRule
}
