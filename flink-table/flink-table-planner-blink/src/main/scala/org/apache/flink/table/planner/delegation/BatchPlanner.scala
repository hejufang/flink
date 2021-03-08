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

package org.apache.flink.table.planner.delegation

import org.apache.flink.api.dag.Transformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.internal.SelectTableSink
import org.apache.flink.table.api.{ExplainDetail, TableConfig, TableEnvironment, TableException, TableSchema}
import org.apache.flink.table.catalog.{Catalog, CatalogManager, FunctionCatalog, ObjectIdentifier}
import org.apache.flink.table.delegation.Executor
import org.apache.flink.table.operations.{CatalogSinkModifyOperation, ModifyOperation, Operation, QueryOperation}
import org.apache.flink.table.operations.ddl.AnalyzeTableOperation
import org.apache.flink.table.planner.operations.PlannerQueryOperation
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistributionTraitDef
import org.apache.flink.table.planner.plan.nodes.exec.{BatchExecNode, ExecNode}
import org.apache.flink.table.planner.plan.nodes.process.DAGProcessContext
import org.apache.flink.table.planner.plan.optimize.{BatchCommonSubGraphBasedOptimizer, Optimizer}
import org.apache.flink.table.planner.plan.reuse.DeadlockBreakupProcessor
import org.apache.flink.table.planner.plan.stats.StatisticGenerator
import org.apache.flink.table.planner.plan.utils.{ExecNodePlanDumper, FlinkRelOptUtil}
import org.apache.flink.table.planner.sinks.BatchSelectTableSink
import org.apache.flink.table.planner.utils.{DummyStreamExecutionEnvironment, ExecutorUtils, JavaScalaConversionUtil, PlanUtil, TableStatsConverter}

import org.apache.calcite.plan.{ConventionTraitDef, RelTrait, RelTraitDef}
import org.apache.calcite.rel.logical.LogicalTableModify
import org.apache.calcite.rel.{RelCollationTraitDef, RelNode}
import org.apache.calcite.sql.SqlExplainLevel
import java.util

import _root_.scala.collection.JavaConversions._

class BatchPlanner(
    executor: Executor,
    config: TableConfig,
    functionCatalog: FunctionCatalog,
    catalogManager: CatalogManager)
  extends PlannerBase(executor, config, functionCatalog, catalogManager, isStreamingMode = false) {

  override protected def getTraitDefs: Array[RelTraitDef[_ <: RelTrait]] = {
    Array(
      ConventionTraitDef.INSTANCE,
      FlinkRelDistributionTraitDef.INSTANCE,
      RelCollationTraitDef.INSTANCE)
  }

  override protected def getOptimizer: Optimizer = new BatchCommonSubGraphBasedOptimizer(this)

  override private[flink] def translateToExecNodePlan(
      optimizedRelNodes: Seq[RelNode]): util.List[ExecNode[_, _]] = {
    val execNodePlan = super.translateToExecNodePlan(optimizedRelNodes)
    val context = new DAGProcessContext(this)
    // breakup deadlock
    new DeadlockBreakupProcessor().process(execNodePlan, context)
  }

  override def translateToPlan(
      execNodes: util.List[ExecNode[_, _]]): util.List[Transformation[_]] = {
    val planner = createDummyPlanner()
    planner.overrideEnvParallelism()

    execNodes.map {
      case node: BatchExecNode[_] => node.translateToPlan(planner)
      case _ =>
        throw new TableException("Cannot generate BoundedStream due to an invalid logical plan. " +
            "This is a bug and should not happen. Please file an issue.")
    }
  }

  override def createSelectTableSink(tableSchema: TableSchema): SelectTableSink = {
    new BatchSelectTableSink(tableSchema)
  }

  override def explain(operations: util.List[Operation], extraDetails: ExplainDetail*): String = {
    require(operations.nonEmpty, "operations should not be empty")
    val sinkRelNodes = operations.map {
      case queryOperation: QueryOperation =>
        val relNode = getRelBuilder.queryOperation(queryOperation).build()
        relNode match {
          // SQL: explain plan for insert into xx
          case modify: LogicalTableModify =>
            // convert LogicalTableModify to CatalogSinkModifyOperation
            val qualifiedName = modify.getTable.getQualifiedName
            require(qualifiedName.size() == 3, "the length of qualified name should be 3.")
            val modifyOperation = new CatalogSinkModifyOperation(
              ObjectIdentifier.of(qualifiedName.get(0), qualifiedName.get(1), qualifiedName.get(2)),
              new PlannerQueryOperation(modify.getInput)
            )
            translateToRel(modifyOperation)
          case _ =>
            relNode
        }
      case modifyOperation: ModifyOperation =>
        translateToRel(modifyOperation)
      case o => throw new TableException(s"Unsupported operation: ${o.getClass.getCanonicalName}")
    }
    val optimizedRelNodes = optimize(sinkRelNodes)
    val execNodes = translateToExecNodePlan(optimizedRelNodes)

    validateBeforeExecuteIfNeeded(execNodes)
    val transformations = translateToPlan(execNodes)

    val execEnv = getExecEnv
    ExecutorUtils.setBatchProperties(execEnv, getTableConfig)
    val streamGraph = ExecutorUtils.generateStreamGraph(execEnv, transformations)
    ExecutorUtils.setBatchProperties(streamGraph, getTableConfig)
    val executionPlan = PlanUtil.explainStreamGraph(streamGraph)

    val sb = new StringBuilder
    sb.append("== Abstract Syntax Tree ==")
    sb.append(System.lineSeparator)
    sinkRelNodes.foreach { sink =>
      sb.append(FlinkRelOptUtil.toString(sink))
      sb.append(System.lineSeparator)
    }

    sb.append("== Optimized Logical Plan ==")
    sb.append(System.lineSeparator)
    val explainLevel = if (extraDetails.contains(ExplainDetail.ESTIMATED_COST)) {
      SqlExplainLevel.ALL_ATTRIBUTES
    } else {
      SqlExplainLevel.EXPPLAN_ATTRIBUTES
    }
    sb.append(ExecNodePlanDumper.dagToString(execNodes, explainLevel))
    sb.append(System.lineSeparator)

    sb.append("== Physical Execution Plan ==")
    sb.append(System.lineSeparator)
    sb.append(executionPlan)
    sb.toString()
  }

  override def generateQueryFromAnalyzeTableOperation(
      tEnv: TableEnvironment,
      analyzeTableOperation: AnalyzeTableOperation): String = {
    val (statsSql, _) = StatisticGenerator.generateAnalyzeTableSql(
      tEnv,
      JavaScalaConversionUtil.toScala(analyzeTableOperation.getTableIdentifier.toList).toArray,
      Option(JavaScalaConversionUtil.toScala(analyzeTableOperation.getColumnList).toArray),
      Option(analyzeTableOperation.getPartitions.toMap))
    statsSql
  }

  override def executeAnalyzeTable(
      tEnv: TableEnvironment,
      catalog: Catalog,
      analyzeTableOperation: AnalyzeTableOperation): Unit = {
    val tableStats = StatisticGenerator.generateTableStats(
      tEnv,
      JavaScalaConversionUtil.toScala(analyzeTableOperation.getTableIdentifier.toList).toArray,
      Option(JavaScalaConversionUtil.toScala(analyzeTableOperation.getColumnList).toArray),
      Option(analyzeTableOperation.getPartitions.toMap))
    val catalogTableStatistics = TableStatsConverter.convertToCatalogTableStatistics(tableStats)
    val catalogColumnStatistics = TableStatsConverter.convertToCatalogColumnStatistics(tableStats)
    val tablePath = analyzeTableOperation.getTableIdentifier.toObjectPath
    catalog.alterTableStatistics(tablePath, catalogTableStatistics, false)
    catalog.alterTableColumnStatistics(tablePath, catalogColumnStatistics, false)
  }

  private def createDummyPlanner(): BatchPlanner = {
    val dummyExecEnv = new DummyStreamExecutionEnvironment(getExecEnv)
    val executor = new BatchExecutor(dummyExecEnv)
    config.getConfiguration.setBoolean(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CHAIN_ENABLE, true)
    new BatchPlanner(executor, config, functionCatalog, catalogManager)
  }

}
