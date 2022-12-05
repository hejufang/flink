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

package org.apache.flink.table.planner.plan.optimize

import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.catalog.{CatalogManager, FunctionCatalog}
import org.apache.flink.table.planner.calcite.{FlinkContext, SqlExprToRexConverterFactory}
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacySink
import org.apache.flink.table.planner.plan.optimize.program.{BatchOptimizeContext, FlinkBatchProgram}
import org.apache.flink.table.planner.plan.schema.IntermediateRelTable
import org.apache.flink.table.planner.plan.utils.RuleStatisticsListener
import org.apache.flink.table.planner.utils.{JavaScalaConversionUtil, Logging, TableConfigUtils}
import org.apache.flink.util.Preconditions

import org.apache.calcite.rel.RelNode

import java.util.Collections

/**
  * A [[CommonSubGraphBasedOptimizer]] for Batch.
  */
class BatchCommonSubGraphBasedOptimizer(planner: BatchPlanner)
  extends CommonSubGraphBasedOptimizer with Logging {

  override protected def doOptimize(roots: Seq[RelNode]): Seq[RelNodeBlock] = {
    // TODO currently join hint only works in BATCH
    // resolve hints before optimizing
    val joinHintResolver = new JoinHintResolver()
    val resolvedHintRoots = joinHintResolver.resolve(JavaScalaConversionUtil.toJava(roots))

    // clear query block alias before optimizing
//    val clearQueryBlockAliasResolver = new ClearQueryBlockAliasResolver
//    val resolvedAliasRoots = clearQueryBlockAliasResolver.resolve(resolvedHintRoots)
    // build RelNodeBlock plan
    val rootBlocks = RelNodeBlockPlanBuilder.buildRelNodeBlockPlan(
      JavaScalaConversionUtil.toScala(resolvedHintRoots), planner.getTableConfig)
    // optimize recursively RelNodeBlock
    rootBlocks.foreach(optimizeBlock)
    if (LOG.isDebugEnabled) {
      LOG.debug(planner.plannerContext.getRuleStatisticsListener.dumpStatistic())
    }
    rootBlocks
  }

  private def optimizeBlock(block: RelNodeBlock): Unit = {
    block.children.foreach { child =>
      if (child.getNewOutputNode.isEmpty) {
        optimizeBlock(child)
      }
    }

    val originTree = block.getPlan
    val optimizedTree = optimizeTree(originTree)

    optimizedTree match {
      case _: BatchExecLegacySink[_] => // ignore
      case _ =>
        val name = createUniqueIntermediateRelTableName
        val intermediateRelTable =  new IntermediateRelTable(Collections.singletonList(name),
          optimizedTree)
        val newTableScan = wrapIntermediateRelTableToTableScan(intermediateRelTable, name)
        block.setNewOutputNode(newTableScan)
        block.setOutputTableName(name)
    }
    block.setOptimizedPlan(optimizedTree)
  }

  /**
    * Generates the optimized [[RelNode]] tree from the original relational node tree.
    *
    * @param relNode The original [[RelNode]] tree
    * @return The optimized [[RelNode]] tree
    */
  private def optimizeTree(relNode: RelNode): RelNode = {
    val config = planner.getTableConfig
    val programs = TableConfigUtils.getCalciteConfig(config).getBatchProgram
      .getOrElse(FlinkBatchProgram.buildProgram(config.getConfiguration))
    Preconditions.checkNotNull(programs)

    val context = relNode.getCluster.getPlanner.getContext.unwrap(classOf[FlinkContext])

    programs.optimize(relNode, new BatchOptimizeContext {
      override def getTableConfig: TableConfig = config

      override def getFunctionCatalog: FunctionCatalog = planner.functionCatalog

      override def getCatalogManager: CatalogManager = planner.catalogManager

      override def getSqlExprToRexConverterFactory: SqlExprToRexConverterFactory =
        context.getSqlExprToRexConverterFactory

      override def getRuleStatisticsListener: RuleStatisticsListener =
        planner.plannerContext.getRuleStatisticsListener
    })
  }

}
