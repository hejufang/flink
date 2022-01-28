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
package org.apache.flink.table.planner.plan.nodes.physical.stream

import org.apache.flink.annotation.Experimental
import org.apache.flink.api.dag.Transformation
import org.apache.flink.configuration.ConfigOption
import org.apache.flink.configuration.ConfigOptions.key
import org.apache.flink.streaming.api.operators.KeyedProcessOperator
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator
import org.apache.flink.table.planner.codegen.sort.ComparatorCodeGenerator
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.calcite.Rank
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{KeySelectorUtil, _}
import org.apache.flink.table.runtime.operators.rank._
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel._
import org.apache.calcite.rel.`type`.RelDataTypeField
import org.apache.calcite.util.ImmutableBitSet

import java.lang.{Long => JLong}
import java.util
import scala.collection.JavaConversions._


/**
  * Stream physical RelNode for [[Rank]].
  */
class StreamExecRank(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    partitionKey: ImmutableBitSet,
    orderKey: RelCollation,
    rankType: RankType,
    rankRange: RankRange,
    rankNumberType: RelDataTypeField,
    outputRankNumber: Boolean,
    rankStrategy: RankProcessStrategy,
    rankFuncName: String = null)
  extends Rank(
    cluster,
    traitSet,
    inputRel,
    partitionKey,
    orderKey,
    rankType,
    rankRange,
    rankNumberType,
    outputRankNumber)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {
  override def requireWatermark: Boolean = false

  override def copy(traitSet: RelTraitSet, inputs: util.List[RelNode]): RelNode = {
    new StreamExecRank(
      cluster,
      traitSet,
      inputs.get(0),
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      rankStrategy)
  }

  def copy(newStrategy: RankProcessStrategy): StreamExecRank = {
    new StreamExecRank(
      cluster,
      traitSet,
      inputRel,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      newStrategy)
  }

  def copy(funcName: String): StreamExecRank = {
    new StreamExecRank(
      cluster,
      traitSet,
      inputRel,
      partitionKey,
      orderKey,
      rankType,
      rankRange,
      rankNumberType,
      outputRankNumber,
      rankStrategy,
      funcName)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val inputRowType = inputRel.getRowType
    val writer = pw.input("input", getInput)
      .item("strategy", rankStrategy)
      .item("rankType", rankType)
      .item("rankRange", rankRange.toString(inputRowType.getFieldNames))
      .item("partitionBy", RelExplainUtil.fieldToString(partitionKey.toArray, inputRowType))
      .item("orderBy", RelExplainUtil.collationToString(orderKey, inputRowType))
      .item("select", getRowType.getFieldNames.mkString(", "))
    if (rankFuncName != null) {
      writer.item("function", rankFuncName)
    } else {
      writer
    }
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    List(getInput.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {
    val tableConfig = planner.getTableConfig
    rankType match {
      case RankType.ROW_NUMBER => // ignore
      case RankType.RANK =>
        throw new TableException("RANK() on streaming table is not supported currently")
      case RankType.DENSE_RANK =>
        throw new TableException("DENSE_RANK() on streaming table is not supported currently")
      case k =>
        throw new TableException(s"Streaming tables do not support $k rank function.")
    }

    val inputRowTypeInfo = RowDataTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val fieldCollations = orderKey.getFieldCollations
    val (sortFields, sortDirections, nullsIsLast) = SortUtil.getKeysAndOrders(fieldCollations)
    val sortKeySelector = KeySelectorUtil.getRowDataSelector(
      sortFields, inputRowTypeInfo, tableConfig)
    val sortKeyType = sortKeySelector.getProducedType
    val sortKeyComparator = ComparatorCodeGenerator.gen(tableConfig, "StreamExecSortComparator",
      sortFields.indices.toArray, sortKeyType.getLogicalTypes, sortDirections, nullsIsLast)
    val generateUpdateBefore = ChangelogPlanUtils.generateUpdateBefore(this)
    val cacheSize = tableConfig.getConfiguration.getLong(StreamExecRank.TABLE_EXEC_TOPN_CACHE_SIZE)
    val enableTop1 =
      tableConfig.getConfiguration.getBoolean(ExecutionConfigOptions.TABLE_EXEC_TOP1_ENABLE)
    val minIdleStateRetentionTime = tableConfig.getMinIdleStateRetentionTime
    val maxIdleStateRetentionTime = tableConfig.getMaxIdleStateRetentionTime

    val processFunction = rankStrategy match {
      case AppendFastStrategy =>
        if (enableTop1 && RankUtil.isTop1(rankRange)) {
          new FastTop1Function(
            minIdleStateRetentionTime,
            maxIdleStateRetentionTime,
            inputRowTypeInfo,
            sortKeyComparator,
            sortKeySelector,
            rankType,
            rankRange,
            generateUpdateBefore,
            outputRankNumber,
            cacheSize)
        } else {
          new AppendOnlyTopNFunction(
            minIdleStateRetentionTime,
            maxIdleStateRetentionTime,
            inputRowTypeInfo,
            sortKeyComparator,
            sortKeySelector,
            rankType,
            rankRange,
            generateUpdateBefore,
            outputRankNumber,
            cacheSize)
        }

      case UpdateFastStrategy(primaryKeys) =>

        if (enableTop1 && RankUtil.isTop1(rankRange)) {
          new FastTop1Function(
            minIdleStateRetentionTime,
            maxIdleStateRetentionTime,
            inputRowTypeInfo,
            sortKeyComparator,
            sortKeySelector,
            rankType,
            rankRange,
            generateUpdateBefore,
            outputRankNumber,
            cacheSize)
        } else {
          val rowKeySelector = KeySelectorUtil.getRowDataSelector(
            primaryKeys, inputRowTypeInfo, tableConfig)
          new UpdatableTopNFunction(
            minIdleStateRetentionTime,
            maxIdleStateRetentionTime,
            inputRowTypeInfo,
            rowKeySelector,
            sortKeyComparator,
            sortKeySelector,
            rankType,
            rankRange,
            generateUpdateBefore,
            outputRankNumber,
            cacheSize)
        }

      // TODO Use UnaryUpdateTopNFunction after SortedMapState is merged
      case RetractStrategy =>
        val equaliserCodeGen = new EqualiserCodeGenerator(inputRowTypeInfo.getLogicalTypes)
        val generatedEqualiser = equaliserCodeGen.generateRecordEqualiser("RankValueEqualiser")

        new RetractableTopNFunction(
          minIdleStateRetentionTime,
          maxIdleStateRetentionTime,
          inputRowTypeInfo,
          sortKeyComparator,
          sortKeySelector,
          rankType,
          rankRange,
          generatedEqualiser,
          generateUpdateBefore,
          outputRankNumber)
    }
    val operator = new KeyedProcessOperator(processFunction)
    processFunction.setKeyContext(operator)
    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    val outputRowTypeInfo = RowDataTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getRowType))
    val ret = new OneInputTransformation(
      inputTransform,
      this.copy(processFunction.getClass.getSimpleName).getRelDetailedDescription,
      operator,
      outputRowTypeInfo,
      inputTransform.getParallelism)
    PhysicalPlanUtil.setDebugLoggingConverter(tableConfig, getRowType, ret)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    val selector = KeySelectorUtil.getRowDataSelector(
      partitionKey.toArray, inputRowTypeInfo, tableConfig)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)

    ret.setHasState(true)
    ret
  }
}
object StreamExecRank {

  // It is a experimental config, will may be removed later.
  @Experimental
  val TABLE_EXEC_TOPN_CACHE_SIZE: ConfigOption[JLong] =
  key("table.exec.topn.cache-size")
      .defaultValue(JLong.valueOf(10000L))
      .withDescription("TopN operator has a cache which caches partial state contents to reduce" +
          " state access. Cache size is the number of records in each TopN task.")
}
