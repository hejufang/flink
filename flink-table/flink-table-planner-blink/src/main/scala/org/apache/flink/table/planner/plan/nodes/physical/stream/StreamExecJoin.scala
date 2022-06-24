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

import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.transformations.{PartitionTransformation, TwoInputTransformation}
import org.apache.flink.streaming.runtime.partitioner.BroadcastPartitioner
import org.apache.flink.table.api.{TableConfig, ValidationException}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.{FlinkContext, FlinkTypeFactory}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.nodes.common.CommonPhysicalJoin
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.utils.{JoinUtil, KeySelectorUtil, PhysicalPlanUtil}
import org.apache.flink.table.runtime.operators.join.stream.state.JoinInputSideSpec
import org.apache.flink.table.runtime.operators.join.stream.{BroadcastJoinOperator, MiniBatchStreamingJoinOperator, StreamingJoinOperator, StreamingSemiAntiJoinOperator}
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.calcite.plan._
import org.apache.calcite.rel.core.{Join, JoinRelType}
import org.apache.calcite.rel.metadata.RelMetadataQuery
import org.apache.calcite.rel.{RelNode, RelWriter}
import org.apache.calcite.rex.RexNode
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE
import org.apache.flink.table.factories.FactoryUtil

import scala.collection.JavaConversions._
import java.util

/**
  * Stream physical RelNode for regular [[Join]].
  *
  * Regular joins are the most generic type of join in which any new records or changes to
  * either side of the join input are visible and are affecting the whole join result.
  */
class StreamExecJoin(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    leftRel: RelNode,
    rightRel: RelNode,
    condition: RexNode,
    joinType: JoinRelType,
    val joinConfOption: Option[JoinUtil.JoinConfig])
  extends CommonPhysicalJoin(cluster, traitSet, leftRel, rightRel, condition, joinType)
  with StreamPhysicalRel
  with StreamExecNode[RowData] {

  def inputUniqueKeyContainsJoinKey(inputOrdinal: Int): Boolean = {
    val input = getInput(inputOrdinal)
    val inputUniqueKeys = getCluster.getMetadataQuery.getUniqueKeys(input)
    if (inputUniqueKeys != null) {
      val joinKeys = if (inputOrdinal == 0) {
        // left input
        keyPairs.map(_.source).toArray
      } else {
        // right input
        keyPairs.map(_.target).toArray
      }
      inputUniqueKeys.exists {
        uniqueKey => joinKeys.forall(uniqueKey.toArray.contains(_))
      }
    } else {
      false
    }
  }

  override def requireWatermark: Boolean = false

  override def copy(
      traitSet: RelTraitSet,
      conditionExpr: RexNode,
      left: RelNode,
      right: RelNode,
      joinType: JoinRelType,
      semiJoinDone: Boolean): Join = {
    new StreamExecJoin(cluster, traitSet, left, right, conditionExpr, joinType, joinConfOption)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val tableConfig = getCluster.getPlanner.getContext.
      asInstanceOf[FlinkContext].getTableConfig
    val writer = super
      .explainTerms(pw)
      .item("leftInputSpec", analyzeJoinInput(left, tableConfig))
      .item("rightInputSpec", analyzeJoinInput(right, tableConfig))
    if (joinConfOption.isDefined) {
      writer.item("broadcast", joinConfOption.get.toString)
    } else {
      writer
    }
  }

  override def computeSelfCost(planner: RelOptPlanner, metadata: RelMetadataQuery): RelOptCost = {
    val elementRate = 100.0d * 2 // two input stream
    planner.getCostFactory.makeCost(elementRate, elementRate, 0)
  }

  //~ ExecNode methods -----------------------------------------------------------

  override def getInputNodes: util.List[ExecNode[StreamPlanner, _]] = {
    getInputs.map(_.asInstanceOf[ExecNode[StreamPlanner, _]])
  }

  override def replaceInputNode(
      ordinalInParent: Int,
      newInputNode: ExecNode[StreamPlanner, _]): Unit = {
    replaceInput(ordinalInParent, newInputNode.asInstanceOf[RelNode])
  }

  override protected def translateToPlanInternal(
      planner: StreamPlanner): Transformation[RowData] = {

    val tableConfig = planner.getTableConfig
    val returnType = RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(getRowType))

    val leftTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]
    var rightTransform = getInputNodes.get(1).translateToPlan(planner)
      .asInstanceOf[Transformation[RowData]]

    val leftType = leftTransform.getOutputType.asInstanceOf[RowDataTypeInfo]
    val rightType = rightTransform.getOutputType.asInstanceOf[RowDataTypeInfo]

    val (leftJoinKey, rightJoinKey) =
      JoinUtil.checkAndGetJoinKeys(keyPairs, getLeft, getRight, allowEmptyKey = true)

    val leftSelect = KeySelectorUtil.getRowDataSelector(leftJoinKey, leftType, tableConfig)
    val rightSelect = KeySelectorUtil.getRowDataSelector(rightJoinKey, rightType, tableConfig)

    val leftInputSpec = analyzeJoinInput(left, tableConfig)
    val rightInputSpec = analyzeJoinInput(right, tableConfig)

    val generatedCondition = JoinUtil.generateConditionFunction(
      tableConfig,
      cluster.getRexBuilder,
      getJoinInfo,
      leftType.toRowType,
      rightType.toRowType)

    val minRetentionTime = tableConfig.getMinIdleStateRetentionTime

    val isMiniBatchJoinEnabled = tableConfig.getConfiguration.getBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED) &&
      tableConfig.getConfiguration.getBoolean(
        ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLE_REGULAR_JOIN)

    val miniBatchSize = tableConfig.getConfiguration.getLong(TABLE_EXEC_MINIBATCH_SIZE)
    if (isMiniBatchJoinEnabled && miniBatchSize <= 0) {
      throw new IllegalArgumentException(
        TABLE_EXEC_MINIBATCH_SIZE.key() + " must be greater than zero when mini batch is enabled")
    }

    var operatorPrefixName = ""
    val operator = if (joinConfOption.isDefined) {
      if (joinType != JoinRelType.LEFT && joinType != JoinRelType.INNER) {
        throw new ValidationException("Broadcast join only support left and inner join")
      }
      if (!rightTransform.withSameWatermarkPerBatch()) {
        throw new ValidationException(String.format("Currently, broadcast join's right side only" +
          " accept connectors which support `%s`", FactoryUtil.SOURCE_SCAN_INTERVAL.key()));
      }
      rightTransform = new PartitionTransformation[RowData](
        rightTransform, new BroadcastPartitioner[RowData]())
      rightTransform.setName("BroadcastPartition")
      operatorPrefixName = "Broadcast"
      new BroadcastJoinOperator(
        leftType,
        rightType,
        generatedCondition,
        leftInputSpec,
        rightInputSpec,
        joinType == JoinRelType.LEFT,
        filterNulls,
        leftSelect,
        rightSelect,
        minRetentionTime,
        joinConfOption.get.allowLatencyMs,
        joinConfOption.get.maxBuildLatencyMs,
        joinConfOption.get.table)
    } else if (joinType == JoinRelType.ANTI || joinType == JoinRelType.SEMI) {
      new StreamingSemiAntiJoinOperator(
        joinType == JoinRelType.ANTI,
        leftType,
        rightType,
        generatedCondition,
        leftInputSpec,
        rightInputSpec,
        filterNulls,
        minRetentionTime)
    } else {
      val leftIsOuter = joinType == JoinRelType.LEFT || joinType == JoinRelType.FULL
      val rightIsOuter = joinType == JoinRelType.RIGHT || joinType == JoinRelType.FULL
      if (isMiniBatchJoinEnabled){
        new MiniBatchStreamingJoinOperator(
          leftType,
          rightType,
          generatedCondition,
          leftInputSpec,
          rightInputSpec,
          leftIsOuter,
          rightIsOuter,
          filterNulls,
          minRetentionTime,
          miniBatchSize
        )
      } else {
        new StreamingJoinOperator(
          leftType,
          rightType,
          generatedCondition,
          leftInputSpec,
          rightInputSpec,
          leftIsOuter,
          rightIsOuter,
          filterNulls,
          minRetentionTime)
      }
    }

    val ret = new TwoInputTransformation[RowData, RowData, RowData](
      leftTransform,
      rightTransform,
      operatorPrefixName + getRelDetailedDescription,
      operator,
      returnType,
      leftTransform.getParallelism)
    PhysicalPlanUtil.setDebugLoggingConverter(tableConfig, getRowType, ret)

    if (inputsContainSingleton()) {
      ret.setParallelism(1)
      ret.setMaxParallelism(1)
    }

    // set KeyType and Selector for state
    ret.setStateKeySelectors(leftSelect, rightSelect)
    ret.setStateKeyType(leftSelect.getProducedType)

    ret.setHasState(true)
    ret
  }

  def getNonMiniBatchInput: Option[RelNode] = joinConfOption match {
    case Some(_) => Some(right)
    case _ => None
  }

  private def analyzeJoinInput(input: RelNode, tableConfig: TableConfig): JoinInputSideSpec = {
    val uniqueKeys = cluster.getMetadataQuery.getUniqueKeys(input)
    if (uniqueKeys == null || uniqueKeys.isEmpty) {
      JoinInputSideSpec.withoutUniqueKey()
    } else {
      val inRowType = RowDataTypeInfo.of(FlinkTypeFactory.toLogicalRowType(input.getRowType))
      val joinKeys = if (input == left) {
        keyPairs.map(_.source).toArray
      } else {
        keyPairs.map(_.target).toArray
      }
      val uniqueKeysContainedByJoinKey = uniqueKeys
        .filter(uk => uk.toArray.forall(joinKeys.contains(_)))
        .map(_.toArray)
        .toArray
      if (uniqueKeysContainedByJoinKey.nonEmpty) {
        // join key contains unique key
        val smallestUniqueKey = getSmallestKey(uniqueKeysContainedByJoinKey)
        val uniqueKeySelector = KeySelectorUtil.getRowDataSelector(
          smallestUniqueKey, inRowType, tableConfig)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKeyContainedByJoinKey(uniqueKeyTypeInfo, uniqueKeySelector)
      } else {
        val smallestUniqueKey = getSmallestKey(uniqueKeys.map(_.toArray).toArray)
        val uniqueKeySelector = KeySelectorUtil.getRowDataSelector(
          smallestUniqueKey, inRowType, tableConfig)
        val uniqueKeyTypeInfo = uniqueKeySelector.getProducedType
        JoinInputSideSpec.withUniqueKey(uniqueKeyTypeInfo, uniqueKeySelector)
      }
    }
  }

  private def getSmallestKey(keys: Array[Array[Int]]): Array[Int] = {
    var smallest = keys.head
    for (key <- keys) {
      if (key.length < smallest.length) {
        smallest = key
      }
    }
    smallest
  }
}
