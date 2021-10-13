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
import org.apache.flink.streaming.api.transformations.OneInputTransformation
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.dataformat.BaseRow
import org.apache.flink.table.planner.calcite.FlinkRelBuilder.PlannerNamedWindowProperty
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.agg.AggsHandlerCodeGenerator
import org.apache.flink.table.planner.codegen.{CodeGeneratorContext, EqualiserCodeGenerator}
import org.apache.flink.table.planner.delegation.StreamPlanner
import org.apache.flink.table.planner.plan.logical._
import org.apache.flink.table.planner.plan.nodes.exec.{ExecNode, StreamExecNode}
import org.apache.flink.table.planner.plan.rules.physical.stream.{StreamExecGroupWindowAggregateRule, StreamExecRetractionRules}
import org.apache.flink.table.planner.plan.utils.AggregateUtil._
import org.apache.flink.table.planner.plan.utils.{AggregateInfoList, AggregateUtil, KeySelectorUtil, RelExplainUtil, WindowEmitStrategy}
import org.apache.flink.table.runtime.generated.{GeneratedNamespaceAggsHandleFunction, GeneratedRecordEqualiser}
import org.apache.flink.table.runtime.operators.window.assigners.{UserDefinedMergingWindowAssigner, UserDefinedWindowAssigner, WindowAssigner}
import org.apache.flink.table.runtime.operators.window.{CountWindow, TimeWindow, WindowOperator, WindowOperatorBuilder}
import org.apache.flink.table.runtime.types.LogicalTypeDataTypeConverter.fromDataTypeToLogicalType
import org.apache.flink.table.runtime.typeutils.BaseRowTypeInfo
import org.apache.flink.table.types.logical.LogicalType

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.AggregateCall
import org.apache.calcite.rel.{RelNode, RelWriter, SingleRel}
import org.apache.calcite.tools.RelBuilder

import java.time.Duration
import java.util

import scala.collection.JavaConversions._

/**
  * Streaming group window aggregate physical node which will be translate to window operator.
  */
class StreamExecGroupWindowAggregate(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    outputRowType: RelDataType,
    inputRowType: RelDataType,
    grouping: Array[Int],
    val aggCalls: Seq[AggregateCall],
    val window: LogicalWindow,
    namedProperties: Seq[PlannerNamedWindowProperty],
    inputTimeFieldIndex: Int,
    val emitStrategy: WindowEmitStrategy)
  extends SingleRel(cluster, traitSet, inputRel)
  with StreamPhysicalRel
  with StreamExecNode[BaseRow] {

  override def producesUpdates: Boolean = emitStrategy.produceUpdates

  override def consumesRetractions = true

  override def needsUpdatesAsRetraction(input: RelNode) = true

  override def producesRetractions: Boolean = false

  override def requireWatermark: Boolean = window match {
    case TumblingGroupWindow(_, timeField, size, _)
      if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) => true
    case SlidingGroupWindow(_, timeField, size, _, _)
      if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) => true
    case SessionGroupWindow(_, timeField, _)
      if isRowtimeAttribute(timeField) => true
    case _ => false
  }

  def getGrouping: Array[Int] = grouping

  def getWindowProperties: Seq[PlannerNamedWindowProperty] = namedProperties

  override def deriveRowType(): RelDataType = outputRowType

  override def copy(traitSet: RelTraitSet, inputs: java.util.List[RelNode]): RelNode = {
    new StreamExecGroupWindowAggregate(
      cluster,
      traitSet,
      inputs.get(0),
      outputRowType,
      inputRowType,
      grouping,
      aggCalls,
      window,
      namedProperties,
      inputTimeFieldIndex,
      emitStrategy)
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .itemIf("groupBy", RelExplainUtil.fieldToString(grouping, inputRowType), grouping.nonEmpty)
      .item("window", window)
      .itemIf("properties", namedProperties.map(_.name).mkString(", "), namedProperties.nonEmpty)
      .item("select", RelExplainUtil.streamWindowAggregationToString(
        inputRowType,
        grouping,
        outputRowType,
        aggCalls,
        namedProperties))
      .itemIf("emit", emitStrategy, !emitStrategy.toString.isEmpty)
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
      planner: StreamPlanner): Transformation[BaseRow] = {
    val config = planner.getTableConfig

    val inputTransform = getInputNodes.get(0).translateToPlan(planner)
      .asInstanceOf[Transformation[BaseRow]]

    val inputRowTypeInfo = inputTransform.getOutputType.asInstanceOf[BaseRowTypeInfo]
    val outRowType = BaseRowTypeInfo.of(FlinkTypeFactory.toLogicalRowType(outputRowType))

    val inputIsAccRetract = StreamExecRetractionRules.isAccRetract(input)

    val enableRetractInput = config.getConfiguration.getBoolean(
      StreamExecGroupWindowAggregateRule.TABLE_EXEC_WINDOW_ALLOW_RETRACT_INPUT)

    if (!enableRetractInput && inputIsAccRetract) {
      throw new TableException(
        "Group Window Agg: Retraction on windowed GroupBy aggregation is not supported yet. \n" +
          "please re-check sql grammar. \n" +
          "Note: Windowed GroupBy aggregation should not follow a" +
          "non-windowed GroupBy aggregation.")
    }

    val isCountWindow = window match {
      case TumblingGroupWindow(_, _, size, _) if hasRowIntervalType(size) => true
      case SlidingGroupWindow(_, _, size, _, _) if hasRowIntervalType(size) => true
      case _ => false
    }

    if (isCountWindow && grouping.length > 0 && config.getMinIdleStateRetentionTime < 0) {
      LOG.warn(
        "No state retention interval configured for a query which accumulates state. " +
          "Please provide a query configuration with valid retention interval to prevent " +
          "excessive state size. You may specify a retention time of 0 to not clean up the state.")
    }

    val aggString = RelExplainUtil.streamWindowAggregationToString(
      inputRowType,
      grouping,
      outputRowType,
      aggCalls,
      namedProperties)

    val timeIdx = if (isRowtimeAttribute(window.timeAttribute)) {
      if (inputTimeFieldIndex < 0) {
        throw new TableException(
          "Group window aggregate must defined on a time attribute, " +
            "but the time attribute can't be found.\n" +
          "This should never happen. Please file an issue."
        )
      }
      inputTimeFieldIndex
    } else {
      -1
    }

    val needRetraction = StreamExecRetractionRules.isAccRetract(getInput)
    val aggInfoList = transformToStreamAggregateInfoList(
      aggCalls,
      inputRowType,
      Array.fill(aggCalls.size)(needRetraction),
      needInputCount = needRetraction,
      isStateBackendDataViews = true)

    val aggsHandler = createAggsHandler(
      aggInfoList,
      config,
      planner.getRelBuilder,
      inputRowTypeInfo.getLogicalTypes,
      needRetraction)

    val aggResultTypes = aggInfoList.getActualValueTypes.map(fromDataTypeToLogicalType)
    val windowPropertyTypes = namedProperties.map(_.property.resultType).toArray
    val generator = new EqualiserCodeGenerator(aggResultTypes ++ windowPropertyTypes)
    val equaliser = generator.generateRecordEqualiser("WindowValueEqualiser")

    val aggValueTypes = aggInfoList.getActualValueTypes.map(fromDataTypeToLogicalType)
    val accTypes = aggInfoList.getAccTypes.map(fromDataTypeToLogicalType)
    val operator = createWindowOperator(
      config,
      aggsHandler,
      equaliser,
      accTypes,
      windowPropertyTypes,
      aggValueTypes,
      inputRowTypeInfo.getLogicalTypes,
      timeIdx)

    val transformation = new OneInputTransformation(
      inputTransform,
      getRelDetailedDescription,
      operator,
      outRowType,
      inputTransform.getParallelism)

    if (inputsContainSingleton()) {
      transformation.setParallelism(1)
      transformation.setMaxParallelism(1)
    }

    val selector = KeySelectorUtil.getBaseRowSelector(grouping, inputRowTypeInfo, config)

    // set KeyType and Selector for state
    transformation.setStateKeySelector(selector)
    transformation.setStateKeyType(selector.getProducedType)
    transformation
  }

  private def createAggsHandler(
      aggInfoList: AggregateInfoList,
      config: TableConfig,
      relBuilder: RelBuilder,
      fieldTypeInfos: Seq[LogicalType],
      needRetraction: Boolean): GeneratedNamespaceAggsHandleFunction[_] = {

    val needMerge = window match {
      case SlidingGroupWindow(_, _, size, _, _) if hasTimeIntervalType(size) => true
      case SessionGroupWindow(_, _, _) => true
      case _ => false
    }
    val windowClass = window match {
      case TumblingGroupWindow(_, _, size, _) if hasRowIntervalType(size) =>
        classOf[CountWindow]
      case SlidingGroupWindow(_, _, size, _, _) if hasRowIntervalType(size) =>
        classOf[CountWindow]
      case UserDefinedGroupWindow(_, _, _, _, _, _) => classOf[TimeWindow]
      case _ => classOf[TimeWindow]
    }

    val generator = new AggsHandlerCodeGenerator(
      CodeGeneratorContext(config),
      relBuilder,
      fieldTypeInfos,
      copyInputField = false)

    generator.needAccumulate()
    if (needMerge) {
      generator.needMerge(mergedAccOffset = 0, mergedAccOnHeap = false)
    }
    if (needRetraction) {
      generator.needRetract()
    }

    generator.generateNamespaceAggsHandler(
      "GroupingWindowAggsHandler",
      aggInfoList,
      namedProperties.map(_.property),
      windowClass)
  }

  private def createWindowOperator(
      config: TableConfig,
      aggsHandler: GeneratedNamespaceAggsHandleFunction[_],
      recordEqualiser: GeneratedRecordEqualiser,
      accTypes: Array[LogicalType],
      windowPropertyTypes: Array[LogicalType],
      aggValueTypes: Array[LogicalType],
      inputFields: Seq[LogicalType],
      timeIdx: Int): WindowOperator[_, _] = {

    val builder = WindowOperatorBuilder
      .builder()
      .withInputFields(inputFields.toArray)

    var notUseMiniBatch = false

    val newBuilder = window match {
      case TumblingGroupWindow(_, timeField, size, offset)
          if isProctimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.tumble(toDuration(size), toDuration(offset)).withProcessingTime()

      case TumblingGroupWindow(_, timeField, size, offset)
          if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.tumble(toDuration(size), toDuration(offset)).withEventTime(timeIdx)

      case TumblingGroupWindow(_, timeField, size, _)
          if isProctimeAttribute(timeField) && hasRowIntervalType(size) =>
        builder.countWindow(toLong(size))

      case TumblingGroupWindow(_, _, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SlidingGroupWindow(_, timeField, size, slide, offset)
          if isProctimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.sliding(toDuration(size), toDuration(slide), toDuration(offset))
          .withProcessingTime()

      case SlidingGroupWindow(_, timeField, size, slide, offset)
          if isRowtimeAttribute(timeField) && hasTimeIntervalType(size) =>
        builder.sliding(toDuration(size), toDuration(slide), toDuration(offset))
          .withEventTime(timeIdx)

      case SlidingGroupWindow(_, timeField, size, slide, _)
          if isProctimeAttribute(timeField) && hasRowIntervalType(size) =>
        builder.countWindow(toLong(size), toLong(slide))

      case SlidingGroupWindow(_, _, _, _, _) =>
        // TODO: EventTimeTumblingGroupWindow should sort the stream on event time
        // before applying the  windowing logic. Otherwise, this would be the same as a
        // ProcessingTimeTumblingGroupWindow
        throw new UnsupportedOperationException(
          "Event-time grouping windows on row intervals are currently not supported.")

      case SessionGroupWindow(_, timeField, gap)
          if isProctimeAttribute(timeField) =>
        // disable mini-batch for processing-time session-window for now because:
        // when current session window is triggered, there may be some data in cache,
        // after processing these data, the triggered window may have been merged.
        if (config.getConfiguration.getBoolean(
            ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_DISABLE_PROCTIME_SESSION_WINDOW)) {
          notUseMiniBatch = true
        }
        builder.session(toDuration(gap)).withProcessingTime()

      case SessionGroupWindow(_, timeField, gap)
          if isRowtimeAttribute(timeField) =>
        builder.session(toDuration(gap)).withEventTime(timeIdx)

      case UserDefinedGroupWindow(_, _, windowFunction, assignWindowMethodParamTypes,
      hasMergeMethod, paramIndices) => {
        var userDefinedWindowAssigner: WindowAssigner[TimeWindow] = null
        if (hasMergeMethod) {
          userDefinedWindowAssigner = new UserDefinedMergingWindowAssigner(
            assignWindowMethodParamTypes, paramIndices, windowFunction, inputTimeFieldIndex >= 0)
        } else {
          userDefinedWindowAssigner = new UserDefinedWindowAssigner(
            assignWindowMethodParamTypes, paramIndices, windowFunction, inputTimeFieldIndex >= 0)
        }
        builder.assigner(userDefinedWindowAssigner)
        if (inputTimeFieldIndex >= 0) {
          builder.withEventTime(inputTimeFieldIndex)
        } else {
          builder.withProcessingTime();
        }
        // TODO: add user defined trigger.
        builder
      }
    }

    if (emitStrategy.produceUpdates) {
      // mark this operator will send retraction and set new trigger
      newBuilder
        .withSendRetraction()
        .triggering(emitStrategy.getTrigger)
    }

    if (emitStrategy.enableEmitUnchanged) {
      newBuilder.enableEmitUnchanged()
    }

    if (config.getConfiguration.getBoolean(ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED)
        && !notUseMiniBatch) {
      newBuilder.enableMiniBatch()
        .withBundleTrigger(AggregateUtil.createMiniBatchTrigger(config))
    }

    newBuilder
      .aggregate(aggsHandler, recordEqualiser, accTypes, aggValueTypes, windowPropertyTypes)
      .withAllowedLateness(Duration.ofMillis(emitStrategy.getAllowLateness))
      .build()
  }
}
