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

package org.apache.flink.table.planner.plan.nodes.common

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelNode
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.dag.Transformation
import org.apache.flink.api.java.typeutils.InputTypeConfigurable
import org.apache.flink.runtime.state.KeyGroupRangeAssignment
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.SpecificParallelism
import org.apache.flink.streaming.api.functions.sink.OutputFormatSinkFunction
import org.apache.flink.streaming.api.operators.{ChainingStrategy, SimpleOperatorFactory}
import org.apache.flink.streaming.api.transformations.{OneInputTransformation, PartitionTransformation, SinkTransformation}
import org.apache.flink.streaming.runtime.partitioner.KeyGroupStreamPartitioner
import org.apache.flink.table.api.{TableConfig, TableException}
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.config.ExecutionConfigOptions.{TABLE_EXEC_SINK_UPSERT_MATERIALIZE, UpsertMaterialize}
import org.apache.flink.table.catalog.{CatalogTable, ObjectIdentifier}
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.sink.{DataStreamSinkProvider, DynamicTableSink, OutputFormatProvider, SinkFunctionProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.codegen.EqualiserCodeGenerator
import org.apache.flink.table.planner.plan.nodes.calcite.Sink
import org.apache.flink.table.planner.plan.nodes.physical.FlinkPhysicalRel
import org.apache.flink.table.planner.plan.utils.KeySelectorUtil
import org.apache.flink.table.planner.sinks.TableSinkUtils
import org.apache.flink.table.runtime.connector.sink.SinkRuntimeProviderContext
import org.apache.flink.table.runtime.operators.sink.{SinkNotNullEnforcer, SinkOperator, SinkUpsertMaterializer}
import org.apache.flink.table.runtime.types.InternalSerializers
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo
import org.apache.flink.table.runtime.util.StateTtlConfigUtil
import org.apache.flink.table.types.logical.RowType
import org.apache.flink.types.RowKind

import scala.collection.JavaConversions._

/**
 * Base physical RelNode to write data to an external sink defined by a [[DynamicTableSink]].
 */
class CommonPhysicalSink (
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    inputRel: RelNode,
    tableIdentifier: ObjectIdentifier,
    catalogTable: CatalogTable,
    tableSink: DynamicTableSink,
    needMaterialize: Boolean = false)
  extends Sink(cluster, traitSet, inputRel, tableIdentifier, catalogTable, tableSink)
  with FlinkPhysicalRel {

  /**
   * Common implementation to create sink transformation for both batch and streaming.
   */
  protected def createSinkTransformation(
      env: StreamExecutionEnvironment,
      inputTransformation: Transformation[RowData],
      tableConfig: TableConfig,
      rowtimeFieldIndex: Int,
      isBounded: Boolean,
      changelogMode: ChangelogMode): Transformation[Any] = {
    val inputTypeInfo = new RowDataTypeInfo(FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val runtimeProvider = tableSink.getSinkRuntimeProvider(
      new SinkRuntimeProviderContext(isBounded))

    var parallelism = -1

    val notNullEnforcer = tableConfig.getConfiguration
      .get(ExecutionConfigOptions.TABLE_EXEC_SINK_NOT_NULL_ENFORCER)
    val sinkChainEnable =
      tableConfig.getConfiguration.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SINK_CHAIN_ENABLE)
    val notNullFieldIndices = TableSinkUtils.getNotNullFieldIndices(catalogTable)
    val fieldNames = catalogTable.getSchema.toPhysicalRowDataType
      .getLogicalType.asInstanceOf[RowType]
      .getFieldNames
      .toList.toArray
    val enforcer = new SinkNotNullEnforcer(notNullEnforcer, notNullFieldIndices, fieldNames)

    runtimeProvider match {
      case provider: DataStreamSinkProvider =>
        val dataStream = new DataStream(env, inputTransformation).filter(enforcer)
        if (!sinkChainEnable) {
          dataStream.disableChaining()
        }
        provider.consumeDataStream(dataStream).getTransformation.asInstanceOf[Transformation[Any]]
          // set useDefaultParallelism to true, so that we can overwrite parallelism for it
          // when pipeline.use-max-source-parallelism-as-default-parallelism is true.
          .setUseDefaultParallelism(true)
      case _ =>
        val sinkFunction = runtimeProvider match {
          case provider: SinkFunctionProvider => {
            val sinkfunction = provider.createSinkFunction()
            if (sinkfunction.isInstanceOf[SpecificParallelism]) {
              parallelism = sinkfunction.asInstanceOf[SpecificParallelism].getParallelism
            }
            sinkfunction
          }
          case provider: OutputFormatProvider => {
            val outputFormat = provider.createOutputFormat()
            if (outputFormat.isInstanceOf[SpecificParallelism]) {
              parallelism = outputFormat.asInstanceOf[SpecificParallelism].getParallelism
            }
            new OutputFormatSinkFunction(outputFormat)
          }
        }

        sinkFunction match {
          case itc: InputTypeConfigurable =>
            // configure the type if needed
            itc.setInputType(inputTypeInfo, env.getConfig)
          case _ => // nothing to do
        }

        val primaryKeyIndices = catalogTable.getSchema.getPrimaryKeyIndices
        val newInputTransformation = if (needMaterialize) {
          val keybyTransform = applyKeyBy(
            tableConfig,
            inputTransformation,
            primaryKeyIndices,
            parallelism
          )
          applyUpsertMaterialize(tableConfig, keybyTransform, primaryKeyIndices)
        } else {
          inputTransformation
        }

        val operator = new SinkOperator(env.clean(sinkFunction), rowtimeFieldIndex, enforcer)
        if (!sinkChainEnable) {
          operator.setChainingStrategy(ChainingStrategy.NEVER)
        }

        val result = new SinkTransformation(
          newInputTransformation,
          getRelDetailedDescription,
          SimpleOperatorFactory.of(operator),
          newInputTransformation.getParallelism).asInstanceOf[Transformation[Any]]
        if (parallelism > 0) {
          result.setParallelism(parallelism)
        }
        result
    }
  }

  private def applyKeyBy(
      config: TableConfig,
      inputTransform: Transformation[RowData],
      primaryKeys: Array[Int],
      sinkParallelism: Int): Transformation[RowData] = {
    val inputTypeInfo = inputTransform.getOutputType.asInstanceOf[RowDataTypeInfo]
    val selector = KeySelectorUtil.getRowDataSelector(primaryKeys, inputTypeInfo, config)
    val partitioner = new KeyGroupStreamPartitioner[RowData, RowData](selector,
      KeyGroupRangeAssignment.DEFAULT_LOWER_BOUND_MAX_PARALLELISM)
    val partitionedTransform = new PartitionTransformation[RowData](inputTransform, partitioner)
    partitionedTransform.setParallelism(sinkParallelism)
    partitionedTransform
  }

  private def applyUpsertMaterialize(
      config: TableConfig,
      inputTransform: Transformation[RowData],
      primaryKeys: Array[Int]): Transformation[RowData] = {
    val ttlMs = config.getConfiguration.get(ExecutionConfigOptions.IDLE_STATE_RETENTION).toMillis
    val ttlConfig = StateTtlConfigUtil.createTtlConfig(ttlMs)

    val inputRowTypeInfo = RowDataTypeInfo.of(
      FlinkTypeFactory.toLogicalRowType(getInput.getRowType))
    val equaliserCodeGen = new EqualiserCodeGenerator(inputRowTypeInfo.getLogicalTypes)
    val generatedEqualiser = equaliserCodeGen.generateRecordEqualiser("SinkMaterializeEqualiser")

    val serializer = InternalSerializers.create(inputRowTypeInfo.toRowType)
      .asInstanceOf[TypeSerializer[RowData]]

    val fieldNames = inputRowTypeInfo.toRowType.getFieldNames
    val operatorName = s"SinkMaterializer(pk=[" +
      s"${primaryKeys.map(idx => fieldNames.get(idx)).mkString(", ")}]"

    val upsertMaterializer = new SinkUpsertMaterializer(ttlConfig, serializer, generatedEqualiser)
    val ret = new OneInputTransformation(
      inputTransform,
      operatorName,
      upsertMaterializer,
      inputTransform.getOutputType,
      inputTransform.getParallelism)

    val selector = KeySelectorUtil.getRowDataSelector(primaryKeys, inputRowTypeInfo, config)
    ret.setStateKeySelector(selector)
    ret.setStateKeyType(selector.getProducedType)

    ret.setHasState(true)
    ret
  }

}
