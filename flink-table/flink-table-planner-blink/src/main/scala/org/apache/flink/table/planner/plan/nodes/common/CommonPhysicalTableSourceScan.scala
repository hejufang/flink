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

import org.apache.flink.api.common.eventtime.WatermarkStrategy
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.datastream.SingleOutputStreamOperator
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.functions.SpecificParallelism
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.connector.source.{DataStreamScanProvider, InputFormatProvider, ScanTableSource, SourceFunctionProvider, SourceProvider}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.plan.schema.TableSourceTable
import org.apache.flink.table.runtime.connector.source.ScanRuntimeProviderContext
import org.apache.flink.table.runtime.typeutils.RowDataTypeInfo

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
 * Base physical RelNode to read data from an external source defined by a [[ScanTableSource]].
 */
abstract class CommonPhysicalTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: TableSourceTable)
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: TableSourceTable = relOptTable.unwrap(classOf[TableSourceTable])

  protected[flink] val tableSource: ScanTableSource =
    tableSourceTable.tableSource.asInstanceOf[ScanTableSource]

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    super.explainTerms(pw)
      .item("fields", getRowType.getFieldNames.asScala.mkString(", "))
  }

  protected def createSourceTransformation(
      env: StreamExecutionEnvironment,
      name: String,
      tableConfig: TableConfig): Transformation[RowData] = {
    val runtimeProvider = tableSource.getScanRuntimeProvider(ScanRuntimeProviderContext.INSTANCE)
    val outRowType = FlinkTypeFactory.toLogicalRowType(tableSourceTable.getRowType)
    val outTypeInfo = new RowDataTypeInfo(outRowType)
    val sourceChainEnable =
      tableConfig.getConfiguration.getBoolean(ExecutionConfigOptions.TABLE_EXEC_SOURCE_CHAIN_ENABLE)

    runtimeProvider match {
      case provider: SourceFunctionProvider =>
        val sourceFunction = provider.createSourceFunction()
        val operator = env
          .addSource(sourceFunction, name, outTypeInfo)
        if (!sourceChainEnable) {
          operator.disableChaining()
        }
        val result = operator.getTransformation
        if (sourceFunction.isInstanceOf[SpecificParallelism]) {
          setParallelism(result, sourceFunction.asInstanceOf[SpecificParallelism])
        }
        result
      case provider: InputFormatProvider =>
        val inputFormat = provider.createInputFormat()
        val result =
          createInputFormatTransformation(env, inputFormat, name, outTypeInfo, sourceChainEnable)
        if (inputFormat.isInstanceOf[SpecificParallelism]) {
          setParallelism(result, inputFormat.asInstanceOf[SpecificParallelism])
        }
        result
      case provider: SourceProvider =>
        // TODO: Push down watermark strategy to source scan
        val strategy: WatermarkStrategy[RowData] = WatermarkStrategy.noWatermarks()
        val operator = env.fromSource(provider.createSource(), strategy, name)
        if (!sourceChainEnable) {
          operator.disableChaining()
        }
        operator.getTransformation
      case provider: DataStreamScanProvider =>
        val dataStream = provider.produceDataStream(env)
        if (!sourceChainEnable && dataStream.isInstanceOf[SingleOutputStreamOperator[RowData]]) {
          dataStream.asInstanceOf[SingleOutputStreamOperator[RowData]]
            .disableChaining().getTransformation
        } else {
          dataStream.getTransformation
        }
    }
  }

  private def setParallelism(
      transformation: Transformation[RowData],
      specificParallelism: SpecificParallelism) = {
    val parallelism = specificParallelism.getParallelism
    if (parallelism > 0) {
      transformation.setParallelism(parallelism)
    }
  }

  /**
   * Creates a [[Transformation]] based on the given [[InputFormat]].
   * The implementation is different for streaming mode and batch mode.
   */
  protected def createInputFormatTransformation(
      env: StreamExecutionEnvironment,
      inputFormat: InputFormat[RowData, _],
      name: String,
      outTypeInfo: RowDataTypeInfo,
      enableChain: Boolean): Transformation[RowData]
}
