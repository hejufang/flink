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

package org.apache.flink.table.planner.plan.nodes.physical

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.InputFormat
import org.apache.flink.api.common.typeinfo.TypeInformation
import org.apache.flink.api.dag.Transformation
import org.apache.flink.core.io.InputSplit
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.streaming.api.transformations.{FakeTransformation, LegacySourceTransformation}
import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter.fromDataTypeToTypeInfo
import org.apache.flink.table.sources.{InputFormatTableSource, StreamTableSource, TableSource}

import org.apache.calcite.plan.{RelOptCluster, RelTraitSet}
import org.apache.calcite.rel.RelWriter
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.rel.core.TableScan

import scala.collection.JavaConverters._

/**
  * Base physical RelNode to read data from an external source defined by a [[TableSource]].
  */
abstract class PhysicalLegacyTableSourceScan(
    cluster: RelOptCluster,
    traitSet: RelTraitSet,
    relOptTable: LegacyTableSourceTable[_])
  extends TableScan(cluster, traitSet, relOptTable) {

  // cache table source transformation.
  protected var sourceTransform: Transformation[_] = _

  protected val tableSourceTable: LegacyTableSourceTable[_] =
    relOptTable.unwrap(classOf[LegacyTableSourceTable[_]])

  protected[flink] val tableSource: TableSource[_] = tableSourceTable.tableSource

  override def deriveRowType(): RelDataType = {
    // TableScan row type should always keep same with its
    // interval RelOptTable's row type.
    relOptTable.getRowType
  }

  override def explainTerms(pw: RelWriter): RelWriter = {
    val optionalHybridSourceInfo = relOptTable.catalogTable.getTableHybridSourceInfo
    val relWriter = super.explainTerms(pw)
    relWriter.item("fields", getRowType.getFieldNames.asScala.mkString(", "))
    if (optionalHybridSourceInfo.isPresent) {
      val hybridSourceInfo = optionalHybridSourceInfo.get()
      val hybridSourceString = s"name=${hybridSourceInfo.getName}, " +
        s"isStreaming=${hybridSourceInfo.isStream}, config=${hybridSourceInfo.getConfig}"
      relWriter.item("hybridSource", hybridSourceString)
    }
    relWriter
  }

  def createInput[IN](
      env: StreamExecutionEnvironment,
      format: InputFormat[IN, _ <: InputSplit],
      t: TypeInformation[IN]): Transformation[IN]

  def getSourceTransformation(
      env: StreamExecutionEnvironment,
      isStreaming: Boolean): Transformation[_] = {
    if (sourceTransform == null) {
      val optionalHybridSourceInfo = relOptTable.catalogTable.getTableHybridSourceInfo
      if (optionalHybridSourceInfo.isPresent) {
        val streamTableSource = tableSource.asInstanceOf[StreamTableSource[_]]
        if (optionalHybridSourceInfo.get().isStream) {
          if (streamTableSource.isBounded) {
            throw new TableException("The first table of hybrid source should be " +
              "streaming table.")
          }
        } else {
          if (!streamTableSource.isBounded) {
            throw new TableException("The second table of hybrid source should be batch table.")
          }
        }
      }
      sourceTransform = tableSource match {
        case format: InputFormatTableSource[_] =>
          // we don't use InputFormatTableSource.getDataStream, because in here we use planner
          // type conversion to support precision of Varchar and something else.
          val typeInfo = fromDataTypeToTypeInfo(format.getProducedDataType)
            .asInstanceOf[TypeInformation[Any]]
          createInput(
            env,
            format.getInputFormat.asInstanceOf[InputFormat[Any, _ <: InputSplit]],
            typeInfo.asInstanceOf[TypeInformation[Any]])
        case s: StreamTableSource[_] => s.getDataStream(env).getTransformation
      }

      if (optionalHybridSourceInfo.isPresent) {
        var realSourceTransformation: Transformation[_] = sourceTransform
        while (realSourceTransformation.getChildren.size() > 0) {
          realSourceTransformation = realSourceTransformation.getChildren.get(0)
        }
        realSourceTransformation match {
          case transformation: LegacySourceTransformation[_] =>
            transformation.setHybridSource(optionalHybridSourceInfo.get())
          // TODO: support flip-27 new source operator.
          case _ => throw new TableException("Unsupported source transformation for " +
            "hybrid source: " + realSourceTransformation.getClass)
        }
      }

      if (isStreaming) {
        sourceTransform = new FakeTransformation(
          sourceTransform, "ChangeToDefaultParallel", ExecutionConfig.PARALLELISM_DEFAULT)
      }
    }
    sourceTransform
  }
}
