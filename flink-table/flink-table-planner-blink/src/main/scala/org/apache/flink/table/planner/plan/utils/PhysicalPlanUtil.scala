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
package org.apache.flink.table.planner.plan.utils

import org.apache.calcite.rel.`type`.RelDataType
import org.apache.flink.api.dag.Transformation
import org.apache.flink.streaming.api.operators.StreamOperatorFactory
import org.apache.flink.streaming.api.transformations.{AbstractMultipleInputTransformation, LegacySourceTransformation, OneInputTransformation, SourceTransformation, TwoInputTransformation, UnionTransformation}
import org.apache.flink.table.api.TableConfig
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.data.RowData
import org.apache.flink.table.data.conversion.DataStructureConverters
import org.apache.flink.table.planner.calcite.FlinkTypeFactory
import org.apache.flink.table.planner.utils.DataTypeDebugLoggingConverter
import org.apache.flink.table.types.utils.LogicalTypeDataTypeConverter
import org.apache.flink.util.FlinkRuntimeException

import scala.collection.JavaConversions._

/**
  * Utilities for physical plans.
  *
  * Since other utils are mainly for each operator,
  * this util focuses on common utility methods for all physical nodes.
  */
object PhysicalPlanUtil {

  def setDebugLoggingConverter(
      tableConfig: TableConfig,
      relDataType: RelDataType,
      operatorFactory: StreamOperatorFactory[_]): Unit = {
    val enabled = tableConfig.getConfiguration.getBoolean(
      TableConfigOptions.OPERATOR_DEBUG_LOGGING_ENABLED)

    if (!enabled) {
      return
    }

    val location = tableConfig.getConfiguration.get(
      TableConfigOptions.OPERATOR_DEBUG_LOGGING_LOCATION)

    val outputType = FlinkTypeFactory.toLogicalRowType(relDataType)
    val dataType = LogicalTypeDataTypeConverter.toDataType(outputType)
    val converter = DataStructureConverters.getConverter(dataType)
    val debugLoggingConverter = new DataTypeDebugLoggingConverter(converter)
    operatorFactory.setDebugLoggingConverter(debugLoggingConverter)
    operatorFactory.setDebugLoggingLocation(location)
  }

  def setDebugLoggingConverter(
      tableConfig: TableConfig,
      relDataType: RelDataType,
      transformation: Transformation[RowData]): Unit = {
    val operatorFactory: StreamOperatorFactory[_] = transformation match {
      case sourceTransformation: SourceTransformation[_] =>
        sourceTransformation.getOperatorFactory
      case oneInputTransformation: OneInputTransformation[_, _] =>
        oneInputTransformation.getOperatorFactory
      case twoInputTransformation: TwoInputTransformation[_, _, _] =>
        twoInputTransformation.getOperatorFactory
      case multipleInputTransformation: AbstractMultipleInputTransformation[_] =>
        multipleInputTransformation.getOperatorFactory
      case legacySourceTransformation: LegacySourceTransformation[_] =>
        legacySourceTransformation.getOperatorFactory
      case unionTransformation: UnionTransformation[RowData] =>
        for (child <- unionTransformation.getInputs) {
          setDebugLoggingConverter(tableConfig, relDataType, child)
        }
        return
      case _ => throw new FlinkRuntimeException("Unexpected transformation: " + transformation
        + ", this is bug, please raise an Oncall for this.")
    }
    if (operatorFactory != null) {
      setDebugLoggingConverter(tableConfig, relDataType, operatorFactory)
    }
  }
}
