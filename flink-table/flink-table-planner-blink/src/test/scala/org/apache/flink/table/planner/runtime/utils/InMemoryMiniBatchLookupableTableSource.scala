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

package org.apache.flink.table.planner.runtime.utils

import java.util
import java.util.concurrent.atomic.AtomicInteger

import org.apache.flink.annotation.VisibleForTesting
import org.apache.flink.streaming.api.datastream.DataStream
import org.apache.flink.streaming.api.environment.StreamExecutionEnvironment
import org.apache.flink.table.api.TableSchema
import org.apache.flink.table.data.{GenericRowData, RowData}
import org.apache.flink.table.functions.{AsyncTableFunction, FunctionContext, MiniBatchTableFunction, TableFunction}
import org.apache.flink.table.sources._
import org.apache.flink.table.types.DataType

import scala.annotation.varargs
import scala.collection.JavaConversions._
import scala.collection.JavaConverters._
import scala.collection.mutable

/**
 * A [[InMemoryMiniBatchLookupableTableSource]] which stores table in memory,
 * this is mainly used for testing.
 */
@SerialVersionUID(1L)
class InMemoryMiniBatchLookupableTableSource(
    schema: TableSchema,
    data: List[Seq[AnyRef]])
  extends LookupableTableSource[RowData]
    with StreamTableSource[RowData] {

  val resourceCounter = new AtomicInteger(0)

  override def getLookupFunction(lookupKeys: Array[String]): TableFunction[RowData] = {
    new InMemoryBatchedLookupFunction(schema.getFieldNames, lookupKeys, data, resourceCounter)
  }

  override def getDataStream(execEnv: StreamExecutionEnvironment): DataStream[RowData] = {
    null
  }

  override def isAsyncEnabled: Boolean = false

  override def getProducedDataType: DataType = schema.toRowDataType

  override def getTableSchema: TableSchema = schema

  override def isBounded: Boolean = false

  override def getAsyncLookupFunction(lookupKeys: Array[String]): AsyncTableFunction[RowData] = null

  @VisibleForTesting
  def getResourceCounter: Int = resourceCounter.get()
}

/**
 * A lookup function which find matched rows with the given fields.
 */
@SerialVersionUID(1L)
private class InMemoryBatchedLookupFunction(
    fieldNames: Array[String],
    lookupKeys: Array[String],
    data: List[Seq[AnyRef]],
    resourceCounter: AtomicInteger)
  extends MiniBatchTableFunction[RowData]{
  private var dataMap: Map[RowData, List[RowData]] = null

  override def open(context: FunctionContext): Unit = {
    dataMap = convertDataToMap(lookupKeys)
    resourceCounter.incrementAndGet()
  }

  @varargs
  def eval(inputs: AnyRef*): Unit = {
    throw new IllegalStateException()
  }

  private def convertDataToMap(lookupKeys: Array[String]): Map[RowData, List[RowData]] = {
    val lookupFieldIndexes = lookupKeys.map(fieldNames.indexOf(_))
    val map = mutable.HashMap[RowData, List[RowData]]()
    data.foreach { ele =>
      val key = GenericRowData.of(lookupFieldIndexes.map(ele.get): _*)
      val row = GenericRowData.of(ele: _*)
      val oldValue = map.get(key)
      if (oldValue.isDefined) {
        map.put(key, oldValue.get ++ List(row))
      } else {
        map.put(key, List(row))
      }
    }
    map.toMap
  }

  override def close(): Unit = {
    resourceCounter.decrementAndGet()
  }

  override def eval(keySequenceList: util.List[Array[AnyRef]])
  : util.List[util.Collection[RowData]] = {
    keySequenceList.map(keys => {
      val keyRow = GenericRowData.of(keys: _*)
      dataMap.get(keyRow) match {
        case Some(list) => list.asJavaCollection
        case None => null
      }
    }).toList
  }

}
