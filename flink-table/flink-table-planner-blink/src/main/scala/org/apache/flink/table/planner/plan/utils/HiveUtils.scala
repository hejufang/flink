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

import org.apache.flink.table.api.ValidationException
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.util.{FlinkRuntimeException, MathUtils}
import org.apache.calcite.rel.{RelCollation, RelCollationTraitDef, RelCollations, RelDistribution}
import org.apache.calcite.rel.`type`.RelDataType
import org.apache.calcite.sql.`type`.SqlTypeName

import scala.collection.JavaConversions._
import java.util
import java.util.{List => JList}

/**
 * HiveUtils.
 */
object HiveUtils {
  val PROPERTY_HIVE_COMPATIBLE = "hive.compatible"
  val PROPERTY_NUM_BUCKET_COLS = "flink.sql.sources.schema.numBucketCols"
  val PROPERTY_BUCKET_COL_PREFIX = "flink.sql.sources.schema.bucketCol."
  val PROPERTY_BUCKET_NUM = "flink.sql.sources.schema.numBuckets"
  val PROPERTY_NUM_SORT_COLS = "flink.sql.sources.schema.numSortCols"
  val PROPERTY_BUCKET_SORT_COL_PREFIX = "flink.sql.sources.schema.sortCol."

  def getBucketNum(options: util.Map[String, String]): Int = {
    val bucketNum = options.getOrDefault(PROPERTY_BUCKET_NUM, "-1").toInt
    if (bucketNum > 0 && !MathUtils.isPowerOf2(bucketNum)) {
      throw new FlinkRuntimeException("Current we only support bucket number is power of 2");
    }
    bucketNum
  }

  def getDistributionFromProperties(
      tableName: JList[String],
      relDataType: RelDataType,
      options: util.Map[String, String]): RelDistribution = {
    val hiveBucket = options.getOrDefault(PROPERTY_HIVE_COMPATIBLE, "false").toBoolean
    if (!hiveBucket) {
      return null
    }
    val numBucket = options.getOrDefault(PROPERTY_BUCKET_NUM, "0").toInt
    if (numBucket == 0) {
      return null
    }
    val numBucketCols = options.getOrDefault(PROPERTY_NUM_BUCKET_COLS, "-1").toInt
    val bucketColIndexList = getColIndexList(tableName,
      PROPERTY_BUCKET_COL_PREFIX, relDataType, numBucketCols, options)

    FlinkRelDistribution.createHiveBucketDistribution(bucketColIndexList)
  }

  def getRelCollationsFromProperties(
      tableName: JList[String],
      relDataType: RelDataType,
      options: util.Map[String, String]): RelCollation = {
    val numBucketCols = options.getOrDefault(PROPERTY_NUM_SORT_COLS, "-1").toInt
    val sortColumns = getColIndexList(tableName,
      PROPERTY_BUCKET_SORT_COL_PREFIX, relDataType, numBucketCols, options)
    val collations = sortColumns.map(
      idx =>
        FlinkRelOptUtil.ofRelFieldCollation(idx.intValue())
    )
    RelCollationTraitDef.INSTANCE.canonize(RelCollations.of(collations.toList))
  }

  private def getColIndexList(
      tableName: util.List[String],
      prefix: String,
      relDataType: RelDataType,
      numCols: Int,
      options: util.Map[String, String]): Seq[Integer] = {
    (0 until numCols)
      .map(i => options.get(prefix + i))
      .map{
        case null => throw new ValidationException(
          s"Table ${tableName.mkString(".")} bucketNum $numCols, this can't happen")
        case name =>
          val dataField = relDataType.getField(name, true, false)
          if (dataField == null) {
            throw new ValidationException(s"Table ${tableName.mkString(".")} fields " +
              s"${relDataType.getFieldNames.mkString(",")} don't exists field $name")
          }
          dataField.getType.getSqlTypeName match {
            case SqlTypeName.BIGINT | SqlTypeName.INTEGER |
                 SqlTypeName.SMALLINT | SqlTypeName.TINYINT =>
            case _ =>
              throw new ValidationException("Current only support int types.")
          }

          Integer.valueOf(dataField.getIndex)
      }
  }

  def addBucketProperties(
      bucketNum: Int,
      bucketCols: util.List[String],
      sortCols: util.List[String],
      properties: util.Map[String, String]): Unit = {
    properties.put(PROPERTY_BUCKET_NUM, bucketNum.toString)
    properties.put(PROPERTY_NUM_BUCKET_COLS, bucketCols.size().toString)
    bucketCols.zipWithIndex.foreach {
      case (col, idx) =>
        properties.put(PROPERTY_BUCKET_COL_PREFIX + idx.toString, col)
    }
    properties.put(PROPERTY_NUM_SORT_COLS, sortCols.size().toString)
    sortCols.zipWithIndex.foreach {
      case (col, idx) =>
        properties.put(PROPERTY_BUCKET_SORT_COL_PREFIX + idx.toString, col)
    }
  }
}
