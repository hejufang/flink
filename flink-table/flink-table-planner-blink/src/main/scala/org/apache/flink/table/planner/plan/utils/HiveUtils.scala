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

import org.apache.flink.table.api.{TableConfig, ValidationException}
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.catalog.CatalogTable
import org.apache.flink.util.{FlinkRuntimeException, MathUtils}

import org.apache.calcite.plan.RelTraitSet
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

  class BucketColumnGetter(
      val tableName: String,
      fieldNames: List[String],
      fieldTypes: List[SqlTypeName]) {
    def this(relDataType: RelDataType, tableNames: JList[String]) = {
      this(tableNames.mkString("."), relDataType.getFieldNames.toList,
        relDataType.getFieldList.map(_.getType.getSqlTypeName).toList)
    }

    def getBucketFieldIndex(name: String): Int = {
      val fieldIndex = fieldNames.indexOf(name)
      if (fieldIndex < 0) {
        throw new ValidationException(s"Table $tableName fields " +
          s"${fieldNames.mkString(",")} don't contains field $name")
      }
      fieldTypes.get(fieldIndex) match {
        case SqlTypeName.BIGINT | SqlTypeName.INTEGER |
             SqlTypeName.SMALLINT | SqlTypeName.TINYINT | SqlTypeName.VARCHAR =>
        case fType =>
          throw new ValidationException(
            "Current only support int, bigint, smallint, tinyint, varchar types, " +
              s"field $name is $fType not in this types.")
      }
      fieldIndex
    }
  }

  def replaceBucketRelTraitSet(
      traitSetInput: RelTraitSet,
      tableConfig: TableConfig,
      bucketColumnGetter: BucketColumnGetter,
      catalog: CatalogTable,
      isSink: Boolean): RelTraitSet = {
    var traitSet = traitSetInput
    val useHiveBucketRead = tableConfig.getConfiguration.getBoolean(
      TableConfigOptions.TABLE_EXEC_SUPPORT_HIVE_BUCKET)
    val useHiveBucketWrite = tableConfig.getConfiguration.getBoolean(
      TableConfigOptions.TABLE_EXEC_SUPPORT_HIVE_BUCKET_WRITE)
    if (useHiveBucketRead && !isSink) {
      traitSet = tryAddDistributionTrait(traitSet, bucketColumnGetter, catalog.getOptions)
    }

    if (useHiveBucketWrite && isSink) {
      traitSet = tryAddDistributionTrait(traitSet, bucketColumnGetter, catalog.getOptions)
      traitSet = tryAddRelCollationTrait(traitSet, bucketColumnGetter, catalog.getOptions)
    }
    traitSet
  }

  def tryAddDistributionTrait(
      relTraitSet: RelTraitSet,
      bucketColumnGetter: BucketColumnGetter,
      options: util.Map[String, String]): RelTraitSet = {
    val distribution = HiveUtils.getDistributionFromProperties(
      bucketColumnGetter, options)
    if (distribution == null) {
      return relTraitSet
    }
    relTraitSet.replace(distribution)
  }

  def tryAddRelCollationTrait(
      relTraitSet: RelTraitSet,
      bucketColumnGetter: BucketColumnGetter,
      options: util.Map[String, String]): RelTraitSet = {
    val relCollation = HiveUtils.getRelCollationsFromProperties(
      bucketColumnGetter, options)
    if (relCollation == null) {
      return relTraitSet
    }
    relTraitSet.replace(relCollation)
  }

  def getBucketNum(options: util.Map[String, String]): Int = {
    val bucketNum = options.getOrDefault(PROPERTY_BUCKET_NUM, "-1").toInt
    if (bucketNum > 0 && !MathUtils.isPowerOf2(bucketNum)) {
      throw new FlinkRuntimeException("Current we only support bucket number is power of 2")
    }
    bucketNum
  }

  def getDistributionFromProperties(
      bucketColumnGetter: BucketColumnGetter,
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
    val bucketColIndexList = getColIndexList(bucketColumnGetter,
      PROPERTY_BUCKET_COL_PREFIX, numBucketCols, options)

    FlinkRelDistribution.createHiveBucketDistribution(bucketColIndexList)
  }

  def getRelCollationsFromProperties(
      bucketColumnGetter: BucketColumnGetter,
      options: util.Map[String, String]): RelCollation = {
    val numBucketCols = options.getOrDefault(PROPERTY_NUM_SORT_COLS, "-1").toInt
    val sortColumns = getColIndexList(
      bucketColumnGetter, PROPERTY_BUCKET_SORT_COL_PREFIX, numBucketCols, options)
    val collations = sortColumns.map(
      idx =>
        FlinkRelOptUtil.ofRelFieldCollation(idx.intValue())
    )
    RelCollationTraitDef.INSTANCE.canonize(RelCollations.of(collations.toList))
  }

  private def getColIndexList(
      bucketColumnGetter: BucketColumnGetter,
      prefix: String,
      numCols: Int,
      options: util.Map[String, String]): Seq[Integer] = {
    (0 until numCols)
      .map(i => options.get(prefix + i))
      .map{
        case null => throw new ValidationException(
          s"Table ${bucketColumnGetter.tableName} bucketNum $numCols, this can't happen")
        case name =>
          val dataField = bucketColumnGetter.getBucketFieldIndex(name)
          Integer.valueOf(dataField)
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
