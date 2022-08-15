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
package org.apache.flink.table.planner.plan.batch.sql

import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala._
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.table.api._
import org.apache.flink.table.api.config.{ExecutionConfigOptions, TableConfigOptions}
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.catalog.{CatalogTableImpl, ObjectPath}
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.plan.rules.physical.batch.BatchExecSortMergeJoinRule
import org.apache.flink.table.planner.plan.utils.HiveUtils
import org.apache.flink.table.planner.utils.{BatchTableTestUtil, TableTestBase}
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils

import scala.collection.JavaConverters._
import org.junit.{Before, Test}

class BucketTableTest extends TableTestBase {
  private val BUCKET_TABLE_A = "bucketTableA"
  private val BUCKET_TABLE_B = "bucketTableB"

  protected val util: BatchTableTestUtil = batchTestUtil()

  val stats = new CatalogTableStatistics(1200000, -1, -1, -1)
  util.addTableSource[(Int, Long, String)]("MyTable1", 'a, 'b, 'c)
  util.addTableSource[(Int, Long, Int, String, Long)]("MyTable2", 'd, 'e, 'f, 'g, 'h)
  registerBucketTable[(Int, Long, String, Long, String, Long)](
    BUCKET_TABLE_A, Array("bucketColA1", "bucketColA2"),
    4, Some(stats), 'a1, 'bucketColA1, 'a2, 'bucketColA2, 'a3, 'a4)
  registerBucketTable[(Int, Long, String, Long, String)](
    BUCKET_TABLE_B, Array("bucketColB1", "bucketColB2"), 2, None,
    'b1, 'bucketColB1, 'b2, 'bucketColB2, 'b3)

  @Before
  def before(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "NestedLoopJoin, BroadcastHashJoin, SortMergeJoin, SortAgg")
    util.tableConfig.getConfiguration.setBoolean(
      TableConfigOptions.TABLE_EXEC_SUPPORT_HIVE_BUCKET, true)
    util.tableConfig.getConfiguration.setBoolean(
      BatchExecSortMergeJoinRule.TABLE_OPTIMIZER_SMJ_REMOVE_SORT_ENABLED, true)
  }

  @Test
  def testBucketGroupByBucketCol(): Unit = {
    util.verifyTransformation(
      s"select sum(a1) from $BUCKET_TABLE_A group by bucketColA1, bucketColA2")
  }

  @Test
  def testBucketGroupByWithNonBucketCol(): Unit = {
    util.verifyTransformation(
      s"select sum(a1) from $BUCKET_TABLE_A group by bucketColA1, bucketColA2, a3")
  }

  @Test
  def testBucketJoin(): Unit = {
    util.verifyTransformation(
      s"""
        |select A.bucketColA1, B.bucketColB2, A.a1 + 1, B.b1 + 1  from
        |   $BUCKET_TABLE_A A left join
        |   $BUCKET_TABLE_B B
        |on A.bucketColA1 = B.bucketColB1 and A.bucketColA2 = B.bucketColB2
        | where A.a1 > 0 and B.b1 > 0
        |""".stripMargin)
  }

  @Test
  def testBucketJoinWithGroupBy(): Unit = {
    util.verifyTransformation(
      s"""
         |select a.bucketColA1, a.bucketColA2 from
         |(select bucketColA1, bucketColA2, sum(a1) as a1
         |from $BUCKET_TABLE_A
         |group by bucketColA1, bucketColA2) a
         |join MyTable2 t1 on a.bucketColA1 = t1.e and a.bucketColA2 = t1.h
         |""".stripMargin
    )
  }

  @Test
  def testBucketJoinWithGroupBy2(): Unit = {
    util.verifyTransformation(
      s"""SELECT
         |    a.b1, a.b2, a.f, a.g,
         |    t.a1, t.a4, CASE WHEN a.b1 IS NULL THEN 0 ELSE 1 END case_when
         |FROM    (
         |    SELECT
         |        bucketColA1, sum(a1) as a1, sum(a4) as a4, bucketColA2
         |    FROM    (
         |                SELECT
         |                       bucketColA1, IF(a1 > 0 or a4 < 1, 1, 0) AS a1, bucketColA2,
         |                       IF(a4 > 0 or a1 > 1, 1, 0) AS a4
         |                FROM    bucketTableA
         |            ) t1
         |    GROUP BY
         |            bucketColA1, bucketColA2) t
         |LEFT JOIN
         |        (
         |            SELECT  e as b1, h as b2, f, g
         |            FROM    MyTable2 where f > 0
         |        ) a
         |ON t.bucketColA1 = a.b1 AND t.bucketColA2 = a.b2
         |where true
         |""".stripMargin
    )
  }

  @Test
  def testSortAgg(): Unit = {
    util.tableEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_DISABLED_OPERATORS,
      "NestedLoopJoin, BroadcastHashJoin, SortMergeJoin")
    util.verifyTransformation(
      s"""
         | select bucketColA1, bucketColA2, sum(a1) from
         | $BUCKET_TABLE_A group by bucketColA1, bucketColA2
         |""".stripMargin)
  }

  private def registerBucketTable[T: TypeInformation](
      tableName: String,
      bucketCols: Array[String],
      bucketNum: Int,
      catalogStats: Option[CatalogTableStatistics],
      fields: Expression*): Unit = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val tableSchema =
      if (fields.isEmpty) {
        val fieldTypes: Array[TypeInformation[_]] = typeInfo match {
          case tt: TupleTypeInfo[_] => (0 until tt.getArity).map(tt.getTypeAt).toArray
          case ct: CaseClassTypeInfo[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
          case at: AtomicType[_] => Array[TypeInformation[_]](at)
          case pojo: PojoTypeInfo[_] => (0 until pojo.getArity).map(pojo.getTypeAt).toArray
          case _ => throw new TableException(s"Unsupported type info: $typeInfo")
        }
        val types = fieldTypes.map(TypeConversions.fromLegacyInfoToDataType)
        val names = FieldInfoUtils.getFieldNames(typeInfo)
        TableSchema.builder().fields(names, types).build()
      } else {
        FieldInfoUtils.getFieldsInfo(typeInfo, fields.toArray).toTableSchema
      }

    val properties = getBucketProperty(bucketCols, bucketNum)
    val catalogTable = new CatalogTableImpl(tableSchema, properties, "")
    val objectPath = new ObjectPath(util.getTableEnv.getCurrentDatabase, tableName)
    val catalog = util.getTableEnv.getCatalog(util.getTableEnv.getCurrentCatalog).get()
    catalog.createTable(objectPath, catalogTable, true)
    if (catalogStats.isDefined) {
      catalog.alterTableStatistics(objectPath, catalogStats.get, false)
    }
  }

  private def getBucketProperty(
      bucketCols: Array[String],
      bucketNum: Int): java.util.Map[String, String] = {
    val property = new java.util.HashMap[String, String]()
    val javaCols = bucketCols.toList.asJava
    HiveUtils.addBucketProperties(bucketNum, javaCols, javaCols, property)

    property.put("connector.type", "TestProjectableSource")
    property.put("is-bounded", "true")
    property.put(HiveUtils.PROPERTY_HIVE_COMPATIBLE, "true")
    property
  }
}
