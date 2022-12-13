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

package org.apache.flink.table.planner.runtime.batch.sql.join

import org.apache.flink.api.scala._
import org.apache.flink.table.api._
import org.apache.flink.table.planner.runtime.utils.BatchTestBase
import org.apache.flink.table.planner.runtime.utils.TestData.{data3, nullablesOfData3, type3}
import org.apache.flink.table.planner.utils.BucketTableUtils
import org.apache.flink.types.Row

import org.junit.jupiter.api.AfterEach
import org.junit.rules.ExpectedException
import org.junit.{Assert, Before, Rule, Test}

import scala.collection.JavaConverters._

/**
 * BucketJoinITCase.
 */
class BucketJoinITCase extends BatchTestBase {
  private val BUCKET_2_TABLE = "bkt_num_2"
  private val BUCKET_4_TABLE = "bkt_num_4"
  private val BUCKET_8_TABLE = "bkt_num_8"

  val expectedException: ExpectedException = ExpectedException.none()

  @Rule
  def thrown: ExpectedException = expectedException

  val bucketARows = (0 until 8).map(i => BatchTestBase.row(i.toLong, i.toInt, s"a$i"))

  BucketTableUtils.registerBucketCatalogTable[(Long, Int, String)](
    tEnv, Some(bucketARows), BUCKET_2_TABLE, Array("id"), 2, true, 'id, 'a2, 'a3)
  BucketTableUtils.registerBucketCatalogTable[(Long, Int, String)](
    tEnv, Some(bucketARows), BUCKET_4_TABLE, Array("id"), 4, true, 'id, 'a2, 'a3)
  BucketTableUtils.registerBucketCatalogTable[(Long, Int, String)](
    tEnv, Some(bucketARows), BUCKET_8_TABLE, Array("id"), 8, true, 'id, 'a2, 'a3)
  registerCollection("Table3", data3, type3, "a, b, c", nullablesOfData3)

  @Before
  override def before(): Unit = {
    super.before()
    tEnv.getConfig.getConfiguration.setString("table.exec.enable-hive-bucket-support", "true")
    tEnv.getConfig.getConfiguration.setString("table.exec.enable-hive-bucket-write-support", "true")
  }

  @Test
  def testBucketWriteError(): Unit = {
    expectedException.expect(classOf[TableException])
    execInsertSqlAndWaitResult(
      """
        |insert into bucket.`default`.bkt_num_8
        |select id, a2, a3 from bucket.`default`.bkt_num_4
        |""".stripMargin)
  }

  @Test
  def testBucketWriteBucket(): Unit = {
    execInsertSqlAndWaitResult(
      """
        |insert into bucket.`default`.bkt_num_4
        |select id, a2, a3 from bucket.`default`.bkt_num_8
        |""".stripMargin)
    Assert.assertEquals(BucketTableUtils.result.size(), 4)
    BucketTableUtils.result.asScala.values.foreach(assertValuesSorted)
  }

  @Test
  def testNonBucketTable2Bucket(): Unit = {
    execInsertSqlAndWaitResult(
      """
        |insert into bucket.`default`.bkt_num_4
        |select cast(a as bigint), cast(b as int), c from Table3
        |""".stripMargin)
    Assert.assertEquals(BucketTableUtils.result.size(), 4)
    BucketTableUtils.result.asScala.values.foreach(assertValuesSorted)
  }

  @Test
  def testSourceMoreThanSink(): Unit = {
    tEnv.getConfig.getConfiguration.setString("table.exec.enable-hive-bucket-support", "false")
    execInsertSqlAndWaitResult(
      """
        |insert into bucket.`default`.bkt_num_4
        |select id, a2, a3 from bucket.`default`.bkt_num_2
        |""".stripMargin)
    Assert.assertEquals(BucketTableUtils.result.size(), 4)
    BucketTableUtils.result.asScala.values.foreach(assertValuesSorted)
  }

  def assertValuesSorted(rows: java.util.List[Row]): Unit = {
    val initArray = rows.asScala.map(r => r.getField(0).asInstanceOf[Long]).toArray
    Assert.assertArrayEquals(initArray, initArray.sorted)
  }

  @AfterEach
  override def after(): Unit = {
    super.after()
    BucketTableUtils.clear()
  }
}
