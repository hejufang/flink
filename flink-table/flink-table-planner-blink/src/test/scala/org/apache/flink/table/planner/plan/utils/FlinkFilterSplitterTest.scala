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

import org.apache.calcite.rex.{RexBuilder, RexUtil}
import org.apache.calcite.sql.fun.SqlStdOperatorTable._
import org.apache.calcite.sql.`type`.SqlTypeName.INTEGER
import org.apache.flink.table.planner.calcite.{FlinkTypeFactory, FlinkTypeSystem}

import org.junit.Assert.assertEquals
import org.junit.Test

import scala.collection.JavaConverters._

class FlinkFilterSplitterTest {
  private val typeFactory: FlinkTypeFactory = new FlinkTypeFactory(new FlinkTypeSystem())
  private val rexBuilder = new RexBuilder(typeFactory)
  private val intType = typeFactory.createSqlType(INTEGER)
  private val LITERAL0 = rexBuilder.makeLiteral("0")
  private val LITERAL10 = rexBuilder.makeLiteral("10")
  private val a = rexBuilder.makeInputRef(intType, 0)
  private val b = rexBuilder.makeInputRef(intType, 1)
  private val c = rexBuilder.makeInputRef(intType, 2)
  private val A_GREAT_THAN_0 = rexBuilder.makeCall(GREATER_THAN, a, LITERAL0)
  private val B_GREAT_THAN_0 = rexBuilder.makeCall(GREATER_THAN, b, LITERAL0)
  private val C_GREAT_THAN_0 = rexBuilder.makeCall(GREATER_THAN, c, LITERAL0)
  private val A_LESS_THAN_10 = rexBuilder.makeCall(LESS_THAN, a, LITERAL10)
  private val B_LESS_THAN_10 = rexBuilder.makeCall(LESS_THAN, b, LITERAL10)
  private val C_LESS_THAN_10 = rexBuilder.makeCall(LESS_THAN, c, LITERAL10)

  @Test
  def testSingleExpr(): Unit = {
    // a > 0.
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(A_GREAT_THAN_0, 1, rexBuilder)
    assertEquals(A_GREAT_THAN_0, filterMatch.get)
    assertEquals(None, filterNotMatch)
  }

  @Test
  def testExprIndexOutOfRange(): Unit = {
    // b > 0.
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(B_GREAT_THAN_0, 1, rexBuilder)
    assertEquals(None, filterMatch)
    assertEquals(B_GREAT_THAN_0, filterNotMatch.get)
  }

  @Test
  def testExprOrOneOutOfRange(): Unit = {
    // a > 0 or b > 0.
    val rexNode = rexBuilder.makeCall(OR, A_GREAT_THAN_0, B_GREAT_THAN_0)
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(rexNode, 1, rexBuilder)
    assertEquals(None, filterMatch)
    assertEquals(rexNode, filterNotMatch.get)
  }

  @Test
  def testOrMatches(): Unit = {
    val rexNode = rexBuilder.makeCall(
      OR, A_GREAT_THAN_0, B_GREAT_THAN_0, A_LESS_THAN_10, B_LESS_THAN_10)
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(rexNode, 2, rexBuilder)
    assertEquals(rexNode, filterMatch.get)
    assertEquals(None, filterNotMatch)
  }

  @Test
  def testExprAndOneOutOfRange(): Unit = {
    // a > 0 and b > 0
    val rexNode = rexBuilder.makeCall(AND, A_GREAT_THAN_0, B_GREAT_THAN_0)
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(rexNode, 1, rexBuilder)
    assertEquals(A_GREAT_THAN_0, filterMatch.get)
    assertEquals(B_GREAT_THAN_0, filterNotMatch.get)
  }

  @Test
  def testAndExprList(): Unit = {
    // (a > 0 or b > 0) and (c > 0 and a < 10) and (c < 10 or b < 10) and a < c.
    val subExpr1 = rexBuilder.makeCall(OR, A_GREAT_THAN_0, B_GREAT_THAN_0)
    val subExpr2 = rexBuilder.makeCall(AND, C_GREAT_THAN_0, A_LESS_THAN_10)
    val subExpr3 = rexBuilder.makeCall(OR, C_LESS_THAN_10, B_LESS_THAN_10)
    val a_less_than_c = rexBuilder.makeCall(LESS_THAN, a, c)
    val rexNode = rexBuilder.makeCall(AND, subExpr1, subExpr2, subExpr3, a_less_than_c)
    val (filterMatch, filterNotMatch) =
      FlinkFilterSplitter.splitFilter(rexNode, 2, rexBuilder)
    val matchExpect = RexUtil.composeConjunction(
      rexBuilder, Seq(subExpr1, A_LESS_THAN_10).asJava)
    val nonMatchExpect = RexUtil.composeConjunction(
      rexBuilder, Seq(C_GREAT_THAN_0, subExpr3, a_less_than_c).asJava)
    assertEquals(matchExpect, filterMatch.get)
    assertEquals(nonMatchExpect, filterNotMatch.get)
  }
}
