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

package org.apache.flink.table.planner.plan.batch.sql.agg

import org.apache.flink.table.api.TableException
import org.apache.flink.table.planner.plan.common.DistinctAggregateTestBase

import org.junit.Test

class DistinctAggregateTest extends DistinctAggregateTestBase {

  @Test
  def testApproximateCountDistinct(): Unit = {
    util.verifyPlan("SELECT APPROX_COUNT_DISTINCT(b) FROM MyTable")
  }

  @Test
  def testApproximateCountDistinctAndAccurateDistinctAggregateOnDiffColumn(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("There are both Distinct AggCall and Approximate Distinct AggCall " +
      "in one sql statement, it is not supported yet.")
    util.verifyPlan("SELECT COUNT(DISTINCT a), APPROX_COUNT_DISTINCT(b) FROM MyTable")
  }

  @Test
  def testApproximateCountDistinctAndAccurateDistinctAggregateOnSameColumn(): Unit = {
    thrown.expect(classOf[TableException])
    thrown.expectMessage("There are both Distinct AggCall and Approximate Distinct AggCall " +
      "in one sql statement, it is not supported yet.")
    util.verifyPlan("SELECT COUNT(DISTINCT b), APPROX_COUNT_DISTINCT(b) FROM MyTable")
  }

}
