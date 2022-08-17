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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.calcite.plan.RelOptRule.{any, operand}
import org.apache.calcite.plan.RelOptRuleCall
import org.apache.calcite.rel.RelNode
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLocalSortAggregate, BatchExecUnion}

/**
  * A [[PushLocalSortAggregateAcrossUnionAllRule]] tries to push a local LocalSortAggregate across
  * the union-all operator so that we can reduce records on both of union's input subsets.
  * If a LocalSortAggregate operator is immediately above a Union-All operator, this rule
  * transforms the query tree so that the LocalSortAggregate operator moves to a position
  * immediately below the Union-All operator.
  *
  * for example:
  * SELECT COUNT(*) FROM (
  *   SELECT * FROM MyTable1
  *   UNION ALL
  * SELECT * FROM MyTable2
  * ) as T
  *
  * The physical plan
  *
  * {{{
  *SortAggregate(isMerge=[true], select=[Final_COUNT(count1$0) AS EXPR$0])
  *+- Exchange(distribution=[single])
  *   +- LocalSortAggregate(select=[Partial_COUNT(*) AS count1$0])
  *      +- Union(all=[true], union=[$f0])
  *      :  +- Calc(select=[0 AS $f0])
  *      :     +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ....
  *      :  +- Calc(select=[0 AS $f0])
  *            +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ....
 * }}}
  * will be rewritten to
  *
  * {{{
  *SortAggregate(isMerge=[true], select=[Final_COUNT(count1$0) AS EXPR$0])
  *+- Exchange(distribution=[single])
  *   +- Union(all=[true], union=[$f0])
  *      :- LocalSortAggregate(select=[Partial_COUNT(*) AS count1$0])
  *      :  +- Calc(select=[0 AS $f0])
  *      :     +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ....
  *      +- LocalSortAggregate(select=[Partial_COUNT(*) AS count1$0])
  *         +- Calc(select=[0 AS $f0])
  *            +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ....
  * }}}
  */
class PushLocalSortAggregateAcrossUnionAllRule
  extends PushLocalAggregateAcrossUnionAllRuleBase(
    operand(classOf[RelNode],
      operand(classOf[BatchExecLocalSortAggregate],
        operand(classOf[BatchExecUnion], any))),
  "PushLocalSortAggregateAcrossUnionAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    isMatch(call)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    // this method transforms the query tree so that the BatchExecLocalSortAggregate node moves to
    // a position immediately below the Union node.
    pushDownLocalAggregate(call)
  }

}

object PushLocalSortAggregateAcrossUnionAllRule {
  val INSTANCE = new PushLocalSortAggregateAcrossUnionAllRule
}
