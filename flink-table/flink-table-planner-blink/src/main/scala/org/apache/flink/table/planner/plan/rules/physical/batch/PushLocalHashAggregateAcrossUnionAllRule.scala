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
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLocalHashAggregate, BatchExecUnion}

/**
  * A [[PushLocalHashAggregateAcrossUnionAllRule]] tries to push a local LocalHashAggregate across
  * the union-all operator so that we can reduce records on both of union's input subsets.
  * If a LocalHashAggregate operator is immediately above a Union-All operator, this rule
  * transforms the query tree so that the LocalHashAggregate operator moves to a position
  * immediately below the Union-All operator.  Note that a HashAggregate operator is usually
  * generated if a SELECT statement has a GROUP BY clause.
  *
  * for example:
  * SELECT id, SUM(amount) FROM (
  *   SELECT id, amount FROM MyTable1
  *   UNION ALL
  *   SELECT id, amount FROM MyTable2
  * ) as T
  * GROUP BY id
  *
  * The physical plan
  *
  * {{{
  *HashAggregate(isMerge=[true], groupBy=[id], select=[id, Final_SUM(sum$0) AS EXPR$1])
  *+- Exchange(distribution=[hash[id]])
  *   :- LocalHashAggregate(groupBy=[id], select=[id, Partial_SUM(amount) AS sum$0])
  *      +- Union(all=[true], union=[id, amount])
  *      :  +- Calc(select=[id, amount])
  *      :     +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ...
  *      :  +- Calc(select=[id, amount])
  *            +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ...
  * }}}
  * will be rewritten to
  *
  * {{{
  *HashAggregate(isMerge=[true], groupBy=[id], select=[id, Final_SUM(sum$0) AS EXPR$1])
  *+- Exchange(distribution=[hash[id]])
  *   +- Union(all=[true], union=[id, amount])
  *      :- LocalHashAggregate(groupBy=[id], select=[id, Partial_SUM(amount) AS sum$0])
  *      :  +- Calc(select=[id, amount])
  *      :     +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ...
  *      +- LocalHashAggregate(groupBy=[id], select=[id, Partial_SUM(amount) AS sum$0])
  *         +- Calc(select=[id, amount])
  *            +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ...
  * }}}
  */

class PushLocalHashAggregateAcrossUnionAllRule
  extends PushLocalAggregateAcrossUnionAllRuleBase(
    operand(classOf[RelNode],
      operand(classOf[BatchExecLocalHashAggregate],
        operand(classOf[BatchExecUnion], any))),
  "PushLocalHashAggregateAcrossUnionAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    isMatch(call)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    // this method transforms the query tree so that the BatchExecLocalHashAggregate node moves to
    // a position immediately below the Union node.
    pushDownLocalAggregate(call)
  }

}

object PushLocalHashAggregateAcrossUnionAllRule {
  val INSTANCE = new PushLocalHashAggregateAcrossUnionAllRule
}
