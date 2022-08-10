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
import org.apache.flink.table.planner.plan.nodes.physical.batch.{BatchExecLimit, BatchExecUnion}

/**
  * A [[PushLocalLimitAcrossUnionAllRule]] tries to push a local SortLimit across the union
  * all operator so that we can reduce records on both of union's input subsets.  If a local
  * Limit operator is immediately above a Union-All operator, this rule transforms the query
  * tree so that the Limit operator moves to a position immediately below the Union-All
  * operator.
  *
  * for example:
  * SELECT * FROM (
  *   SELECT * FROM MyTable1
  *   UNION ALL
  *   SELECT * FROM MyTable2
  * ) as T
  * LIMIT 4
  *
  * The physical plan
  *
  * {{{
  *Limit(offset=[0], fetch=[4], global=[true])
  *+- Exchange(distribution=[single])
  *   +- Limit(offset=[0], fetch=[4], global=[false])
  *      +- Union(all=[true], union=[name, id, amount, price])
  *         +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ....
  *         +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ....
  * }}}
  *
  * will be rewritten to
  *
  * {{{
  *Limit(offset=[0], fetch=[4], global=[true])
  *+- Exchange(distribution=[single])
  *   +- Union(all=[true], union=[name, id, amount, price])
  *      :- Limit(offset=[0], fetch=[4], global=[false])
  *      :  +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable1, ....
  *      +- Limit(offset=[0], fetch=[4], global=[false])
  *         +- LegacyTableSourceScan(table=[[default_catalog, default_database, MyTable2, ....
  * }}}
  */
class PushLocalLimitAcrossUnionAllRule
  extends PushLocalLimitAcrossUnionAllRuleBase(
    operand(classOf[RelNode],
      operand(classOf[BatchExecLimit],
        operand(classOf[BatchExecUnion], any))),
  "PushLocalLimitAcrossUnionAllRule") {

  override def matches(call: RelOptRuleCall): Boolean = {
    isMatch(call)
  }

  override def onMatch(call: RelOptRuleCall): Unit = {
    // this method transforms the query tree so that the Limit operator moves to a position
    // immediately below the Union-All operator.
    pushDownLocalLimit(call)
  }

}

object PushLocalLimitAcrossUnionAllRule {
  val INSTANCE = new PushLocalLimitAcrossUnionAllRule
}
