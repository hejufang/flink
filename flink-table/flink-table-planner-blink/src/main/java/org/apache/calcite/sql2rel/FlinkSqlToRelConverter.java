/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to you under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.calcite.sql2rel;

import org.apache.flink.table.planner.plan.nodes.hive.HiveDistribution;

import org.apache.calcite.plan.RelOptCluster;
import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.prepare.Prepare;
import org.apache.calcite.rel.RelCollations;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSelect;
import org.apache.calcite.sql.validate.SelectScope;
import org.apache.calcite.sql.validate.SqlValidator;
import org.apache.calcite.util.Litmus;

import java.util.ArrayList;
import java.util.List;

import static org.apache.calcite.sql.SqlUtil.stripAs;

/**
 * FlinkSqlToRelConverter.
 */
public class FlinkSqlToRelConverter extends SqlToRelConverter {
	public FlinkSqlToRelConverter(
			RelOptTable.ViewExpander viewExpander,
			SqlValidator validator,
			Prepare.CatalogReader catalogReader,
			RelOptCluster cluster,
			SqlRexConvertletTable convertletTable,
			SqlToRelConverter.Config config) {
		super(viewExpander, validator, catalogReader, cluster, convertletTable, config);
	}

	@Override
	protected void convertSelectImpl(Blackboard bb, SqlSelect select) {
		super.convertSelectImpl(bb, select);
		handleDistributeBy(bb, select);
	}

	private void handleDistributeBy(Blackboard bb, SqlSelect select) {
		SqlNodeList sqlNodes = select.getDistributeBy();
		if (sqlNodes == null || sqlNodes.size() == 0) {
			return;
		}

		if (validator.isAggregate(select)) {
			throw new RuntimeException(
				"`Distribute by` can't used with group by or having clause currently");
		}
		// Scan the select list and order exprs for an identical expression.
		List<Integer> distributeColumns = new ArrayList<>();
		final SelectScope selectScope = validator.getRawSelectScope(select);
		for (SqlNode distributeSqlNode: sqlNodes.getList()) {
			for (int i = 0; i < selectScope.getExpandedSelectList().size(); i++) {
				SqlNode selectItem = selectScope.getExpandedSelectList().get(i);
				if (distributeSqlNode.equalsDeep(stripAs(selectItem), Litmus.IGNORE)) {
					distributeColumns.add(i);
				}
			}
		}

		if (distributeColumns.size() != sqlNodes.size()) {
			throw new RuntimeException("All `Distribute by` columns must contains in select columns.");
		}
		bb.setRoot(
			HiveDistribution.create(
				bb.root, RelCollations.EMPTY, distributeColumns), false);
	}
}
