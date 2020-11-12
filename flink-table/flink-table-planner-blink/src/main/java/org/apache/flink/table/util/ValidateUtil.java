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

package org.apache.flink.table.util;

import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.planner.plan.schema.LegacyTableSourceTable;
import org.apache.flink.table.planner.plan.schema.TableSourceTable;
import org.apache.flink.table.planner.plan.utils.DefaultRelShuttle;
import org.apache.flink.table.sources.TableSource;
import org.apache.flink.table.validate.Validatable;

import org.apache.calcite.plan.RelOptTable;
import org.apache.calcite.rel.RelNode;
import org.apache.calcite.rel.core.TableScan;
import org.apache.calcite.rel.logical.LogicalTableScan;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Validate util.
 */
public class ValidateUtil {
	private static final Logger LOG = LoggerFactory.getLogger(ValidateUtil.class);

	/**
	 * Validate {@link LogicalTableScan} with user and psm in relNode tree if it is {@link Validatable}.
	 * */
	public static void validateTableSourceWithUserAndPsm(RelNode relNode, String user, String psm) {
		LOG.info("Validate table source with user: {}, psm: {}.", user, psm);
		if (relNode == null) {
			return;
		}
		ValidateRelShuttle validateRelShuttle = new ValidateRelShuttle(user, psm);
		relNode.accept(validateRelShuttle);
	}

	private static class ValidateRelShuttle extends DefaultRelShuttle {
		private final String user;
		private final String psm;

		private ValidateRelShuttle(String user, String psm) {
			this.user = user;
			this.psm = psm;
		}

		@Override
		public RelNode visit(TableScan scan) {
			RelOptTable relOptTable = scan.getTable();

			if (relOptTable instanceof LegacyTableSourceTable) {
				LegacyTableSourceTable legacyTableSourceTable = (LegacyTableSourceTable) relOptTable;
				TableSource tableSource = legacyTableSourceTable.tableSource();
				if (tableSource instanceof Validatable) {
					((Validatable) tableSource).validateWithUserOrPsm(user, psm);
				}
			} else if (relOptTable instanceof TableSourceTable) {
				TableSourceTable tableSourceTable = (TableSourceTable) relOptTable;
				DynamicTableSource tableSource = tableSourceTable.tableSource();
				if (tableSource instanceof Validatable) {
					((Validatable) tableSource).validateWithUserOrPsm(user, psm);
				}
			}
			return super.visit(scan);
		}
	}
}
