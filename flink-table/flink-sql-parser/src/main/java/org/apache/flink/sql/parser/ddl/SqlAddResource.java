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

package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;

import org.apache.calcite.sql.SqlCall;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import javax.annotation.Nonnull;

import java.util.Collections;
import java.util.List;

/**
 * Add Resource sql call.
 */
public class SqlAddResource extends SqlCall implements ExtendedSqlNode {

	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("ADD RESOURCES", SqlKind.OTHER_DDL);

	private SqlIdentifier resourceName;

	public SqlAddResource(SqlParserPos pos, SqlIdentifier resourceName) {
		super(pos);
		this.resourceName = resourceName;
	}

	public SqlIdentifier getResourceName() {
		return resourceName;
	}

	@Nonnull
	@Override
	public SqlOperator getOperator() {
		return OPERATOR;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return Collections.singletonList(resourceName);
	}

	@Override
	public void unparse(
			SqlWriter writer,
			int leftPrec,
			int rightPrec) {
		writer.keyword("ADD");
		writer.keyword("RESOURCES");
		resourceName.unparse(writer, leftPrec, rightPrec);
	}

	@Override
	public void validate() {
	}
}
