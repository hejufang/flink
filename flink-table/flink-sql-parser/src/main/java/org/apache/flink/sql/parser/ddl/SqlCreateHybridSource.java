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
import org.apache.flink.sql.parser.error.SqlValidateException;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlNodeList;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;
import org.apache.calcite.util.ImmutableNullableList;

import javax.annotation.Nonnull;

import java.util.List;

/**
 * CREATE HYBRID SOURCE DDL and operator call.
 */
public class SqlCreateHybridSource extends SqlCreate implements ExtendedSqlNode {

	private static final SqlSpecialOperator OPERATOR =
		new SqlSpecialOperator("CREATE HYBRID SOURCE", SqlKind.CREATE_TABLE);

	private final SqlIdentifier hybridSourceName;
	private final SqlIdentifier streamViewOrTableName;
	private final SqlIdentifier batchViewOrTableName;
	private final SqlNodeList propertyList;

	public SqlCreateHybridSource(
			SqlParserPos pos,
			SqlIdentifier hybridSourceName,
			SqlIdentifier streamViewOrTableName,
			SqlIdentifier batchViewOrTableName,
			SqlNodeList propertyList) {
		super(OPERATOR, pos, false, false);
		this.hybridSourceName = hybridSourceName;
		this.streamViewOrTableName = streamViewOrTableName;
		this.batchViewOrTableName = batchViewOrTableName;
		this.propertyList = propertyList;
	}

	public SqlIdentifier getHybridSourceName() {
		return hybridSourceName;
	}

	public SqlIdentifier getStreamViewOrTableName() {
		return streamViewOrTableName;
	}

	public SqlIdentifier getBatchViewOrTableName() {
		return batchViewOrTableName;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(hybridSourceName, streamViewOrTableName, batchViewOrTableName, propertyList);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		writer.keyword("HYBRID");
		writer.keyword("SOURCE");
		hybridSourceName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		streamViewOrTableName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AND");
		batchViewOrTableName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("WITH");

		SqlWriter.Frame withFrame = writer.startList("(", ")");
		for (SqlNode property : propertyList) {
			printIndent(writer);
			property.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.endList(withFrame);
	}

	public boolean isIfNotExists() {
		return ifNotExists;
	}

	@Override
	public void validate() throws SqlValidateException {
		// move validation to convert stage.
	}

	private void printIndent(SqlWriter writer) {
		writer.sep(",", false);
		writer.newlineAndIndent();
		writer.print("  ");
	}
}
