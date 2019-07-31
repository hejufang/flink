package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;

import org.apache.calcite.sql.SqlCreate;
import org.apache.calcite.sql.SqlIdentifier;
import org.apache.calcite.sql.SqlKind;
import org.apache.calcite.sql.SqlNode;
import org.apache.calcite.sql.SqlOperator;
import org.apache.calcite.sql.SqlSpecialOperator;
import org.apache.calcite.sql.SqlWriter;
import org.apache.calcite.sql.parser.SqlParserPos;

import java.util.List;

/**
 * CREATE FUNCTION DDL sql call.
 */
public class SqlCreateFunction extends SqlCreate implements ExtendedSqlNode {
	public static final SqlSpecialOperator OPERATOR = new SqlSpecialOperator("CREATE FUNCTION", SqlKind.CREATE_FUNCTION);

	private SqlIdentifier functionName;

	private String className;

	public SqlCreateFunction(SqlParserPos pos, SqlIdentifier functionName, String className) {
		super(OPERATOR, pos, false, false);
		this.functionName = functionName;
		this.className = className;
	}

	@Override
	public SqlKind getKind() {
		return SqlKind.CREATE_FUNCTION;
	}

	@Override
	public SqlOperator getOperator() {
		return null;
	}

	@Override
	public List<SqlNode> getOperandList() {
		return null;
	}

	public SqlIdentifier getFunctionName() {
		return functionName;
	}

	public void setFunctionName(SqlIdentifier functionName) {
		this.functionName = functionName;
	}

	public String getClassName() {
		return className;
	}

	public void setClassName(String className) {
		this.className = className;
	}

	public void unparse(
		SqlWriter writer,
		int leftPrec,
		int rightPrec) {
		writer.keyword("CREATE");
		writer.keyword("FUNCTION");
		functionName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		writer.print("'" + className + "'");
	}

	public void validate() {
	}
}
