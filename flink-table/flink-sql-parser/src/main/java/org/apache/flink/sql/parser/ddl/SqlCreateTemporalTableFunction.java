package org.apache.flink.sql.parser.ddl;

import org.apache.flink.sql.parser.ExtendedSqlNode;
import org.apache.flink.sql.parser.error.SqlParseException;

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
 * CREATE TEMPORAL TABLE FUNCTION DDL and operator call.
 */
public class SqlCreateTemporalTableFunction extends SqlCreate implements ExtendedSqlNode {

	/** Supported properties for Temporal Table Function. **/
	public static final String PRIMARY_KEYS = "primary-keys";
	public static final String TIME_ATTRIBUTE = "time-attribute";

	private static final SqlSpecialOperator OPERATOR =
		new SqlSpecialOperator("CREATE TEMPORAL TABLE FUNCTION", SqlKind.CREATE_TABLE);

	private final SqlIdentifier temporalTableName;
	private final SqlIdentifier viewOrTableName;
	private final SqlNodeList propertyList;

	public SqlCreateTemporalTableFunction(
			SqlParserPos pos,
			SqlIdentifier temporalTableName,
			SqlIdentifier viewOrTableName,
			SqlNodeList propertyList) {
		super(OPERATOR, pos, false, false);
		this.temporalTableName = temporalTableName;
		this.viewOrTableName = viewOrTableName;
		this.propertyList = propertyList;
	}

	public SqlIdentifier getTemporalTableName() {
		return temporalTableName;
	}

	public SqlIdentifier getViewOrTableName() {
		return viewOrTableName;
	}

	public SqlNodeList getPropertyList() {
		return propertyList;
	}

	@Nonnull
	@Override
	public List<SqlNode> getOperandList() {
		return ImmutableNullableList.of(temporalTableName, viewOrTableName, propertyList);
	}

	@Override
	public void unparse(SqlWriter writer, int leftPrec, int rightPrec) {
		writer.keyword("CREATE");
		writer.keyword("TEMPORAL");
		writer.keyword("TABLE");
		writer.keyword("FUNCTION");
		temporalTableName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("AS");
		viewOrTableName.unparse(writer, leftPrec, rightPrec);
		writer.keyword("WITH");

		SqlWriter.Frame withFrame = writer.startList("(", ")");
		for (SqlNode property : propertyList) {
			printIndent(writer);
			property.unparse(writer, leftPrec, rightPrec);
		}
		writer.newlineAndIndent();
		writer.endList(withFrame);
	}

	@Override
	public void validate() throws SqlParseException {
		// move validation to convert stage.
	}

	private void printIndent(SqlWriter writer) {
		writer.sep(",", false);
		writer.newlineAndIndent();
		writer.print("  ");
	}
}
