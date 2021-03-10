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

package org.apache.flink.table.planner.plan.stats

import org.apache.flink.table.api.internal.TableEnvironmentImpl
import org.apache.flink.table.api.{TableEnvironment, TableException, TableSchema}
import org.apache.flink.table.plan.stats.{ColumnStats, TableStats}
import org.apache.flink.table.planner.delegation.{PlannerBase, StreamPlanner}
import org.apache.flink.table.planner.plan.metadata.FlinkRelMdSize
import org.apache.flink.table.planner.utils.{CollectResultUtil, Logging}
import org.apache.flink.table.types.logical.DecimalType
import org.apache.flink.table.types.logical.LogicalTypeRoot._
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.types.Row
import org.apache.flink.util.StringUtils

import org.apache.calcite.avatica.util.Quoting

import java.util.{List => JList}

import scala.collection.JavaConverters._

/**
 * Util class for analyzing table stats and column stats.
 */
object StatisticGenerator extends Logging {

  /**
   * Analyzes statistics of given table and given columns.
   *
   * @param tableEnv The [[TableEnvironment]] in which the given table name is registered.
   * @param tablePath The table path under which the table is registered in [[TableEnvironment]].
   *                  tablePath can be a single name(e.g. Array("MyTable")) associated with a table
   *                  registered as Table, or can be a nest names
   *                  (e.g. Array("MyCatalog", "MyDb", "MyTable")) associated with a table
   *                  registered as member of an [[org.apache.flink.table.catalog.Catalog]].
   * @param fieldsToAnalyze Field names of the given table to generate [[ColumnStats]].
   *                        fieldsToAnalyze can be either None (all columns will be analyzed)
   *                        or some partial columns.
   *                        Notes: fieldsToAnalyze are case sensitive.
   * @param partitionsToAnalyze Partition properties of the given table to generate [[TableStats]].
   *                            partitionsToAnalyze can be either None
   *                            (all partitions will be analyzed) or some partial partitions.
   * @return [[TableStats]] includes rowCount and the given columns' ColumnStats.
   */
  def generateTableStats(
      tableEnv: TableEnvironment,
      tablePath: Array[String],
      fieldsToAnalyze: Option[Array[String]] = None,
      partitionsToAnalyze: Option[Map[String, String]] = None,
      isForAllColumns: Boolean = true): TableStats = {
    require(tablePath != null && tablePath.nonEmpty, "tablePath must not be null or empty.")
    val tableName = tablePath.mkString(".")
    val (statsSql, selectedFields) = generateAnalyzeTableSql(
      tableEnv, tablePath, fieldsToAnalyze, partitionsToAnalyze, isForAllColumns)

    val table = tableEnv.sqlQuery(statsSql)
    val results: JList[Row] = CollectResultUtil.collect(
      table,
      s"Analyze TableStats for $tableName",
      tableEnv.getCurrentCatalog,
      tableEnv.getCurrentDatabase)
    if (results.size != 1) {
      throw new TableException("Analyze table has no computed result")
    }
    val result = results.get(0)

    val rowCount = result.getField(0).asInstanceOf[Long]
    val numOfColStats = 6
    val colStatsMap = selectedFields.zipWithIndex.map {
      case (columnName, index) =>
        val ndv = result.getField(index * numOfColStats + 1).asInstanceOf[Long]
        val nullCount = result.getField(index * numOfColStats + 2).asInstanceOf[Long]
        val avgLen = result.getField(index * numOfColStats + 3).asInstanceOf[Double]
        val maxLen = result.getField(index * numOfColStats + 4).asInstanceOf[Integer]
        val max = result.getField(index * numOfColStats + 5).asInstanceOf[Comparable[Any]]
        val min = result.getField(index * numOfColStats + 6).asInstanceOf[Comparable[Any]]
        (columnName, ColumnStats.Builder.builder()
          .setNdv(ndv)
          .setNullCount(nullCount)
          .setAvgLen(avgLen)
          .setMaxLen(maxLen)
          .setMax(max)
          .setMin(min)
          .build())
    }.toMap

    new TableStats(rowCount, colStatsMap.asJava)
  }

  /**
   * generate SQL statements for analyzing table
   */
  def generateAnalyzeTableSql(
      tableEnv: TableEnvironment,
      tablePath: Array[String],
      fieldsToAnalyze: Option[Array[String]] = None,
      partitionsToAnalyze: Option[Map[String, String]] = None,
      isForAllColumns: Boolean = true): (String, Array[String]) = {
    require(tablePath != null && tablePath.nonEmpty, "tablePath must not be null or empty.")

    val planner = getPlanner(tableEnv)
    if (planner.isInstanceOf[StreamPlanner]) {
      throw new TableException("Analyze table in streaming mode is not supported.")
    }

    val tableName = tablePath.mkString(".")
    val sourceTable = tableEnv.from(tableName)

    val schema = sourceTable.getSchema
    val allFieldNames = schema.getFieldNames
    val quoting = planner.createFlinkPlanner.config.getParserConfig.quoting()

    val selectedFields = fieldsToAnalyze match {
      case Some(names) =>
        // Case is sensitive here
        val notExistColumns = names.filter(n => !allFieldNames.contains(n))
        if (notExistColumns.nonEmpty) {
          throw new TableException(
            s"Column(s): ${notExistColumns.mkString(", ")} is(are) not found in table: $tableName.")
        }
        if (isForAllColumns) allFieldNames else names
      case _ => allFieldNames
    }

    val partitionFilter = partitionsToAnalyze match {
      case Some(partitions) =>
        partitions.view map {
          case (partition, property) => "%s = %s" format (withQuoting(partition, quoting), property)
        } mkString " AND "
      case _ => ""
    }

    val tableNameWithQuoting = tablePath.map(withQuoting(_, quoting)).mkString(".")
    val rowCountStats = "CAST(COUNT(1) AS BIGINT)"

    val statsSqlSelectParts = if (selectedFields.nonEmpty) {
      val columnStatsSelects = getColumnStatsSelects(tableEnv, schema, selectedFields, quoting)
      s"SELECT $rowCountStats, $columnStatsSelects FROM $tableNameWithQuoting"
    } else {
      s"SELECT $rowCountStats FROM $tableNameWithQuoting"
    }

    val statsSql = if (StringUtils.isNullOrWhitespaceOnly(partitionFilter)) {
      statsSqlSelectParts
    } else {
      s"$statsSqlSelectParts WHERE $partitionFilter"
    }

    if (LOG.isDebugEnabled) {
      LOG.debug(s"Analyze TableStats for $tableName, SQL: $statsSql")
    }
    (statsSql, selectedFields)
  }

  /**
   * generate SQL select items of [[ColumnStats]] for given columns.
   *
   * @param tableEnv The [[TableEnvironment]] in which the given table name is registered.
   * @param schema The schema of the table.
   * @param fieldsToAnalyze Field names of the given table to generate [[ColumnStats]].
   * @param quoting quoting column name in SQL select statements.
   * @return SQL select items for given columns.
   */
  private def getColumnStatsSelects(
      tableEnv: TableEnvironment,
      schema: TableSchema,
      fieldsToAnalyze: Array[String],
      quoting: Quoting): String = {
    require(fieldsToAnalyze.nonEmpty)

    val allFieldNames = schema.getFieldNames

    // is all fields support APPROX_COUNT_DISTINCT
    val allFieldsSupportApproxCountDistinct = fieldsToAnalyze.forall { field =>
      val fieldIndex = allFieldNames.indexOf(field)
      val fieldType = TypeConversions.fromDataToLogicalType(schema.getFieldDataTypes()(fieldIndex))
      fieldType.getTypeRoot match {
        case TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DECIMAL | BOOLEAN | BINARY |
             DATE | TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE | VARCHAR | CHAR |
             VARBINARY => true
        case _ => false
      }
    }

    val typeFactory = getPlanner(tableEnv).getTypeFactory
    val columnStatsSelects = fieldsToAnalyze.map { field =>
      val fieldIndex = allFieldNames.indexOf(field)
      val fieldType = TypeConversions.fromDataToLogicalType(schema.getFieldDataTypes()(fieldIndex))
      val relDataType = typeFactory.createFieldTypeFromLogicalType(fieldType)
      val (isComparableType, sqlTypeName) = fieldType.getTypeRoot match {
        case BOOLEAN | TINYINT | SMALLINT | INTEGER | BIGINT | FLOAT | DOUBLE | DECIMAL | DATE |
            VARCHAR | TIME_WITHOUT_TIME_ZONE | TIMESTAMP_WITHOUT_TIME_ZONE |
            TIMESTAMP_WITH_TIME_ZONE | TIMESTAMP_WITH_LOCAL_TIME_ZONE =>
          val sqlTypeName = if (fieldType.getTypeRoot == DECIMAL) {
            val d = fieldType.asInstanceOf[DecimalType]
            s"${relDataType.getSqlTypeName}(${d.getPrecision}, ${d.getScale})"
          } else {
            relDataType.getSqlTypeName
          }
          (true, sqlTypeName)
        case _ => (false, "UNKNOWN") // `CAST(NULL AS UNKNOWN)` for non-comparable type
      }
      val isStringType = fieldType.getTypeRoot == VARCHAR || fieldType.getTypeRoot == CHAR

      val fixLen = FlinkRelMdSize.averageTypeValueSize(relDataType)

      // Adds CAST here to make sure that the result values are expected types
      val columnNameWithQuoting = withQuoting(field, quoting)
      // Currently, APPROX_COUNT_DISTINCT and COUNT DISTINCT cannot be used in the same query.
      val computeNdv = if (allFieldsSupportApproxCountDistinct) {
        s"CAST(APPROX_COUNT_DISTINCT($columnNameWithQuoting) AS BIGINT)"
      } else {
        s"CAST(COUNT(DISTINCT $columnNameWithQuoting) AS BIGINT)"
      }
      val computeNullCount = s"CAST((COUNT(1) - COUNT($columnNameWithQuoting)) AS BIGINT)"
      val computeAvgLen = if (isStringType) {
        s"CAST(AVG(CAST(CHAR_LENGTH($columnNameWithQuoting) AS DOUBLE)) AS DOUBLE)"
      } else {
        s"CAST($fixLen AS DOUBLE)"
      }
      val computeMaxLen = if (isStringType) {
        s"CAST(MAX(CHAR_LENGTH($columnNameWithQuoting)) AS INTEGER)"
      } else {
        s"CAST($fixLen AS INTEGER)"
      }
      val computeMax = if (isComparableType) {
        s"CAST(MAX($columnNameWithQuoting) AS $sqlTypeName)"
      } else {
        s"CAST(NULL AS $sqlTypeName)"
      }
      val computeMin = if (isComparableType) {
        s"CAST(MIN($columnNameWithQuoting) AS $sqlTypeName)"
      } else {
        s"CAST(NULL AS $sqlTypeName)"
      }

      Seq(computeNdv,
        computeNullCount,
        computeAvgLen,
        computeMaxLen,
        computeMax,
        computeMin).mkString(", ")
    }

    columnStatsSelects.mkString(", ")
  }

  /**
   * Returns name with quoting.
   */
  private def withQuoting(name: String, quoting: Quoting): String = {
    if (name.contains(quoting.string)) {
      throw new TableException(s"$name contains ${quoting.string}, that is not supported now.")
    }
    quoting match {
      case Quoting.BRACKET => s"[$name]"
      case _ => s"${quoting.string}$name${quoting.string}"
    }
  }

  private def getPlanner(tableEnv: TableEnvironment): PlannerBase = {
    tableEnv.asInstanceOf[TableEnvironmentImpl].getPlanner.asInstanceOf[PlannerBase]
  }
}
