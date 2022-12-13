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

package org.apache.flink.table.planner.utils

import org.apache.flink.api.common.ExecutionConfig
import org.apache.flink.api.common.io.statistics.BaseStatistics
import org.apache.flink.api.common.io._
import org.apache.flink.api.common.typeinfo.{AtomicType, TypeInformation}
import org.apache.flink.api.common.typeutils.TypeSerializer
import org.apache.flink.api.java.typeutils.{PojoTypeInfo, RowTypeInfo, TupleTypeInfo}
import org.apache.flink.api.scala.typeutils.CaseClassTypeInfo
import org.apache.flink.configuration.Configuration
import org.apache.flink.core.io.{GenericInputSplit, InputSplit, InputSplitAssigner, LocatableInputSplit}
import org.apache.flink.streaming.api.datastream.{DataStream, DataStreamSink}
import org.apache.flink.table.api.{TableEnvironment, TableException, TableSchema}
import org.apache.flink.table.catalog._
import org.apache.flink.table.catalog.stats.CatalogTableStatistics
import org.apache.flink.table.connector.ChangelogMode
import org.apache.flink.table.connector.source.ScanTableSource.{ScanContext, ScanRuntimeProvider}
import org.apache.flink.table.connector.source.{DynamicTableSource, InputFormatProvider, ScanTableSource}
import org.apache.flink.table.data.RowData
import org.apache.flink.table.expressions.Expression
import org.apache.flink.table.planner.factories.TestValuesTableFactory
import org.apache.flink.table.planner.factories.TestValuesTableFactory.TableSourceWithData
import org.apache.flink.table.planner.plan.utils.HiveUtils
import org.apache.flink.table.planner.runtime.utils.TestData
import org.apache.flink.table.planner.sinks.CollectTableSink
import org.apache.flink.table.sinks.{StreamTableSink, TableSink, TableSinkBase}
import org.apache.flink.table.sources.{InputFormatTableSource, TableSource}
import org.apache.flink.table.types.DataType
import org.apache.flink.table.types.utils.TypeConversions
import org.apache.flink.table.typeutils.FieldInfoUtils
import org.apache.flink.types.Row
import org.apache.flink.util.FlinkRuntimeException

import java.util
import java.util.Optional
import java.util.concurrent.ConcurrentHashMap
import scala.collection.JavaConverters._

/**
 * BucketTableUtils.
 */
object BucketTableUtils {
  private val BUCKET_CATALOG_NAME = "bucket"
  val result: util.Map[Integer, util.List[Row]] = new ConcurrentHashMap[Integer, util.List[Row]]()

  def clear(): Unit = {
    result.clear()
  }

  def registerBucketTable[T: TypeInformation](
      tEnv: TableEnvironment,
      rowsOption: Option[Seq[Row]],
      tableName: String,
      bucketCols: Array[String],
      bucketNum: Int,
      catalogStats: Option[CatalogTableStatistics],
      fields: Expression*): Unit = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val tableSchema = getSchema(typeInfo, fields: _*)
    val properties = getBucketProperty(bucketCols, bucketNum)
    if (rowsOption.isDefined) {
      BucketSourceUtil.registerRows(properties, rowsOption.get, valuesSource = false)
    }
    val catalogTable = new CatalogTableImpl(tableSchema, properties, "")
    val objectPath = new ObjectPath(tEnv.getCurrentDatabase, tableName)
    val catalog = tEnv.getCatalog(tEnv.getCurrentCatalog).get()
    catalog.createTable(objectPath, catalogTable, true)
    if (catalogStats.isDefined) {
      catalog.alterTableStatistics(objectPath, catalogStats.get, false)
    }
  }

  private def getBucketProperty(
      bucketCols: Array[String],
      bucketNum: Int): java.util.Map[String, String] = {
    val property = new java.util.HashMap[String, String]()
    val javaCols = bucketCols.toList.asJava
    HiveUtils.addBucketProperties(bucketNum, javaCols, javaCols, property)

    property.put("connector.type", "TestProjectableSource")
    property.put("is-bounded", "true")
    property.put(HiveUtils.PROPERTY_HIVE_COMPATIBLE, "true")
    property
  }

  def getOrCreateBucketCatalog(tEnv: TableEnvironment): Catalog = {
    val catalogOp = tEnv.getCatalog(BUCKET_CATALOG_NAME)
    if (catalogOp.isPresent) {
      catalogOp.get()
    } else {
      val catalog = new BucketCatalog(BUCKET_CATALOG_NAME)
      tEnv.registerCatalog(BUCKET_CATALOG_NAME, catalog)
      catalog
    }
  }

  def registerBucketCatalogTable[T: TypeInformation](
      tEnv: TableEnvironment,
      rowsOption: Option[Seq[Row]],
      tableName: String,
      bucketCols: Array[String],
      bucketNum: Int,
      legacy: Boolean,
      fields: Expression*): Unit = {
    val typeInfo: TypeInformation[T] = implicitly[TypeInformation[T]]
    val tableSchema = getSchema(typeInfo, fields: _*)
    val cols = bucketCols.toList.asJava
    val rows = rowsOption match {
      case Some(rs) => rs
      case _ => Seq[Row]()
    }
    val catalogTable = if (legacy) {
      LegacyBucketSourceUtil.createBucketCatalogTable(rows,
        tableSchema, bucketNum, cols, cols)
    } else {
      BucketSourceUtil.createBucketCatalogTable(rows,
        tableSchema, bucketNum, cols, cols)
    }
    getOrCreateBucketCatalog(tEnv).createTable(
      new ObjectPath("default", tableName), catalogTable, false)
  }

  private def getSchema(typeInfo: TypeInformation[_], fields: Expression*): TableSchema = {
    if (fields.isEmpty) {
      val fieldTypes: Array[TypeInformation[_]] = typeInfo match {
        case tt: TupleTypeInfo[_] => (0 until tt.getArity).map(tt.getTypeAt).toArray
        case ct: CaseClassTypeInfo[_] => (0 until ct.getArity).map(ct.getTypeAt).toArray
        case at: AtomicType[_] => Array[TypeInformation[_]](at)
        case pojo: PojoTypeInfo[_] => (0 until pojo.getArity).map(pojo.getTypeAt).toArray
        case _ => throw new TableException(s"Unsupported type info: $typeInfo")
      }
      val types = fieldTypes.map(TypeConversions.fromLegacyInfoToDataType)
      val names = FieldInfoUtils.getFieldNames(typeInfo)
      TableSchema.builder().fields(names, types).build()
    } else {
      FieldInfoUtils.getFieldsInfo(typeInfo, fields.toArray).toTableSchema
    }
  }

  class BucketCatalog(name: String) extends GenericInMemoryCatalog(name) {
    override def getTable(tablePath: ObjectPath): CatalogBaseTable = {
      val table = super.getTable(tablePath)
      table match {
        case t: LegacyBucketSourceUtil.BucketCatalogTable =>
          HiveUtils.addBucketProperties(t.bucketNum, t.bucketCols, t.sortCols, t.getProperties)
        case t: BucketSourceUtil.BucketCatalogTable =>
          HiveUtils.addBucketProperties(t.bucketNum, t.bucketCols, t.sortCols, t.getProperties)
        case _ =>
      }
      table
    }
  }
}

object BucketSourceUtil {
  def createBucketCatalogTable(
      rowsOption: Seq[Row],
      tableSchema: TableSchema,
      bucketNum: Int,
      bucketCols: util.List[String],
      sortCols: util.List[String]): CatalogTable = {
    val options = createInputFormatValuesOptions
    registerRows(options, rowsOption, valuesSource = true)
    new BucketCatalogTable(tableSchema, options, "", bucketNum, bucketCols, sortCols)
  }

  def registerRows(
      props: java.util.Map[String, String],
      rows: Seq[Row],
      valuesSource: Boolean): Unit = {
    val dataId = if (valuesSource) {
      TestValuesTableFactory.registerData(rows)
    } else {
      TestLegacyProjectableTableSourceFactory.registerRows(rows)
    }
    props.put("data-id", dataId.toString)
  }

  class BucketCatalogTable(
      tableSchema: TableSchema,
      properties: util.Map[String, String],
      comment: String,
      val bucketNum: Int,
      val bucketCols: util.List[String],
      val sortCols: util.List[String]) extends CatalogTableImpl(tableSchema, properties, comment) {
    override def copy(): CatalogBaseTable =
      new BucketCatalogTable(tableSchema, properties, comment, bucketNum, bucketCols, sortCols)
  }

  class BucketRowDataInputFormat(bucketNum: Int, rows: util.List[Row])
    extends GenericInputFormat[RowData] with BucketInputFormat {
    var curPos: Int = 0
    var parallelism: Int = -1

    override def getBucketNum: Int = bucketNum

    override def setOperatorParallelism(parallelism: Int): Unit = {
      this.parallelism = parallelism
    }

    override def getInputSplitAssigner(
        splits: Array[GenericInputSplit]): DefaultInputSplitAssigner = {
      if (parallelism < 0) {
        throw new FlinkRuntimeException("parallelism is not set for bucket format.")
      }
      super.getInputSplitAssigner(splits)
    }

    override def reachedEnd(): Boolean = {
      curPos == rows.size()
    }

    override def nextRecord(reuse: RowData): RowData = {
      val rowData = TestData.row2GenericRowData(rows.get(curPos))
      curPos += 1
      rowData
    }
  }

  class BucketScanTableSource() extends ScanTableSource with TableSourceWithData {
    var rows: util.List[Row] = _

    override def setTableSourceData(rows: util.Collection[Row]): Unit = {
      println("setTableSourceData")
      this.rows = new util.ArrayList[Row](rows)
    }

    override def getChangelogMode: ChangelogMode = ChangelogMode.insertOnly()

    override def getScanRuntimeProvider(
        runtimeProviderContext: ScanContext): ScanRuntimeProvider = {
      InputFormatProvider.of(new BucketRowDataInputFormat(1, rows))
    }

    override def copy(): DynamicTableSource = {
      val source = new BucketScanTableSource()
      source.setTableSourceData(rows)
      source
    }

    override def asSummaryString(): String = "BucketScanTableSource"
  }

  private def createInputFormatValuesOptions: util.Map[String, String] = {
    val options = new util.HashMap[String, String]()
    options.put("connector", "values")
    options.put("bounded", "true")
    options.put("table-source-class", classOf[BucketScanTableSource].getName)
    options.put("hive.compatible", "true")
    options
  }
}

object LegacyBucketSourceUtil {
  class BucketCollectOutputFormat(bucketNum: Int)
      extends RichOutputFormat[Row] with BucketOutputFormat {
    var resultRows: util.List[Row] = _

    override def getBucketNum: Int = bucketNum

    override def configure(parameters: Configuration): Unit = {}

    override def open(taskNumber: Int, numTasks: Int): Unit = {
      resultRows = BucketTableUtils.result.get(taskNumber)
      if (resultRows == null) {
        resultRows = new util.ArrayList[Row]()
        BucketTableUtils.result.put(taskNumber, resultRows)
      }
    }

    override def writeRecord(record: Row): Unit = {
      System.out.println("Write record")
      resultRows.add(record)
    }

    override def close(): Unit = {
      resultRows = null
    }
  }

  class LegacyBucketTableSink(
        bucketNum: Int,
        produceOutputType: (Array[TypeInformation[_]] => TypeInformation[Row]))
      extends TableSinkBase[Row] with StreamTableSink[Row] {

    private val collectOutputFormat: BucketCollectOutputFormat =
      new BucketCollectOutputFormat(bucketNum)

    override def consumeDataStream(dataStream: DataStream[Row]): DataStreamSink[_] = {
      dataStream.writeUsingOutputFormat(collectOutputFormat)
        .name("collect")
    }

    override protected def copy: TableSinkBase[Row] = {
      new LegacyBucketTableSink(bucketNum, produceOutputType)
    }

    override def getOutputType: TypeInformation[Row] = {
      produceOutputType(getTableSchema.getFieldTypes)
    }
  }

  class BucketSplitWithData(val row: util.List[Row], splitId: Int)
      extends LocatableInputSplit(splitId, Array[String]()) {
  }

  class BucketAssigner(inputSplits: Array[BucketSplitWithData], parallelism: Int)
      extends InputSplitAssigner {
    private val queues = (0 until parallelism)
      .map(_ => new util.LinkedList[BucketSplitWithData])
      .toArray
    inputSplits.foreach(s => queues(s.getSplitNumber % parallelism).add(s))

    override def getNextInputSplit(host: String, taskId: Int): InputSplit = {
      val queue = queues(taskId)
      if (queue.isEmpty) return null
      queue.remove
    }

    override def returnInputSplit(splits: util.List[InputSplit], taskId: Int): Unit = {
      splits.toArray.foreach(s => queues(taskId).add(s.asInstanceOf[BucketSplitWithData]))
    }
  }

  class BucketRowInputFormat(bucketNum: Int, rows: util.List[Row], serializer: TypeSerializer[Row])
      extends RichInputFormat[Row, BucketSplitWithData]() with BucketInputFormat {
    var parallelism: Int = -1
    var cur: Int = 0
    var curRows: util.List[Row] = _

    override def getBucketNum: Int = bucketNum

    override def setOperatorParallelism(parallelism: Int): Unit = {
      this.parallelism = parallelism
    }

    override def configure(parameters: Configuration): Unit = {}

    override def getStatistics(cachedStatistics: BaseStatistics): BaseStatistics = cachedStatistics

    override def createInputSplits(minNumSplits: Int): Array[BucketSplitWithData] = {
      rows.asScala
        .groupBy(r => getLongHash(r.getField(0).asInstanceOf[java.lang.Long]) % bucketNum)
        .values.zipWithIndex.map(row => new BucketSplitWithData(row._1.asJava, row._2)).toArray
    }

    override def getInputSplitAssigner(
        inputSplits: Array[BucketSplitWithData]): InputSplitAssigner = {
      new BucketAssigner(inputSplits, parallelism)
    }

    override def open(split: BucketSplitWithData): Unit = {
      System.out.println(s"Open size ${split.row.size()}")
      curRows = split.row
      cur = 0
    }

    override def reachedEnd(): Boolean = {
      cur >= curRows.size()
    }

    override def nextRecord(reuse: Row): Row = {
      System.out.println(reuse.toString)
      val row = curRows.get(cur)
      cur += 1
      row
    }

    override def close(): Unit = {
      curRows = null
      cur = 0
    }

    private def getLongHash(obj: Object): Int = {
      java.lang.Long.hashCode(obj.asInstanceOf[java.lang.Long]) & Int.MaxValue
    }
  }

  class LegacyInputFormatSource(
      schema: TableSchema,
      format: BucketRowInputFormat,
      dataType: DataType) extends InputFormatTableSource[Row] {
    def this(schema: TableSchema, format: BucketRowInputFormat) =
      this(schema, format, schema.toRowDataType)

    override def getInputFormat: InputFormat[Row, _] = format

    override def getTableSchema: TableSchema = schema

    override def getProducedDataType: DataType = dataType
  }

  class LegacySink() extends CollectTableSink[Row](new RowTypeInfo(_: _*)) {
    override protected def copy: TableSinkBase[Row] = this
  }

  class BucketCatalogTable(
      tableSource: TableSource[Row],
      tableSchema: TableSchema,
      options: util.Map[String, String],
      val bucketNum: Int,
      val bucketCols: util.List[String],
      val sortCols: util.List[String]) extends ConnectorCatalogTable[Row, Row](
        tableSource, null, tableSchema, true) {

    override def getProperties: util.Map[String, String] = options

    override def getOptions: util.Map[String, String] = options

    override def copy(): CatalogBaseTable = {
      new BucketCatalogTable(tableSource, tableSchema, options, bucketNum, bucketCols, sortCols)
    }

    override def getTableSink: Optional[TableSink[Row]] = {
      val sink = new LegacyBucketTableSink(bucketNum, new RowTypeInfo(_: _*))
      Optional.of(sink.configure(tableSchema.getFieldNames, tableSchema.getFieldTypes))
    }
  }

  def createBucketCatalogTable(
      rows: Seq[Row],
      tableSchema: TableSchema,
      bucketNum: Int,
      bucketCols: util.List[String],
      sortCols: util.List[String]): LegacyBucketSourceUtil.BucketCatalogTable = {
    val serializer = tableSchema.toRowType.createSerializer(new ExecutionConfig)
    val bucketInputFormat = new BucketRowInputFormat(bucketNum, rows.toList.asJava, serializer)
    val tableSource = new LegacyInputFormatSource(tableSchema, bucketInputFormat)
    val map = new util.HashMap[String, String]()
    map.put("hive.compatible", "true")
    new BucketCatalogTable(tableSource, tableSchema, map, bucketNum, bucketCols, sortCols)
  }
}
