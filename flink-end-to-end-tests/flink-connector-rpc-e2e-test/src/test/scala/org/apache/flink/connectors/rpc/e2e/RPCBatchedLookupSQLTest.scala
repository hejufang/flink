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

package org.apache.flink.connectors.rpc.e2e

import org.apache.flink.api.common.typeinfo.{TypeInformation, Types}
import org.apache.flink.api.java.typeutils.RowTypeInfo
import org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService
import org.apache.flink.table.api.config.ExecutionConfigOptions
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamingTestBase, TestingAppendSink}
import org.apache.flink.types.Row

import org.apache.flink.connectors.rpc.shaded.org.apache.thrift.server.{TServer, TSimpleServer}
import org.apache.flink.connectors.rpc.shaded.org.apache.thrift.transport.{TFramedTransport, TServerTransport}

import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import org.slf4j.LoggerFactory

import java.lang.{Long => JLong}

class RPCBatchedLookupSQLTest extends StreamingTestBase {
  private val LOG = LoggerFactory.getLogger(classOf[RPCBatchedLookupSQLTest])
  private var thriftPort: Int = 0

  val data = List(
    Row.of(new JLong(1L), Array[String]( "dim0")),
    Row.of(new JLong(2L), Array[String]( "dim1")),
    Row.of(new JLong(3L), Array[String]( "dim2")))

  val dataRowType: TypeInformation[Row] = new RowTypeInfo(
    Types.LONG,
    Types.OBJECT_ARRAY(Types.STRING))

  val sinkRowType: TypeInformation[Row] = new RowTypeInfo(
    Types.LONG,
    Types.ROW(Types.MAP(Types.STRING, Types.STRING), Types.INT))

  var server:TSimpleServer = null

  @Before
  override def before(): Unit ={
    super.before()
    val (serverTransport: TServerTransport, port) = ThriftTestUtils.tryOpenTServerTransport()
    thriftPort = port
    server = new TSimpleServer(new TServer.Args(serverTransport)
      .transportFactory(new TFramedTransport.Factory())
      .processor(
        new BatchDimMiddlewareService.Processor[BatchDimMiddlewareService.Iface]
        (new BatchLookupServiceImplTest())))
    new Thread(new Runnable {
      override def run(): Unit = {
        server.serve()
      }
    }).start()
    LOG.info("Test thrift server started")

    tEnv.getConfig.getConfiguration.setBoolean(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ENABLED, true)
    tEnv.getConfig.getConfiguration.setLong(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_SIZE, 2L)
    tEnv.getConfig.getConfiguration.setString(
      ExecutionConfigOptions.TABLE_EXEC_MINIBATCH_ALLOW_LATENCY, "10s")
  }

  @Test
  def testBatch(): Unit = {
    tEnv.sqlUpdate(
      s"""
        |create table rpc_lookup(
        |adId bigint,
        |dimensionList array<string>,
        |response row<dimensions map<string, string>, code int>
        |)with
        |(
        |'connector.type' = 'rpc',
        |'connector.consul' = 'xx',
        |'connector.psm' = 'xx',
        |'connector.is-dimension-table'='true',
        |'connector.use-batch-lookup' = 'true',
        |'connector.thrift-service-class' =
        |'org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService',
        |'connector.thrift-method' = 'GetDimInfosBatch',
        |'connector.request-list-name' = 'requests',
        |'connector.response-list-name' = 'responses',
        |'connector.test.host-port' = '127.0.0.1:${thriftPort}'
        |)
        |""".stripMargin)
    val streamTable = env.fromCollection(data)(dataRowType)
      .toTable(tEnv, 'adId, 'dimensionList, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)
    val sinkTable = tEnv.sqlQuery("SELECT T.adId, D.response FROM T JOIN rpc_lookup " +
      "for system_time as of T.proctime AS D ON " +
      "T.adId = D.adId and T.dimensionList = D.dimensionList")

    val sink = new TestingAppendSink
    sinkTable.toAppendStream[Row](sinkRowType).addSink(sink)
    env.execute()

    val expected = Seq(
      "1,{dim0=value0},100",
      "2,{dim0=value0},100",
      "3,{dim0=value0},100")
    assertEquals(expected.sorted, sink.getAppendResults.sorted)
  }

  @After
  def after(): Unit ={
    if(server != null) {
      server.stop()
    }
    LOG.info("Test thrift server stopped")
  }
}
