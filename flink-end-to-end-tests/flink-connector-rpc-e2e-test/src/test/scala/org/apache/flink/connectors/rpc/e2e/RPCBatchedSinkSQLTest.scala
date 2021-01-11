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
import org.apache.flink.streaming.api.TimeCharacteristic
import org.apache.flink.streaming.api.scala.StreamExecutionEnvironment
import org.apache.flink.table.api.EnvironmentSettings
import org.apache.flink.table.api.scala._
import org.apache.flink.table.planner.runtime.utils.{StreamTestSink, StreamingTestBase}
import org.apache.flink.types.Row

import org.apache.thrift.server.{TServer, TSimpleServer}
import org.apache.thrift.transport.{TFramedTransport, TServerTransport}
import org.junit.Assert.assertEquals
import org.junit.{After, Before, Test}
import org.slf4j.LoggerFactory

import java.lang.{Long => JLong}

class RPCBatchedSinkSQLTest extends StreamingTestBase {
  private val LOG = LoggerFactory.getLogger(classOf[RPCBatchedSinkSQLTest])
  private var thriftPort: Int = 0

  val data = List(
    Row.of(new JLong(1L), Array[String]( "dim0")),
    Row.of(new JLong(2L), Array[String]( "dim1")))

  val dataRowType: TypeInformation[Row] = new RowTypeInfo(
    Types.LONG,
    Types.OBJECT_ARRAY(Types.STRING))

  val sinkRowType: TypeInformation[Row] = new RowTypeInfo(
    Types.LONG,
    Types.ROW(Types.MAP(Types.STRING, Types.STRING), Types.INT))

  var server:TSimpleServer = null

  val serviceImpl = new BatchSinkServiceImplTest()

  @Before
  override def before(): Unit ={
    this.env = StreamExecutionEnvironment.getExecutionEnvironment
    env.setParallelism(1)
    this.env.setStreamTimeCharacteristic(TimeCharacteristic.EventTime)
    if (enableObjectReuse) {
      this.env.getConfig.enableObjectReuse()
    }
    val setting = EnvironmentSettings.newInstance().useBlinkPlanner().inStreamingMode().build()
    this.tEnv = StreamTableEnvironment.create(env, setting)

    val (serverTransport: TServerTransport, port) = ThriftTestUtils.tryOpenTServerTransport()
    thriftPort = port
    server = new TSimpleServer(new TServer.Args(serverTransport)
      .transportFactory(new TFramedTransport.Factory())
      .processor(
        new BatchDimMiddlewareService.Processor[BatchDimMiddlewareService.Iface](serviceImpl)))
    new Thread(new Runnable {
      override def run(): Unit = {
        server.serve()
      }
    }).start()
    LOG.info("Test thrift server started")
  }

  @Test
  def testBatch(): Unit = {
    tEnv.sqlUpdate(
      s"""
        |create table rpc_sink(
        |adId bigint,
        |dimensionList array<string>
        |)with
        |(
        |'connector.type' = 'rpc',
        |'connector.consul' = 'xx',
        |'connector.psm' = 'xx',
        |'connector.is-dimension-table'='false',
        |'connector.thrift-service-class' =
        |'org.apache.flink.connectors.rpc.thriftexample.BatchDimMiddlewareService',
        |'connector.thrift-method' = 'GetDimInfosBatch',
        |'connector.batch-size' = '2',
        |'connector.batch-class' =
        |'org.apache.flink.connectors.rpc.thriftexample.GetDimInfosRequest',
        |'connector.test.host-port' = '127.0.0.1:${thriftPort}'
        |)
        |""".stripMargin)
    val streamTable = env.fromCollection(data)(dataRowType)
      .toTable(tEnv, 'adId, 'dimensionList, 'proctime.proctime)
    tEnv.registerTable("T", streamTable)
    tEnv.sqlUpdate("insert into rpc_sink SELECT adId, dimensionList FROM T")
    tEnv.execute("test")

    assertEquals(2, serviceImpl.requests.size())
    assertEquals(1L, serviceImpl.requests.get(0).adId)
    assertEquals("dim0", serviceImpl.requests.get(0).dimensionList.get(0))
    assertEquals(2L, serviceImpl.requests.get(1).adId)
    assertEquals("dim1", serviceImpl.requests.get(1).dimensionList.get(0))
  }

  @After
  def after(): Unit ={
    if(server != null) {
      server.stop()
    }
    LOG.info("Test thrift server stopped")
  }
}
