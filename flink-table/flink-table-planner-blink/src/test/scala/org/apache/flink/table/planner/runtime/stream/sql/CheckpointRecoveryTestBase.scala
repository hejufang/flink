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

package org.apache.flink.table.planner.runtime.stream.sql

import org.apache.flink.api.common.functions.RichMapFunction
import org.apache.flink.configuration.{CheckpointingOptions, Configuration}
import org.apache.flink.runtime.testutils.MiniClusterResourceConfiguration
import org.apache.flink.streaming.api.functions.sink.SinkFunction
import org.apache.flink.streaming.api.functions.source.{RichSourceFunction, SourceFunction}
import org.apache.flink.test.util.MiniClusterWithClientResource
import org.apache.flink.util.TestLogger

import org.junit.{After, Before}
import org.junit.rules.TemporaryFolder
import org.junit.runner.RunWith
import org.junit.runners.Parameterized

import java.math.BigDecimal
import java.util
import java.util.concurrent.CountDownLatch

import scala.collection.mutable

@RunWith(classOf[Parameterized])
class CheckpointRecoveryTestBase(stateBackend: String) extends TestLogger{
  var cluster: MiniClusterWithClientResource = _
  val temporaryFolder = new TemporaryFolder()

  @Before
  def setup(): Unit = {
    val config = new Configuration()

    temporaryFolder.create()
    val checkpointDir = temporaryFolder.newFolder()
    val savepointDir = temporaryFolder.newFolder()

    config.setString(CheckpointingOptions.STATE_BACKEND, stateBackend)
    config.setString(CheckpointingOptions.CHECKPOINTS_DIRECTORY, checkpointDir.toURI.toString)
    config.setString(CheckpointingOptions.SAVEPOINT_DIRECTORY, savepointDir.toURI.toString)

    cluster = new MiniClusterWithClientResource(
      new MiniClusterResourceConfiguration.Builder()
        .setConfiguration(config)
        .setNumberTaskManagers(2)
        .setNumberSlotsPerTaskManager(2)
        .build())
    cluster.before()
  }

  @After
  def close(): Unit = {
    if (cluster != null) {
      cluster.after()
    }
  }
}

object CheckpointRecoveryTestBase {
  @Parameterized.Parameters
  def parameters(): util.Collection[String] = {
    util.Arrays.asList("rocksdb", "filesystem")
  }
}

class TestSource(exitAfterSendData: Boolean) extends RichSourceFunction[
  (Long, Int, Double, Float, BigDecimal, String, String)] {

  val data = List(
    (1L, 1, 1d, 1f, new BigDecimal("1"), "Hi", "a"),
    (2L, 2, 2d, 2f, new BigDecimal("2"), "Hallo", "a"),
    (3L, 2, 2d, 2f, new BigDecimal("2"), "Hello", "a"),
    (4L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (7L, 3, 3d, 3f, new BigDecimal("3"), "Hello", "b"),
    (6L, 5, 5d, 5f, new BigDecimal("5"), "Hello", "a"),
    (8L, 3, 3d, 3f, new BigDecimal("3"), "Hello world", "a"),
    (16L, 4, 4d, 4f, new BigDecimal("4"), "Hello world", "b"))
  var running = true
  var emitted = false

  override def run(ctx: SourceFunction.SourceContext[
    (Long, Int, Double, Float, BigDecimal, String, String)]): Unit = {
    while (running) {
      if (!emitted) {
        emitted = true
        data.foreach(row => {
          ctx.collect(row)
        })
      }
      Thread.sleep(200)

      if (exitAfterSendData) {
        return
      }
    }
  }

  override def cancel(): Unit = {
    running = false
  }
}

class LatchMapper[T] extends RichMapFunction[T, T] {

  override def open(parameters: Configuration): Unit = {
    super.open(parameters)
  }

  override def map(value: T): T = {
    Latch.latch.countDown()
    value
  }
}

class TestSink[T] extends SinkFunction[T] {

  override def invoke(value: T): Unit = {
    TestSink.results += value.toString
    println(value)
  }
}

object TestSink {
  var results: mutable.ArrayBuffer[String] = _

  def reset(): Unit = {
    results = mutable.ArrayBuffer.empty[String]
  }
}

object Latch {
  var latch: CountDownLatch = _

  def init(count: Int): Unit = {
    latch = new CountDownLatch(count)
  }
}
