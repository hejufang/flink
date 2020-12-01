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

package org.apache.flink.table.planner.plan.nodes.exec

import org.apache.flink.api.dag.Transformation
import org.apache.flink.runtime.operators.DamBehavior
import org.apache.flink.table.planner.delegation.BatchPlanner
import org.apache.flink.table.planner.utils.Logging

import org.apache.flink.shaded.byted.com.google.common.util.concurrent.ListenableFuture

import java.util.concurrent.Callable

/**
  * Base class for batch ExecNode.
  */
trait BatchExecNode[T] extends ExecNode[BatchPlanner, T] with Logging {

  /**
    * Returns [[DamBehavior]] of this node.
    */
  def getDamBehavior: DamBehavior

  def translateToPlanMix(planner: BatchPlanner, inputIndex: Int)
      : Either[ListenableFuture[Transformation[T]], Transformation[T]] = {
    if (planner.asyncModeEnabled) {
      Left(planner.executorService.submit(
        new Callable[Transformation[T]] {
          override def call(): Transformation[T] = {
            getInputNodes.get(inputIndex).translateToPlan(planner).asInstanceOf[Transformation[T]]
          }
        }
      ))
    } else {
      Right(getInputNodes.get(inputIndex).translateToPlan(planner).asInstanceOf[Transformation[T]])
    }
  }

  def getTransformFromMix(
      input: Either[ListenableFuture[Transformation[T]], Transformation[T]]): Transformation[T] = {
    input match {
      case Left(l) => l.get()
      case Right(r) => r
    }
  }

}
