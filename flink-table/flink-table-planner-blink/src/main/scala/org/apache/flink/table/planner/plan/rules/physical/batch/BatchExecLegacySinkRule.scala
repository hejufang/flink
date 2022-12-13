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

package org.apache.flink.table.planner.plan.rules.physical.batch

import org.apache.flink.table.api.TableException
import org.apache.flink.table.api.config.TableConfigOptions
import org.apache.flink.table.filesystem.FileSystemOptions
import org.apache.flink.table.planner.plan.`trait`.FlinkRelDistribution
import org.apache.flink.table.planner.plan.nodes.FlinkConventions
import org.apache.flink.table.planner.plan.nodes.logical.FlinkLogicalLegacySink
import org.apache.flink.table.planner.plan.nodes.physical.batch.BatchExecLegacySink
import org.apache.flink.table.planner.plan.utils.{FlinkRelOptUtil, HiveUtils}
import org.apache.flink.table.sinks.PartitionableTableSink

import org.apache.calcite.plan.RelOptRule
import org.apache.calcite.rel.convert.ConverterRule
import org.apache.calcite.rel.{RelCollations, RelNode}

import scala.collection.JavaConversions._

class BatchExecLegacySinkRule extends ConverterRule(
    classOf[FlinkLogicalLegacySink],
    FlinkConventions.LOGICAL,
    FlinkConventions.BATCH_PHYSICAL,
    "BatchExecLegacySinkRule") {

  def convert(rel: RelNode): RelNode = {
    val sinkNode = rel.asInstanceOf[FlinkLogicalLegacySink]
    val newTrait = rel.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    val enableBucketWrite = FlinkRelOptUtil.getTableConfigFromContext(rel).getConfiguration
      .getBoolean(TableConfigOptions.TABLE_EXEC_SUPPORT_HIVE_BUCKET_WRITE)
    var requiredTraitSet = if (enableBucketWrite) {
      sinkNode.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    } else {
      sinkNode.getInput.getTraitSet.replace(FlinkConventions.BATCH_PHYSICAL)
    }
    if (sinkNode.catalogTable != null && sinkNode.catalogTable.isPartitioned) {
      sinkNode.sink match {
        case partitionSink: PartitionableTableSink =>
          partitionSink.setStaticPartition(sinkNode.staticPartitions)
          val dynamicPartFields = sinkNode.catalogTable.getPartitionKeys
              .filter(!sinkNode.staticPartitions.contains(_))

          if (dynamicPartFields.nonEmpty) {
            val dynamicPartIndices =
              dynamicPartFields.map(partitionSink.getTableSchema.getFieldNames.indexOf(_))

            val shuffleEnable = sinkNode
                .catalogTable
                .getProperties
                .get(FileSystemOptions.SINK_SHUFFLE_BY_PARTITION.key())

            if (shuffleEnable != null && shuffleEnable.toBoolean) {
              if (enableBucketWrite &&
                    HiveUtils.getBucketNum(sinkNode.catalogTable.getOptions) > 0) {
                throw new TableException(s"Bucket table ${sinkNode.sinkName} write can't used " +
                  s"with dynamic partition, please disable `sink.shuffle-by-partition.enable`.")
              }

              requiredTraitSet = requiredTraitSet.plus(
                FlinkRelDistribution.hash(dynamicPartIndices
                    .map(Integer.valueOf), requireStrict = false))
            }

            if (partitionSink.configurePartitionGrouping(true)) {
              // default to asc.
              val fieldCollations = dynamicPartIndices.map(FlinkRelOptUtil.ofRelFieldCollation)
              requiredTraitSet = requiredTraitSet.plus(RelCollations.of(fieldCollations: _*))
            }
          }
        case _ => throw new TableException("We need PartitionableTableSink to write data to" +
            s" partitioned table: ${sinkNode.sinkName}")
      }
    }

    val newInput = RelOptRule.convert(sinkNode.getInput, requiredTraitSet)

    new BatchExecLegacySink(
      rel.getCluster,
      newTrait,
      newInput,
      sinkNode.sink,
      sinkNode.sinkName)
  }
}

object BatchExecLegacySinkRule {

  val INSTANCE: RelOptRule = new BatchExecLegacySinkRule

}
