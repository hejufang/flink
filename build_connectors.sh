#!/usr/bin/env bash
################################################################################
#  Licensed to the Apache Software Foundation (ASF) under one
#  or more contributor license agreements.  See the NOTICE file
#  distributed with this work for additional information
#  regarding copyright ownership.  The ASF licenses this file
#  to you under the Apache License, Version 2.0 (the
#  "License"); you may not use this file except in compliance
#  with the License.  You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
#  Unless required by applicable law or agreed to in writing, software
#  distributed under the License is distributed on an "AS IS" BASIS,
#  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
#  See the License for the specific language governing permissions and
# limitations under the License.
################################################################################
# build connector libraries.
rm -rf output/runtime_files
mkdir -p output/runtime_files
mvn clean install -DskipTests -T 1C -Psql-jars

cd flink-connectors
cp flink-connector-clickhouse/target/flink-connector-clickhouse-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-jdbc/target/flink-jdbc_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-sql-connector-kafka-0.10/target/flink-sql-connector-kafka-0.10_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-redis/target/flink-connector-redis-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-print/target/flink-connector-print-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-filesystem/target/flink-connector-filesystem_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-sql-connector-elasticsearch6/target/flink-sql-connector-elasticsearch6_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-sql-connector-elasticsearch7/target/flink-sql-connector-elasticsearch7_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-elasticsearch6-ad/target/flink-connector-elasticsearch6-ad_2.11-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-metrics/target/flink-connector-metrics-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-rocketmq/target/flink-connector-rocketmq-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-loghouse/target/flink-connector-loghouse-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-bytable/target/flink-connector-bytable-1.9-byted-SNAPSHOT.jar ../output/runtime_files
cp flink-connector-databus/target/flink-connector-databus-1.9-byted-SNAPSHOT.jar ../output/runtime_files

