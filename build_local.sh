#!/bin/bash
################################################################################
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
################################################################################

set -eo pipefail

. /etc/profile
export JAVA_HOME=/usr/local/jdk

mvn clean install -DskipTests -T 1C -Pinclude-hadoop -Psql-jars -Pdocs-and-source
mvn clean test -Dsurefire.rerunFailingTestsCount=3 -Dflink.forkCount=2 -Dflink.forkCountTestPackage=2 -pl flink-runtime,flink-core,flink-metrics,flink-table/flink-sql-parser,flink-table/flink-sql-parser-hive,flink-table/flink-table-api-java,flink-table/flink-table-api-java-bridge,flink-table/flink-table-common,flink-table/flink-table-planner-blink,flink-table/flink-table-runtime-blink,flink-table/flink-table-uber-blink,flink-clients
