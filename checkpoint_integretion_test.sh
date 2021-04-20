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
export MAVEN_OPTS=-Xmx1024m

mvn clean install -DskipTests -Dcheckstyle.skip=true -T 1C -Pinclude-hadoop -Psql-jars -Pdocs-and-source

# flink test
mvn clean -Dflink.forkCount=1 -Dflink.forkCountTestPackage=1 -Dtest="org/apache/flink/test/checkpointing/*" test -pl flink-tests
mvn clean -Dflink.forkCount=1 -Dflink.forkCountTestPackage=1 -Dtest="org/apache/flink/test/state/*" test -pl flink-tests
