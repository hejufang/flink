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
set -e

for module in $@
do
  pushd "${module}"
    mvn clean install -DskipTests -T 1C -Pinclude-hadoop -Psql-jars
  popd
done

pushd flink-dist
  mvn clean install -DskipTests -T 1C -Pinclude-hadoop -Psql-jars
popd

cp -r flink-dist/target/flink-1.11-byted-SNAPSHOT-bin/flink-1.11-byted-SNAPSHOT/* output/deploy/flink-1.11/

# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/deploy/flink-1.11/"
