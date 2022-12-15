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
bash tob_config_check.sh

rm -rf output
rm -rf flink-dist/src/main/assemblies
rm -rf flink-dist/src/main/flink-bin
cp -r flink-dist/tob/main/assemblies flink-dist/src/main/assemblies
cp -r flink-dist/tob/main/flink-bin flink-dist/src/main/flink-bin

# compile current branch
mvn clean package -U -T 1C -DskipTests -Dtob-build -Dspecified-modules -Pinclude-hadoop-tob -Dflink.hadoop.version=3.2.1 -Dhadoop-uber.version=3.2.1-cfs-1.3.4 -Psql-jars -Pdocs-and-source | grep -v "Progress"

# copy flink-1.11 to output
mkdir -p output
rm -rf flink-dist/target/flink-1.16-byted-SNAPSHOT-bin/flink-1.16-byted-SNAPSHOT/opt
cp -r flink-dist/target/flink-1.16-byted-SNAPSHOT-bin/flink-1.16-byted-SNAPSHOT/* output/

# tob yaml
mv output/conf/flink-conf-tob.yaml output/conf/flink-conf.yaml

# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/"
