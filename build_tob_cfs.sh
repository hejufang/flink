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

rm -rf output
rm -rf flink-dist
cp -r flink-dist-tob flink-dist
rm -rf pom.xml
cp pom_tob.xml pom.xml
rm -rf flink-filesystems/pom.xml
cp flink-filesystems/pom_tob.xml flink-filesystems/pom.xml

# compile current branch
mvn clean package -U -DskipTests -Pinclude-hadoop -Dhadoop.version=2.6.0-cdh5.4.4-cfs1.0.1-online1.0.0.77 -Psql-jars -Pdocs-and-source

# copy flink-1.11 to output
mkdir -p output
cp -r flink-dist/target/flink-1.11-byted-SNAPSHOT-bin/flink-1.11-byted-SNAPSHOT/* output/
mkdir output/plugins/s3-fs-presto
cp output/opt/flink-s3-fs-presto-1.11-byted-SNAPSHOT.jar output/plugins/s3-fs-presto/
rm -rf output/opt

# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/"
