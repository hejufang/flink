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
# prepare output dir
rm -rf temp_output
mkdir -p temp_output
rm -rf output
mkdir -p output

# compile current branch
if [ "$BUILD_TYPE" == "online" ] && [ "$BUILD_REPO_BRANCH" == "flink-1.9" ] ; then
	mvn clean deploy -U -DskipTests -Pinclude-hadoop -Pdocs-and-source
else
	mvn clean install -U -DskipTests -Pinclude-hadoop
fi
mkdir -p temp_output/deploy
cp -r flink-dist/target/flink-1.9-byted-SNAPSHOT-bin/flink-1.9-byted-SNAPSHOT/flink_deploy/deploy/flink-1.9 temp_output/deploy
mkdir -p temp_output/deploy/flink-1.9/lib
mkdir -p temp_output/deploy/flink-1.9/basejar
cp -r flink-dist/target/flink-1.9-byted-SNAPSHOT-bin/flink-1.9-byted-SNAPSHOT/lib/* temp_output/deploy/flink-1.9/lib/
cp -r flink-dist/target/flink-1.9-byted-SNAPSHOT-bin/flink-1.9-byted-SNAPSHOT/basejar/* temp_output/deploy/flink-1.9/basejar/

git checkout -b flink-1.9 origin/flink-1.9
git branch -D flink-1.5

# compile flink-1.5
git clean -xdf  flink-end-to-end-tests/
git clean -xdf flink-formats/flink-parquet/
git clean -xdf flink-python/
git checkout -b flink-1.5 origin/flink-1.5
mvn clean install -U -DskipTests

# copy to output dir
cp -r flink-dist/target/flink-1.5-byted-SNAPSHOT-bin/flink-1.5-byted-SNAPSHOT/flink_deploy/* output
cp -r temp_output/deploy/flink-1.9 output/deploy
