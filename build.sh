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

# compile current branch
if [ "$BUILD_TYPE" == "online" ] && [ "$BUILD_REPO_BRANCH" == "flink-1.10" ] ; then
        mvn clean deploy -U -DskipTests -Pinclude-hadoop -Pdocs-and-source -Drat.skip=true
else
        mvn clean install -U -DskipTests -Pinclude-hadoop -Drat.skip=true
fi

# copy flink-1.10 to output
mkdir -p output/deploy/flink-1.10
rm -rf flink-dist/target/flink-1.10-byted-SNAPSHOT-bin/flink-1.10-byted-SNAPSHOT/opt
cp -r flink-dist/target/flink-1.10-byted-SNAPSHOT-bin/flink-1.10-byted-SNAPSHOT/* output/deploy/flink-1.10/

# copy flink-tools to output
cp -r flink-dist/target/flink-1.10-byted-SNAPSHOT-bin/flink-1.10-byted-SNAPSHOT/flink_deploy/* output/
