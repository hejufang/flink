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

rm -rf output

# compile current branch
if [ "$CUSTOM_USE_CUSTOM_DEPENDENCY_VERSION" == 'true' ] && [ ! -z "$CUSTOM_HTAP_CLIENT_VERSION" ]; then
  mvn clean package -U -T 1C -DskipTests -Dfast -Dbyted-modules -Dspecified-modules -Pinclude-hadoop -Psql-jars -Dhtap.jclient.version=$CUSTOM_HTAP_CLIENT_VERSION | grep -v Progress
else
  mvn clean package -U -T 1C -DskipTests -Dfast -Dbyted-modules -Dspecified-modules -Pinclude-hadoop -Psql-jars | grep -v Progress
fi

# copy flink-1.11 to output
mkdir -p output
rm -rf flink-dist/target/flink-1.11-byted-SNAPSHOT-bin/flink-1.11-byted-SNAPSHOT/opt
cp -r flink-dist/target/flink-1.11-byted-SNAPSHOT-bin/flink-1.11-byted-SNAPSHOT/* output/
