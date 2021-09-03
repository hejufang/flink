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

# The entrypoint script of flink-kubernetes integration.
# It is the command of jobmanager and taskmanager container.

bin=`dirname "$0"`
bin=`cd "$bin"; pwd`

# get Flink config
. "$bin"/config.sh

FLINK_CLASSPATH=`manglePathList $(constructFlinkClassPath):$INTERNAL_HADOOP_CLASSPATHS`

DYNAMIC_FILES=`getDynamicFilesFromFlinkConf`
if [ "$DYNAMIC_FILES" != "" ]; then
        DYNAMIC_FILES=${DYNAMIC_FILES//;/:}
        FLINK_CLASSPATH=$DYNAMIC_FILES:$FLINK_CLASSPATH
fi
echo "FLINK_CLASSPATH = $FLINK_CLASSPATH"

# FLINK_CLASSPATH will be used by KubernetesUtils.java to generate jobmanager and taskmanager start command.
export FLINK_CLASSPATH

echo "Start command : $*"

exec "$@"
