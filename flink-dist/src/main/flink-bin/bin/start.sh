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

target="$0"
# For the case, the executable has been directly symlinked, figure out
# the correct bin path by following its symlink up to an upper bound.
# Note: we can't use the readlink utility here if we want to be POSIX
# compatible.
iteration=0
while [ -L "$target" ]; do
    if [ "$iteration" -gt 100 ]; then
        echo "Cannot resolve path: You have a cyclic symlink in $target."
        break
    fi
    ls=`ls -ld -- "$target"`
    target=`expr "$ls" : '.* -> \(.*\)$'`
    iteration=$((iteration + 1))
done

KEY_JOB_MANAGER_HOST="jobmanager.rpc.address"
KEY_JOB_MANGER_PORT="jobmanager.rpc.port"

# Convert relative path to absolute path
bin=`dirname "$target"`
# get flink config
. "$bin"/config.sh

usage() {
    echo "./start.sh topologyJar topologyYamlConf cluster ...args"
}

usage_exit() {
    usage
    exit 1
}

FLINK_BASE_DIR="$bin/.."

FLINK_LIBS="$FLINK_BASE_DIR/lib"
REQUIRED_LIBS=""
for i in $(ls ${FLINK_LIBS}); do
    if [ -z "$REQUIRED_LIBS" ]; then
        REQUIRED_LIBS=${FLINK_LIBS}"/"${i}
    else
        REQUIRED_LIBS=${REQUIRED_LIBS}":"${FLINK_LIBS}"/"${i}
    fi
done

if [ "$1" = "save_pid" ]
then
    echo $$ > pid
    args=${@:5}
    flink_job_jar=$2
    topology_yaml_conf=$3
    flink_cluster_name=$4
else
    args=${@:4}
    flink_job_jar=$1
    topology_yaml_conf=$2
    flink_cluster_name=$3
fi

echo $flink_job_jar
echo $topology_yaml_conf
echo $flink_cluster_name

if [ ! -n "$FLINK_JOB_JAR" ]; then
    FLINK_JOB_JAR=$flink_job_jar
    if [ ! -n "$FLINK_JOB_JAR" ]; then
        echo "FLINK_JOB_JAR not found."
        usage_exit
    fi
fi

if [ ! -n "$TOPOLOGY_YAML_CONF" ]; then
    TOPOLOGY_YAML_CONF=$topology_yaml_conf
    if [ ! -n "$TOPOLOGY_YAML_CONF" ]; then
        echo "TOPOLOGY_YAML_CONF not found."
        usage_exit
        exit 1
    fi
fi


if [ ! -n "$FLINK_CLUSTER_NAME" ]; then
    FLINK_CLUSTER_NAME=$flink_cluster_name
    if [ ! -n "$FLINK_CLUSTER_NAME" ]; then
        echo "FLINK_CLUSTER_NAME not found"
        usage_exit
    fi
fi

if [ ! -n "$UD_FLINK_CONF_DIR" ]; then
    UD_FLINK_CONF_DIR=$FLINK_CONF_DIR
    if [ ! -n "$UD_FLINK_CONF_DIR" ]; then
        echo "Flink config not found."
    fi
fi

if [ ! -n "$UD_HADOOP_CONF_DIR" ]; then
    UD_HADOOP_CONF_DIR=${FLINK_BASE_DIR}/hadoop-conf
fi

if [ ! -n "$BATCH_RUNNER" ]; then
    BATCH_RUNNER="/opt/tiger/pyjstorm/pytopology/runner/batch_bolt_new.py"
fi

export PATH=/opt/tiger/jdk/jdk1.8.0_131/bin:$PATH
export FLINK_ROOT_DIR

cmd="java -DflinkBatchRunner=${BATCH_RUNNER} \
-Djstorm.jar=$FLINK_JOB_JAR \
-DflinkConfDir=${FLINK_CONF_DIR} \
-Dlog4j.configuration=file:${FLINK_CONF_DIR}/log4j-cli.properties \
-DyamlConf=${TOPOLOGY_YAML_CONF} -DclusterName=${FLINK_CLUSTER_NAME} \
-cp ${FLINK_JOB_JAR}:${REQUIRED_LIBS}:${UD_HADOOP_CONF_DIR} \
storm.kafka.FlinkJStormTopology $args"
echo $cmd
exec $cmd
