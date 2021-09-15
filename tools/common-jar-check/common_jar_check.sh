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

# common jar path need to check, the target jar should not have class start with these.
COMMON_JAR_PATH_LIST=("com.bytedance.commons"
                       "com.fasterxml.jackson"
                       "com.google.protobuf"
                       "com.google.common"
                       "okhttp3"
                       "org.json.simple"
                       "org.byted.infsec"
                       "okio"
                       "com.codahale.metrics"
                       "kotlin"
                       "org.springframework"
                       )

CHECK_LIBS_LIST=("lib/*"
                 "connectors/*"
                 "formats/*")

SKIP_LIBS=("flink-pb"
           "flink-fast-pb"
           "flink-dist")

flink_home="$1"

# record the total num of file conflict
file_conflict_count=0

# check the special jar whether has conflicts with common jar paths
jar_check () {
    local jar_path="$1"
    for element in ${SKIP_LIBS[@]}
    do
        if [[ $jar_path =~ $element ]]; then
            echo "[INFO] [Jar-Conflict-Check] $jar_path is in skip-list, so skip it."
            return
        fi
    done
    echo "[INFO] [Jar-Conflict-Check] Starting to check $jar_path"
    for element in ${COMMON_JAR_PATH_LIST[@]}
    do
        count=`jar -tf $jar_path |grep -Hsi "^$element" | wc -l`
        count=$[ count - 0 ]
        if [ $count -gt 0 ]; then
            echo "[ERROR] [Jar-Conflict-Check] The jar($jar_path) has $count conflict files with COMMON_JAR_PATH($element)."
        fi
        file_conflict_count=$[ count + file_conflict_count ]
    done
}

cd $flink_home

# check special lib, for example "lib/*;connector/*"
multi_jars_check () {
    for lib_path in ${CHECK_LIBS_LIST[@]}
    do
        files=( $(find "$lib_path" -maxdepth 1 -name "*.jar") )
        for filename in $files
        do
            jar_check "$filename"
        done
    done

    if [ $file_conflict_count -gt 0 ]; then
        echo "[ERROR] [Jar-Conflict-Check] There are $file_conflict_count conflict files need to modify, please check the log."
        exit 1
    else
        echo "[INFO] [Jar-Conflict-Check] Good job, there is no conflicts with common jar."
    fi
}

multi_jars_check
