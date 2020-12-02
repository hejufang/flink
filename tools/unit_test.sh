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

HERE="`dirname \"$0\"`"				# relative
HERE="`( cd \"$HERE\" && pwd )`" # absolutized and normalized

ARTIFACTS_DIR="${HERE}/artifacts"
mkdir -p $ARTIFACTS_DIR || {
  echo "[ERROR] FAILURE: cannot create log directory '${ARTIFACTS_DIR}'."
  exit 1
}

CMD_PID="${ARTIFACTS_DIR}/watchdog.mvn.pid"
CMD_EXIT="${ARTIFACTS_DIR}/watchdog.mvn.exit"
CMD_OUT="${ARTIFACTS_DIR}/mvn.out"

TRACE_OUT="${ARTIFACTS_DIR}/jps-traces.out"

if [ -z "$HERE" ]; then
  # error; for some reason, the path is not accessible
  # to the script (e.g. permissions re-evaled after suid)
  exit 1 # fail
fi

source "${HERE}/ci/common_utils.sh" "${CMD_PID}" "${CMD_EXIT}" "${CMD_OUT}" "${TRACE_OUT}"

LOG4J_PROPERTIES=${HERE}/log4j-travis.properties
# Maven command to run.
MVN_LOGGING_OPTIONS="-Dlog.dir=${ARTIFACTS_DIR} -Dlog4j.configurationFile=file://$LOG4J_PROPERTIES"
MVN_COMMON_OPTIONS="-Dsurefire.rerunFailingTestsCount=3 -Dflink.forkCount=${1:-1} -Dflink.forkCountTestPackage=${1:-1} $MVN_LOGGING_OPTIONS"
MVN_TEST_MODULES="-pl ${2:-flink-runtime,flink-core}"
MVN_TEST="mvn clean test $MVN_COMMON_OPTIONS $MVN_TEST_MODULES"

if [[ $(basename $PWD) == "tools" ]]; then
  cd ..
fi

MAVEN_TEST_SUCCESS_PATTERN="'\[INFO\] BUILD SUCCESS'"
CHECK_MAVEN_TEST_RESULT_COMMAND="grep ${MAVEN_TEST_SUCCESS_PATTERN} ${CMD_OUT} >/dev/null 2>&1"

run_with_watchdog "$MVN_TEST" 900
exit_code=$?

echo "===========================start check mvn test result==========================="
if [ $exit_code -lt 2 ]; then
  echo "[INFO] Exec command finish. Command: $MVN_TEST "
  checkTestsResult "${CHECK_MAVEN_TEST_RESULT_COMMAND}"
  exit_code=$?
  if [ $exit_code -eq 0 ]; then
    echo "[INFO] All tests passed"
  else
    echo "[ERROR] Some tests failed"
  fi
else
  echo "[ERROR] Exec command failed"
  echo "[ERROR] Command: $MVN_TEST "
fi
echo "===========================finish check mvn test result=========================="

exit $exit_code
