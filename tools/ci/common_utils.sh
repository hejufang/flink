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

CMD_PID="$1"
CMD_EXIT="$2"
CMD_OUT="$3"

TRACE_OUT="$4"

# Number of seconds to sleep before checking the output again
SLEEP_TIME=20

# =============================================================================
# TIME FUNCTION
# =============================================================================
function mod_time() {
  if [[ $(uname) == 'Darwin' ]]; then
    eval $(stat -s $CMD_OUT)
    echo $st_mtime
  else
    echo $(stat -c "%Y" $CMD_OUT)
  fi
}

function the_time() {
  echo $(date +%s)
}

# =============================================================================
# STACKTRACE
# =============================================================================

function print_stacktraces() {
  echo "[INFO] =============================================================================="
  echo "[INFO] The following Java processes are running (JPS)"
  echo "[INFO] =============================================================================="

  jps

  local pids=($(jps | awk '{print $1}'))

  for pid in "${pids[@]}"; do
    echo "[INFO] =============================================================================="
    echo "[INFO] Printing stack trace of Java process ${pid}"
    echo "[INFO] =============================================================================="

    jstack $pid
  done
}

# =============================================================================
# WATCHDOG
# =============================================================================

function watchdog() {
  echo $CMD_OUT
  touch $CMD_OUT
  MAX_NO_OUTPUT=$1
  echo "[INFO] A watchdog that monitors no output process for ${MAX_NO_OUTPUT} seconds"
  while true; do
    sleep $SLEEP_TIME

    time_diff=$(($(the_time) - $(mod_time)))

    if [ $time_diff -ge $MAX_NO_OUTPUT ]; then
      echo "[ERROR] =============================================================================="
      echo "[ERROR] Process produced no output for ${MAX_NO_OUTPUT} seconds."
      echo "[ERROR] =============================================================================="

      print_stacktraces | tee $TRACE_OUT

      # Kill $CMD and all descendants
      pkill -P $(<$CMD_PID)

      exit 1
    fi
  done
}

function run_with_watchdog() {
  local cmd="$1"

  watchdog ${2:-900} &

  WD_PID=$!
  echo "[INFO] STARTED watchdog (${WD_PID})."
  echo "[INFO] RUNNING '${cmd}'."

  # Run $CMD and pipe output to $CMD_OUT for the watchdog. The PID is written to $CMD_PID to
  # allow the watchdog to kill $CMD if it is not producing any output anymore. $CMD_EXIT contains
  # the exit code. This is important for Travis' build life-cycle (success/failure).
  ( $cmd & PID=$! ; echo $PID >&3 ; wait $PID ; echo $? >&4 ) 3>$CMD_PID 4>$CMD_EXIT | tee $CMD_OUT

  EXIT_CODE=$(<$CMD_EXIT)

  echo "[INFO] Exited with EXIT CODE: ${EXIT_CODE}."

  # Make sure to kill the watchdog in any case after $CMD has completed
  echo "[INFO] Trying to KILL watchdog (${WD_PID})."
  (kill -9 $WD_PID 2>&1) >/dev/null

  rm $CMD_PID
  rm $CMD_EXIT
  return $EXIT_CODE
}

# =============================================================================
# RESULT
# =============================================================================

checkTestsResult() {
  echo "[INFO] Check command result use command: $1"
  eval $1
  exit_code=$?
  if [ $exit_code -ne 0 ]; then
    echo "[ERROR] Command returned failed."
  else
    echo "[INFO] Command returned success."
  fi
  return ${exit_code}
}
