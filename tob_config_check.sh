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

# we only check the parameters which are prefixed with common
checkDomain="common"

function parse_yaml() {
    # used in regex match: match space
    local s='[[:space:]]*'
    # used in regex match: match word
    local w='[a-zA-Z0-9_\.\-]*'
    #field-separator
    local fs=$(echo @|tr @ '\034')
    keys=$(
        # set fs between fields we want to separate
        sed -ne "s|^\($s\):|\1|" \
        -e "s|^\($s\)\($w\)$s:$s[\"']\(.*\)[\"']$s\$|\1$fs\2$fs\3|p" \
        -e "s|^\($s\)\($w\)$s:$s\(.*\)$s\$|\1$fs\2$fs\3|p" $1 |

        # use awk to separate the line by fs
        # $1(field_1) is space, which is used to judge the level of current param
        # $2(field_2) is key of the param
        # $3(field_3) is the value of the param
        awk -F$fs \
        '{
            # get the position by the number of space
            indent = length($1)/2;
            nameLevel[indent] = $2;
            # clear nameLevel
            for (i in nameLevel) {
                if (i > indent ) {
                    delete nameLevel[i]
                }
            }
            # if has value and top-level is common
            if (length($3) > 0 && nameLevel[0] == "'$checkDomain'") {
                key="";
                # do not need top-level
                for (i=1; i<indent; i++) {
                    key=(key)(nameLevel[i])(".")
                }
                key=(key)$2;
                printf("%s\n", key);
            }
       }'
    )
    echo $keys;
}

# load exclude keys and temporary exclude keys
for line in `cat tob_exclude_key.txt`
do
  tobExcludeKeyList+=($line)
done

# parse yaml
innerList=$(parse_yaml flink-dist/src/main/resources/flink-conf.yaml )
tobList=$(parse_yaml flink-dist-tob/src/main/resources/flink-conf.yaml )

# calc
for inner in ${innerList[@]}
do
	excludeFlag=0
	for excludeKey in ${tobExcludeKeyList[@]}
  do
      if [[ "$inner" == "$excludeKey" ]]; then
          excludeFlag=1
          break
      fi
  done;
  if [[ "$excludeFlag" == "1" ]]; then
      continue
  fi

	existFlag=0
  for tob in ${tobList[@]}
  do
      if [[ "$inner" == "$tob" ]]; then
          existFlag=1
          break
      fi
  done;

  # if not found echo result
  if [[ "$existFlag" == "0" ]]; then
      diffArray+=($inner)
  fi
done;


# print result
if [[ "${#diffArray[@]}" > "0" ]]; then
    echo "Found keys that are not included in toB flink-conf."
    echo "If keys are not required, move them to tob_exclude_key.txt file."
    echo "Key Names:"
    for line in ${diffArray[@]}
    do
        echo "-" $line
    done;
    exit 1;
fi
