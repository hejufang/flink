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
set -e

func() {
    echo "Usage:"
    echo "build_dev_image [-m build_module] [-s src_image] [-d dst_image]"
    echo "Examples:"
    echo "default image repository:hub.byted.org/streaming/flink_dev_test_image"
    echo "  sh build_dev_image -m flink-runtime,flink-kubernetes -s v0.1 -d v0.2"
    echo "support use custom repository"
    echo "  sh build_dev_image -m flink-runtime,flink-kubernetes -s {SrcImageRepo:tag} -d {DstImageRepo:tag}"
    exit -1
}

while getopts "m:s:d:" opts
do
    case $opts in
        m)  build_module=$OPTARG;;
        s)  src_image=$OPTARG;;
        d)  dst_image=$OPTARG;;
        h)  func;;
        ?)  func;;
    esac
done

modules=(`echo $build_module | tr ',' ' '` )

for module in ${modules[@]}
do
  pushd "${module}"
    mvn clean install -DskipTests -T 1C -Pinclude-hadoop -Psql-jars
  popd
done

pushd flink-dist
  mvn clean install -DskipTests -T 1C -Pinclude-hadoop -Psql-jars
popd

rm -rf output
mkdir -p output/deploy/flink-1.11
cp -r flink-dist/target/flink-1.11-byted-SNAPSHOT-bin/flink-1.11-byted-SNAPSHOT/* output/deploy/flink-1.11/

# common jar conflict
bash tools/common-jar-check/common_jar_check.sh "output/deploy/flink-1.11/"

# start build image
if [[ ! $src_image ]] || [[ ! $dst_image ]]
then
  func;
fi

splitStr=":"
if [[ ! $src_image =~ $splitStr ]]
then
  src_image="hub.byted.org/streaming/flink_dev_test_image:$src_image"
fi
echo "src_image : $src_image"

if [[ ! $dst_image =~ $splitStr ]]
then
  dst_image="hub.byted.org/streaming/flink_dev_test_image:$dst_image"
fi
echo "dst_image : $dst_image"

cd output/deploy/
# The destination path of the copy operation needs to be modified according to the src image
cat <<EOF >  dockerfile
FROM $src_image
COPY flink-1.11 /opt/tiger/flink_deploy/
EOF

docker build . -t $dst_image
