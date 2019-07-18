#!/usr/bin/env bash
mvn clean package -U -DskipTests

mkdir -p output
cp -r flink-dist/target/flink-1.5-byted-SNAPSHOT-bin/flink-1.5-byted-SNAPSHOT/flink_deploy output
