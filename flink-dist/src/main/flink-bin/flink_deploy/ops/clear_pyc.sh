#!/usr/bin/env bash

# crontab is: */10 * * * * lh -s inf.jstorm|sexec -H - -f /opt/tiger/flink_deploy/ops/clear_pyc.sh -p 200

clear_repos_pyc() {
    cd "$1"
    for f in `find .|grep \.pyc$`;
    do
        rm -f $f
    done
}

clear_repos_pyc /opt/tiger/ss_lib
clear_repos_pyc /opt/tiger/pyutil
