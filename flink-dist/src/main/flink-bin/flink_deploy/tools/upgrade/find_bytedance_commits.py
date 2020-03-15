#coding=utf-8
import csv

###################################################################
## 使用方法, 以 flink-1.9 升级 flink-1.10 为例:
##    1. git log flink-1.10..flink-1.9 > commits.txt (找出 flink-1.9 有但是 flink-1.10 的 commits)
##    2. 将 commits.txt 移至本文件同级目录下
##    3. python find_bytedance_commits.py
##    4. 获取同级目录下 migrate_commits.csv
###################################################################

bytedanceCommits = []
num = 0

with open("commits.txt", "rb") as f:
    lines = f.readlines()
    commit = ""
    keep = False
    for line in lines:
        if line.startswith("commit "):
            if keep:
                bytedanceCommits.append(commit)

            keep = False
            commit = ""
        elif "bytedance.com" in line:
            keep = True
            num += 1

        if len(line) > 0:
            commit += line


with open("./migrate_commits.csv", "wb") as f:
    writer = csv.writer(f)
    num = len(bytedanceCommits) + 1
    for commit in bytedanceCommits:
        num-=1
        row = []
        row.append(num)

        commitMesg = ""

        lines = commit.split("\n")
        for line in lines:
            if "commit " in line:
                row.append(line.replace("commit ", "").strip())
            elif "Author:" in line:
                row.append(line.replace("Author:", "").strip())
            elif "Date:" in line:
                row.append(line.replace("Date:", "").strip())
            elif len(line.strip()) > 0:
                commitMesg += line.strip()
                commitMesg += "  "
        row.append(commitMesg)
        writer.writerow(row)


