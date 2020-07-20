/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

include "base.thrift"

namespace cpp data.counter.online
namespace go data.counter.online
namespace py data.counter.online
namespace java com.bytedance.flink.test.onlineThrift
cpp_include "<unordered_map>"

enum ClusterType {
    ONLINE = 0,
    OFFLINE = 1,
}

struct Request {
    1: list<string> counter_names,
    2: list<i64> item_ids,
    3: optional list<i32> time_slots,
    4: optional list<i32> daily_time_slots,
    5: optional list<i64> secondary_keys,
    6: optional i32 cache_expire_time,
    7: ClusterType cluster = ClusterType.ONLINE,
    255: optional base.Base Base,
}

struct Counter {
    1: string id,
    2: i64 item_id,
    3: optional i32 time_slot,
    4: string key,
    5: optional i64 count,
    6: optional string name,
    7: optional i32 daily_time_slot,
    8: optional string table,
    255: optional base.Base Base,
}

struct Response {
    1: list<Counter> counters,
    255: optional base.BaseResp BaseResp,
}

struct RefreshRequest {
    1: optional base.Base Base,
}

struct RefreshResponse {
    1: optional base.BaseResp BaseResp,
}

struct WriteRequest {
    1: list<Counter> counters,
    255: optional base.Base Base,
}
struct WriteResponse {
    255: optional base.BaseResp BaseResp,
}

service CounterQueryManager {
    Response GetCount(1:Request req)
    RefreshResponse RefreshCache(1:RefreshRequest req)
    WriteResponse WriteCount(1:WriteRequest req)
    oneway void WriteCountOneWay(1:WriteRequest req)
}
