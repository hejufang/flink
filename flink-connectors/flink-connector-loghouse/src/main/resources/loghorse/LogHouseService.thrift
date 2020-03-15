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

namespace java org.apache.flink.connectors.loghouse.service
namespace py data.inf.loghouse
include "base.thrift"

struct Status {
  1:i32 errorCode;  // 0 means success
  2:string errorMessage;
}

struct ReadListValue {
  1:string hdfsPath;
  2:map<string, string> keyOffsetMap;

}

struct Key {
  1:string partitionKey;
  2:string clusteringKey;
}

struct Record {
  1:list<Key> keys;  // size must be greater than 0
  2:binary value;
  3:i32 ttl;
}

struct Pair {
  1:string clusteringKey;
  2:binary value;
}

struct PutRequest {
  1:string ns;
  2:list<Record> records;
  255:base.Base Base;
}

struct PutResponse {
  1:Status status;
  255:base.BaseResp BaseResp;
}

struct ScanRequest {
  1:string ns;  // map to abase table?
  2:string partitionKey;
  3:string startClusteringKey;
  4:i32 limit;
  5:bool reverse;
  6:optional string endClusteringKey;
  7:optional string regex;
  255:base.Base Base;
}

struct Scan2Request {
  1:string ns;  // map to abase table?
  2:string partitionKey;
  3:string startClusteringKey;
  4:i32 limit;
  5:bool reverse;
  6:string endClusteringKey;
  7:string regex;
  255:base.Base Base;
}

struct ScanResponse {
  1:Status status;
  2:string sessionId;  // empty means more values pending. an UUID
  3:list<Pair> records;
  4:list<Key> errorKeys;
  255:base.BaseResp BaseResp;
}

struct NextRequest {
  1:string sessionId;  // the same as the one in ScanResponse. must send this req to the same rpc server
  2:i32 batchSize;
  255:base.Base Base;
}

struct GetRequest {
  1:string ns;
  2:Key key;
  255:base.Base Base
}

struct GetListRequest {
  1:string ns;
  2:list<ReadListValue> readValueList;
  255:base.Base Base
}

struct GetResponse {
  1:Status status;
  2:binary value;
  255:base.BaseResp BaseResp;
}

struct GetListResponse {
  1:Status status;
  2:map<string, binary> valueMap;
  3:list<string> errorKeys;
  255:base.BaseResp BaseResp;
}

struct ScanKeysRequest {
  1:string ns;
  2:string partitionKey;
  3:bool reverse;
  4:string startClusteringKey;
  255:base.Base Base
}

struct ScanKeysResponse {
  1:Status status;
  2:list<string> keys;
  255:base.BaseResp BaseResp;
}

struct InternalEchoRequest {
}

struct InternalEchoResponse {
}

service LogHouse {
  PutResponse Put(1:PutRequest req);
  GetResponse Get(1:GetRequest req);
  GetListResponse GetList(1:GetListRequest req);
  ScanResponse Scan(1:ScanRequest req);
  ScanResponse Scan2(1:Scan2Request req);
  ScanResponse Next(1:NextRequest req);
  ScanKeysResponse ScanKeys(1:ScanKeysRequest req);

  // internal usage
  InternalEchoResponse InternalEcho(1:InternalEchoRequest req);
}
