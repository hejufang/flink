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

namespace java org.apache.flink.connectors.rpc.thriftexample

// request type
struct GetDimInfosRequest {
    1: optional i64 adId,
    2: optional i64 creativeId,
    3: optional i64 advertiserId,
    4: optional list<string> dimensionList,
    255: required i32 code
}

// return type
struct GetDimInfosResponse {
    1: optional map<string, string> dimensions,
    255: required i32 code
}

struct GetDimInfosBatchRequest{
   1: list<GetDimInfosRequest> requests
}

struct GetDimInfosBatchResponse {
    1: list<GetDimInfosResponse> responses
}

service BatchDimMiddlewareService {
    GetDimInfosBatchResponse GetDimInfosBatch(1: GetDimInfosBatchRequest request);
}
