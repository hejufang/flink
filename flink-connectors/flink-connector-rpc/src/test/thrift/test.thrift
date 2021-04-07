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

namespace java org.apache.flink.connector.rpc.thrift.generated

enum TestType {
    TYPE1 = 0
    TYPE2 = 1
}

struct InnerTestStruct {
    1: bool boolVal = false,
    2: optional i32 intVal = 1,
    3: optional map<string, i64> mapVal,
    4: optional list<i64> listVal
}

struct SimpleStruct {
    1: i64 longVal,
    2: binary biVal
}

struct TestStruct {
    1: string strVal = "",
    2: optional InnerTestStruct innerTestStruct,
    3: optional map<string, SimpleStruct> mapWithStruct,
    4: optional list<SimpleStruct> listWithStruct,
    5: optional map<string, list<i64>> mapWithList,
    6: optional list<map<string, i32>> listWithMap,
    7: optional map<string, list<SimpleStruct>> nested,
    8: optional list<TestType> enumList
}
