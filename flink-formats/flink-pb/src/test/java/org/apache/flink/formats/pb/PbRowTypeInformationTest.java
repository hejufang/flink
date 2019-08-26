/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.pb;

import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.common.typeinfo.Types;
import org.apache.flink.api.java.typeutils.RowTypeInfo;

import com.google.protobuf.Descriptors;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

/**
 * Test {@link PbRowTypeInformation}.
 */
public class PbRowTypeInformationTest {
	@Test
	public void rowTypeInformationGenerateTest() {
		TypeInformation[] arrayTypes = new TypeInformation[2];
		arrayTypes[0] = Types.STRING;
		arrayTypes[1] = Types.MAP(Types.STRING, Types.INT);
		String[] arrayNames = new String[2];
		arrayNames[0] = "stringTestInMessage";
		arrayNames[1] = "MapTestInMessage";

		TypeInformation[] types = new TypeInformation[9];
		types[0] = Types.INT;
		types[1] = Types.LONG;
		types[2] = Types.STRING;
		types[3] = Types.BOOLEAN;
		types[4] = Types.DOUBLE;
		types[5] = Types.FLOAT;
		types[6] = Types.STRING;
		types[7] = Types.OBJECT_ARRAY(new RowTypeInfo(arrayTypes, arrayNames));
		types[8] = Types.PRIMITIVE_ARRAY(Types.BYTE);
		String[] names = new String[9];
		names[0] = "intTest";
		names[1] = "longTest";
		names[2] = "stringTest";
		names[3] = "boolTest";
		names[4] = "doubleTest";
		names[5] = "floatTest";
		names[6] = "enumTest";
		names[7] = "arrayTest";
		names[8] = "bytesTest";

		Descriptors.Descriptor pbSchema = PbDeserializeTest.TestPbDeserailize.getDescriptor();

		assertEquals(new RowTypeInfo(types, names), PbRowTypeInformation.generateRow(pbSchema));
	}
}
