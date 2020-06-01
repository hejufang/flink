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

package org.apache.flink.connectors.bytable.util;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;

import java.nio.charset.Charset;
import java.sql.Date;
import java.sql.Time;
import java.sql.Timestamp;

/**
 * A utility class to process data exchange with Bytable type system.
 */
@Internal
public class BytableTypeUtils {

	private static final byte[] EMPTY_BYTES = new byte[]{};

	/**
	 * Deserialize byte array to Java Object with the given type.
	 */
	public static Object deserializeToObject(byte[] value, int typeIdx, Charset stringCharset) {
		String valueTmp = new String(value, stringCharset);
		switch (typeIdx) {
			case 0: // byte[]
				return value;
			case 1: // String
				return valueTmp;
			case 2: // byte
				return value[0];
			case 3: // short
				return Short.parseShort(valueTmp);
			case 4: // int
				return Integer.parseInt(valueTmp);
			case 5: // long
				return Long.parseLong(valueTmp);
			case 6: // Float
				return Float.parseFloat(valueTmp);
			case 7: // Double
				return Double.parseDouble(valueTmp);
			case 8: // Boolean
				return valueTmp.equalsIgnoreCase("true");
			case 9: // sql.Timestamp encoded as long
				return new Timestamp(Long.parseLong(valueTmp));
			case 10: // sql.Date encoded as long
				return new Date(Long.parseLong(valueTmp));
			case 11: // sql.Time encoded as long
				return new Time(Long.parseLong(valueTmp));

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	/**
	 * Serialize the Java Object to byte array with the given type.
	 */
	public static byte[] serializeFromObject(Object value, int typeIdx, Charset stringCharset) {
		switch (typeIdx) {
			case 0: // byte[]
				return (byte[]) value;
			case 1: // external String
				return value == null ? EMPTY_BYTES : ((String) value).getBytes(stringCharset);
			case 2: // byte
				return value == null ? EMPTY_BYTES : new byte[]{(byte) value};
			case 3: //short
			case 4: //int
				return value == null ? EMPTY_BYTES :
					(String.valueOf((int) value)).getBytes(stringCharset);
			case 5: //long
				return value == null ? EMPTY_BYTES :
					(String.valueOf((long) value)).getBytes(stringCharset);
			case 6: //float
				return value == null ? EMPTY_BYTES :
					(String.valueOf((float) value)).getBytes(stringCharset);
			case 7: //double
				return value == null ? EMPTY_BYTES :
					(String.valueOf((double) value)).getBytes(stringCharset);
			case 8: //boolean
				return value == null ? EMPTY_BYTES :
					(String.valueOf((boolean) value)).getBytes(stringCharset);
			case 9: // sql.Timestamp encoded to Long
				return value == null ? EMPTY_BYTES :
					(String.valueOf(((Timestamp) value).getTime())).getBytes(stringCharset);
			case 10: // sql.Date encoded as long
				return value == null ? EMPTY_BYTES :
					(String.valueOf(((Date) value).getTime())).getBytes(stringCharset);
			case 11: // sql.Time encoded as long
				return value == null ? EMPTY_BYTES :
					(String.valueOf(((Time) value).getTime())).getBytes(stringCharset);

			default:
				throw new IllegalArgumentException("unsupported type index:" + typeIdx);
		}
	}

	/**
	 * Gets the type index (type representation in Bytable connector) from the {@link TypeInformation}.
	 */
	public static int getTypeIndex(TypeInformation typeInfo) {
		return getTypeIndex(typeInfo.getTypeClass());
	}

	/**
	 * Checks whether the given Class is a supported type in Bytable connector.
	 */
	public static boolean isSupportedType(Class<?> clazz) {
		return getTypeIndex(clazz) != -1;
	}

	private static int getTypeIndex(Class<?> clazz) {
		if (byte[].class.equals(clazz)) {
			return 0;
		} else if (String.class.equals(clazz)) {
			return 1;
		} else if (Byte.class.equals(clazz)) {
			return 2;
		} else if (Short.class.equals(clazz)) {
			return 3;
		} else if (Integer.class.equals(clazz)) {
			return 4;
		} else if (Long.class.equals(clazz)) {
			return 5;
		} else if (Float.class.equals(clazz)) {
			return 6;
		} else if (Double.class.equals(clazz)) {
			return 7;
		} else if (Boolean.class.equals(clazz)) {
			return 8;
		} else if (Timestamp.class.equals(clazz)) {
			return 9;
		} else if (Date.class.equals(clazz)) {
			return 10;
		} else if (Time.class.equals(clazz)) {
			return 11;
		} else {
			return -1;
		}
	}
}
