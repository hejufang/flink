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

package org.apache.flink.connector.rpc.util;

import org.apache.flink.api.java.tuple.Tuple2;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;

/** Util for manipulating objects.*/
public class ObjectUtil {
	public static void updateOrCreateBase(
			Object requestObject,
			Field baseField,
			Field extraField,
			Method extraFieldSetMethod,
			String psm,
			String logID,
			Map<String, String> extraMap) throws Exception {
		Object oriBase = baseField.get(requestObject);
		Class<?> baseClass = baseField.getType();
		if (oriBase == null) {
			oriBase = baseClass.newInstance();
		}
		baseClass.getField("Caller").set(oriBase, psm);
		baseClass.getField("LogID").set(oriBase, logID);
		if (extraField != null) {
			Map<String, String> extra = (Map<String, String>) extraField.get(oriBase);
			if (extra != null) {
				extraMap.forEach((key, value) ->
					extra.merge(key, value, (v1, v2) -> v1));
				extraFieldSetMethod.invoke(oriBase, extra);
			} else {
				extraFieldSetMethod.invoke(oriBase, extraMap);
			}
		}
		baseField.set(requestObject, oriBase);
	}

	public static String generateSetMethodName(String fieldName) {
		//As set method in thrift will return object instead of nothing, we cannot use introspection to
		//get the set method.
		return "set" + Character.toUpperCase(fieldName.charAt(0)) +
			fieldName.substring(1);
	}

	/**
	 * Get class and generic types(if exist) from type.
	 */
	public static Tuple2<Class<?>, Type[]> getClzAndGenericTypes(Type type) {
		Class<?> clz;
		Type[] innerTypes = new Type[]{};
		if (type instanceof Class) {
			clz = (Class<?>) type;
		} else {
			ParameterizedType genericType = (ParameterizedType) type;
			innerTypes = genericType.getActualTypeArguments();
			clz = (Class<?>) genericType.getRawType();
		}
		return new Tuple2<>(clz, innerTypes);
	}
}
