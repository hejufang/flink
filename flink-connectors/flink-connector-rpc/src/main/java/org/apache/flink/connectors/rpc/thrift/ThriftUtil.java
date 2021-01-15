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

package org.apache.flink.connectors.rpc.thrift;

import org.apache.flink.connectors.rpc.util.JsonUtil;
import org.apache.flink.util.FlinkRuntimeException;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * The util of the thrift.
 */
public class ThriftUtil {

	/**
	 * In batch situation, get the field name of the request list.
	 * @param requestClass The final class to invoke in batch method.
	 * @param requestListClass The accumulate request class which will be add to a list.
	 * @return in final class the corresponding request list field name.
	 */
	public static String getFieldNameOfRequestList(Class<?> requestClass, Class<?> requestListClass) {
		Field[] fields = requestClass.getFields();
		for (Field field : fields) {
			if (field.getType().equals(List.class)) {
				ParameterizedType listInnerType = (ParameterizedType) field.getGenericType();
				Type[] listActualTypeArguments = listInnerType.getActualTypeArguments();
				if (listActualTypeArguments[0].equals(requestListClass)) {
					return field.getName();
				}
			}
		}
		throw new FlinkRuntimeException("Failed find the list field which used for accumulate batch.");
	}

	/**
	 * Get the thrift service method response class.
	 * @param serviceClassName the full name of the service java class which generate by thrift.
	 * @param methodName the method in service which will be execute.
	 * @return the response class of method.
	 */
	public static Class<?> getReturnClassOfMethod(String serviceClassName, String methodName) {
		Class<?> serviceClass;
		try {
			serviceClass = Class.forName(serviceClassName);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(String.format("Can't find class : %s", serviceClassName), e);
		}
		Method[] methods = serviceClass.getDeclaredMethods();
		for (Method method : methods) {
			if (methodName.equals(method.getName())) {
				return method.getReturnType();
			}
		}
		throw new FlinkRuntimeException(String.format("Can't find method : %s in service : %s",
			methodName, serviceClassName));
	}

	/**
	 * Get the thrift service method parameter class. In practice, it always be a request struct.
	 * So we just support one parameter at now.
	 * @param serviceClassName the full name of the service java class which generate by thrift.
	 * @param methodName the method in service which will be execute.
	 * @return the parameter class in method.
	 */
	public static Class<?> getParameterClassOfMethod(String serviceClassName, String methodName) {
		Class<?> parameterClass;
		Class<?> serviceClass;
		try {
			serviceClass = Class.forName(serviceClassName);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(String.format("Can't find class : %s", serviceClassName), e);
		}
		Method[] methods = serviceClass.getDeclaredMethods();
		Parameter[] params = new Parameter[0];
		for (Method method : methods) {
			if (methodName.equals(method.getName())) {
				params = method.getParameters();
				break;
			}
		}
		if (params.length == 1) {
			parameterClass = params[0].getType();
		} else {
			throw new FlinkRuntimeException("Service method should have only one parameter.");
		}
		return parameterClass;
	}

	/**
	 * Mapping nest json to nest batch request struct.
	 */
	private static Object constructInstanceWithString(Class<?> innerRequestClass, String constantValue) throws Exception {
		if (constantValue == null) {
			return null;
		}
		return constructInstanceWithString(innerRequestClass, constantValue, null, null);
	}

	/**
	 * User provide constant value for non list in batch situation which is in json format.
	 * This function to map the json to thrift batch request class.
	 * @param requestClass batch request class.
	 * @param constantValue user provide constant value which is in json format.
	 * @param batchListFieldName in batch request class, there must be have a list field which
	 *                           flink will accumulate request in this list. So when mapping
	 *                           request class it will map this list use requestList.
	 *                           This parameter is the list field name.
	 * @param requestList accumulate request list.
	 * @return the batch request object.
	 * @throws Exception
	 */
	public static Object constructInstanceWithString(
			Class<?> requestClass,
			String constantValue,
			String batchListFieldName,
			List<?> requestList) throws Exception {
		Object result;
		if (requestClass.equals(String.class)) {
			result = constantValue;
		} else if (requestClass.equals(short.class) || requestClass.equals(Short.class)) {
			result = Short.valueOf(constantValue);
		} else if (requestClass.equals(int.class) || requestClass.equals(Integer.class)) {
			result = Integer.valueOf(constantValue);
		} else if (requestClass.equals(long.class) || requestClass.equals(Long.class)) {
			result = Long.valueOf(constantValue);
		} else if (requestClass.equals(boolean.class) || requestClass.equals(Boolean.class)) {
			result = Boolean.valueOf(constantValue);
		} else if (requestClass.equals(double.class) || requestClass.equals(Double.class)) {
			result = Double.valueOf(constantValue);
		} else if (requestClass.equals(byte.class) || requestClass.equals(Byte.class)) {
			result = Byte.valueOf(constantValue);
		} else if (requestClass.equals(List.class)
			|| requestClass.equals(Set.class)
			|| requestClass.equals(Map.class)) {
			result = null; // User can't set list, set, map by constantValue now.
		} else if (requestClass.isEnum()) {
			result = requestClass.getMethod("valueOf", String.class).invoke(null, constantValue);
		} else {
			result = requestClass.newInstance();
			Field[] fields = result.getClass().getFields();
			for (int i = 0; i < fields.length - 1; i++) {
				if (fields[i].getName().equals(batchListFieldName)) {
					fields[i].set(result, requestList);
					continue;
				}
				Class<?> innerClass = fields[i].getType();
				String fieldName = fields[i].getName();
				String innerConstantValue = JsonUtil.getInnerJson(fieldName, constantValue);
				Object innerObject = constructInstanceWithString(innerClass, innerConstantValue);
				fields[i].set(result, innerObject);
			}
		}
		return result;
	}

	public static boolean isPrimitivePackageClass(Class<?> classType) {
		return classType.equals(Boolean.class)
			|| classType.equals(Byte.class)
			|| classType.equals(Short.class)
			|| classType.equals(Integer.class)
			|| classType.equals(Long.class)
			|| classType.equals(Double.class)
			|| classType.equals(String.class)
			|| classType.equals(ByteBuffer.class);
	}
}
