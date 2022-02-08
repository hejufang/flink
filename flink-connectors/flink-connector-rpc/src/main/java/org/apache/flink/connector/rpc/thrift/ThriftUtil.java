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

package org.apache.flink.connector.rpc.thrift;

import org.apache.flink.util.FlinkRuntimeException;

import org.apache.thrift.TServiceClient;

import java.lang.reflect.Field;
import java.lang.reflect.Method;
import java.lang.reflect.Parameter;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.ByteBuffer;
import java.util.List;

/**
 * The utils of the thrift.
 */
public class ThriftUtil {

	public static final String CLIENT_CLASS_SUFFIX = "$Client";

	/**
	 * Get the thrift service method response class.
	 * @param serviceClientClass the service client class which generate by thrift.
	 * @param methodName the method in service which will be execute.
	 * @return the response class of method.
	 */
	public static Class<?> getReturnClassOfMethod(Class<?> serviceClientClass, String methodName) {
		Method[] methods = serviceClientClass.getDeclaredMethods();
		for (Method method : methods) {
			if (methodName.equals(method.getName())) {
				return method.getReturnType();
			}
		}
		throw new FlinkRuntimeException(String.format("Can't find method : %s in service : %s",
			methodName, serviceClientClass.getName()));
	}

	/**
	 * Get the thrift service method parameter class. In practice, it always be a request struct.
	 * So we just support one parameter at now.
	 * @param serviceClientClass the service client class which generate by thrift.
	 * @param methodName the method in service which will be execute.
	 * @return the parameter class in method.
	 */
	public static Class<?> getParameterClassOfMethod(Class<?> serviceClientClass, String methodName) {
		Class<?> parameterClass;
		Method[] methods = serviceClientClass.getDeclaredMethods();
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

	@SuppressWarnings("unchecked")
	public static Class<? extends TServiceClient> getThriftClientClass(String thriftServiceClassName) {
		try {
			return (Class<? extends TServiceClient>) Class.forName(thriftServiceClassName + CLIENT_CLASS_SUFFIX);
		} catch (ClassNotFoundException e) {
			throw new FlinkRuntimeException(e);
		}
	}

	public static Class<?> getComponentClassOfListField(Class<?> outerClass, String fieldName) {
		try {
			Field field = outerClass.getField(fieldName);
			if (field.getType().equals(List.class)) {
				ParameterizedType listInnerType = (ParameterizedType) field.getGenericType();
				Type[] listActualTypeArguments = listInnerType.getActualTypeArguments();
				return Class.forName(listActualTypeArguments[0].getTypeName());
			} else {
				throw new IllegalArgumentException("The field " + fieldName + " must be of List type.");
			}
		} catch (Exception ex) {
			throw new FlinkRuntimeException(ex);
		}
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
