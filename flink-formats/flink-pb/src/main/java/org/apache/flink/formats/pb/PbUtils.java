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

import org.apache.flink.formats.pb.proto.ProtoFile;
import org.apache.flink.formats.pb.proto.ProtoFileUtils;
import org.apache.flink.table.api.ValidationException;
import org.apache.flink.util.Preconditions;

import com.google.protobuf.Descriptors;

/**
 * Pb util class.
 */
public class PbUtils {
	public static Descriptors.Descriptor validateAndGetDescriptor(String className, ProtoFile protoFile) {
		Preconditions.checkState(className != null || protoFile != null,
			"className and protoFile can not be null at the same time.");
		if (className != null) {
			return validateAndGetDescriptorByClassName(className);
		}
		return getDescriptorByPbContent(protoFile);
	}

	public static Descriptors.Descriptor getDescriptorByPbContent(ProtoFile protoFile) {
		return ProtoFileUtils.parseDescriptorFromProtoFile(protoFile);
	}

	public static Descriptors.Descriptor validateAndGetDescriptorByClassName(String className) {
		try {
			Class<?> pbClass = Class.forName(className, true, Thread.currentThread().getContextClassLoader());
			return (Descriptors.Descriptor) pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
		} catch (Exception e) {
			throw new ValidationException(String.format("get %s descriptors error!", className), e);
		}
	}
}
