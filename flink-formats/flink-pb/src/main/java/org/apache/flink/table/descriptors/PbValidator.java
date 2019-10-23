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

package org.apache.flink.table.descriptors;

import org.apache.flink.formats.pb.PbConstant;
import org.apache.flink.table.api.ValidationException;

import com.google.protobuf.Descriptors;

/**
 * Validator for PB.
 */
public class PbValidator extends FormatDescriptorValidator {
	@Override
	public void validate(DescriptorProperties properties) {
		super.validate(properties);
		properties.validateInt(PbConstant.FORMAT_PB_SKIP_BYTES, true, 0);
	}

	public static Descriptors.Descriptor validateAndReturnDescriptor(String className) {
		try {
			Class<?> pbClass = Class.forName(className);
			return (Descriptors.Descriptor) pbClass.getMethod(PbConstant.PB_METHOD_GET_DESCRIPTOR).invoke(null);
		} catch (Exception y) {
			throw new ValidationException(String.format("get %s descriptors error!", className), y);
		}
	}
}
