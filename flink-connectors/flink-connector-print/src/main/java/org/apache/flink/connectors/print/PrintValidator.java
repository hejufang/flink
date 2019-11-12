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

package org.apache.flink.connectors.print;

import org.apache.flink.table.descriptors.ConnectorDescriptorValidator;
import org.apache.flink.table.descriptors.DescriptorProperties;

/**
 * The print validator.
 */
public class PrintValidator extends ConnectorDescriptorValidator {
	static final String PRINT = "print";
	static final String CONNECTOR_PRINT_SAMPLE_RATIO = "connector.print-sample-ratio";
	static final double CONNECTOR_PRINT_SAMPLE_RATIO_DEFAULT = 0.01;
	@Override
	public void validate(DescriptorProperties properties) {
		properties.validateValue(CONNECTOR_TYPE, PRINT, false);
		properties.validateDouble(CONNECTOR_PRINT_SAMPLE_RATIO, true, 0, 1);
	}
}
