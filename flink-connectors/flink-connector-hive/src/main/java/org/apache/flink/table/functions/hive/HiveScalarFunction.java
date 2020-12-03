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

package org.apache.flink.table.functions.hive;

import org.apache.flink.annotation.Internal;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.catalog.hive.client.HiveShim;
import org.apache.flink.table.functions.FunctionContext;
import org.apache.flink.table.functions.ScalarFunction;
import org.apache.flink.table.functions.hive.conversion.HiveInspectors;
import org.apache.flink.table.functions.hive.util.HiveFunctionUtil;
import org.apache.flink.table.runtime.types.TypeInfoDataTypeConverter;
import org.apache.flink.table.types.DataType;

import org.apache.hadoop.hive.common.type.HiveChar;
import org.apache.hadoop.hive.ql.exec.UDF;
import org.apache.hadoop.hive.ql.exec.UDFArgumentException;
import org.apache.hadoop.hive.ql.udf.generic.GenericUDF;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

/**
 * Abstract class to provide more information for Hive {@link UDF} and {@link GenericUDF} functions.
 */
@Internal
public abstract class HiveScalarFunction<UDFType> extends ScalarFunction implements HiveFunction {

	protected final HiveFunctionWrapper<UDFType> hiveFunctionWrapper;
	protected final HiveShim hiveShim;

	protected Object[] constantArguments;
	protected DataType[] argTypes;

	protected transient UDFType function;
	protected transient ObjectInspector returnInspector;

	private transient boolean isArgsSingleArray;

	HiveScalarFunction(HiveFunctionWrapper<UDFType> hiveFunctionWrapper, HiveShim hiveShim) {
		this.hiveFunctionWrapper = hiveFunctionWrapper;
		this.hiveShim = hiveShim;
	}

	@Override
	public void setArgumentTypesAndConstants(Object[] constantArguments, DataType[] argTypes) {
		this.constantArguments = new Object[constantArguments.length];
		this.argTypes = new DataType[argTypes.length];
		for (int i = 0; i < argTypes.length; i++) {
			// we always use string type for string constant arg because that's what hive UDFs expect
			if (constantArguments[i] instanceof String) {
				// the const string arg may not comply with the type, e.g. it can exceed the length parameter
				Object hiveObject = HiveInspectors.getConversion(
						HiveInspectors.toInspectors(hiveShim, new Object[]{constantArguments[i]}, new DataType[]{argTypes[i]})[0],
						argTypes[i].getLogicalType(),
						hiveShim).toHiveObject(constantArguments[i]);
				this.constantArguments[i] = hiveObject instanceof HiveChar ?
						((HiveChar) hiveObject).getStrippedValue() : hiveObject.toString();
				this.argTypes[i] = DataTypes.STRING();
			} else {
				this.constantArguments[i] = constantArguments[i];
				this.argTypes[i] = argTypes[i];
			}
		}
	}

	@Override
	public boolean isDeterministic() {
		try {
			org.apache.hadoop.hive.ql.udf.UDFType udfType =
				hiveFunctionWrapper.getUDFClass()
					.getAnnotation(org.apache.hadoop.hive.ql.udf.UDFType.class);

			return udfType != null && udfType.deterministic() && !udfType.stateful();
		} catch (ClassNotFoundException e) {
			throw new FlinkHiveUDFException(e);
		}
	}

	@Override
	public TypeInformation getResultType(Class[] signature) {
		return TypeInfoDataTypeConverter.fromDataTypeToTypeInfo(
			getHiveResultType(this.constantArguments, this.argTypes));
	}

	@Override
	public void open(FunctionContext context) throws UDFArgumentException {
		openInternal();

		isArgsSingleArray = HiveFunctionUtil.isSingleBoxedArray(argTypes);
	}

	/**
	 * See {@link ScalarFunction#open(FunctionContext)}.
	 */
	protected abstract void openInternal() throws UDFArgumentException;

	public Object eval(Object... args) {

		// When the parameter is (Integer, Array[Double]), Flink calls udf.eval(Integer, Array[Double]), which is not a problem.
		// But when the parameter is an single array, Flink calls udf.eval(Array[Double]),
		// at this point java's var-args will cast Array[Double] to Array[Object] and let it be
		// Object... args, So we need wrap it.
		if (isArgsSingleArray) {
			args = new Object[] {args};
		}

		return evalInternal(args);
	}

	/**
	 * Evaluation logical, args will be wrapped when is a single array.
	 */
	protected abstract Object evalInternal(Object[] args);
}
