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

package org.apache.flink.table.functions;

import org.apache.flink.annotation.PublicEvolving;
import org.apache.flink.api.common.functions.InvalidTypesException;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.api.java.typeutils.TypeExtractor;
import org.apache.flink.table.api.ValidationException;

/**
 * User defined window function.
 */
@PublicEvolving
public abstract class WindowFunction extends UserDefinedFunction {
	/**
	 * Returns the result type of the evaluation method with a given signature.
	 *
	 * <p>This method needs to be overridden in case Flink's type extraction facilities are not
	 * sufficient to extract the {@link TypeInformation} based on the return type of the evaluation
	 * method. Flink's type extraction facilities can handle basic types or
	 * simple POJOs but might be wrong for more complex, custom, or composite types.
	 *
	 * @param signature signature of the method the return type needs to be determined
	 * @return {@link TypeInformation} of result type or <code>null</code> if Flink should
	 *         determine the type
	 */
	public TypeInformation<?> getResultType(Class<?>[] signature) {
		return null;
	}

	/**
	 * Returns {@link TypeInformation} about the operands of the evaluation method with a given
	 * signature.
	 *
	 * <p>In order to perform operand type inference in SQL (especially when <code>NULL</code> is
	 * used) it might be necessary to determine the parameter {@link TypeInformation} of an
	 * evaluation method. By default Flink's type extraction facilities are used for this but might
	 * be wrong for more complex, custom, or composite types.
	 *
	 * @param signature signature of the method the operand types need to be determined
	 * @return {@link TypeInformation} of operand types
	 */
	public TypeInformation<?>[] getParameterTypes(Class<?>[] signature) {
		final TypeInformation<?>[] types = new TypeInformation<?>[signature.length];
		for (int i = 0; i < signature.length; i++) {
			try {
				types[i] = TypeExtractor.getForClass(signature[i]);
			} catch (InvalidTypesException e) {
				throw new ValidationException(
					"Parameter types of scalar function " + this.getClass().getCanonicalName() +
						" cannot be automatically determined. Please provide type information manually.");
			}
		}
		return types;
	}

	@Override
	public FunctionKind getKind() {
		return FunctionKind.WINDOW;
	}
}
