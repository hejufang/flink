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

package org.apache.flink.table.runtime.util;

import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.math.BigInteger;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatIllegalArgumentException;

/**
 * Tests for {@link NumberUtils}.
 */
class NumberUtilsTests {
	@Test
	void convertToByte() {
		assertThat((Byte) NumberUtils.convertNumberToTargetClass(1, Byte.class))
			.isEqualTo(Byte.valueOf("1"));
		assertToNumberOverflow(Byte.MAX_VALUE + 1, Byte.class);
		assertToNumberOverflow(Byte.MIN_VALUE - 1, Byte.class);
	}

	@Test
	void convertToShort() {
		assertThat((Short) NumberUtils.convertNumberToTargetClass(Byte.valueOf("1"), Short.class))
			.isEqualTo(Short.valueOf("1"));
		assertThat((Short) NumberUtils.convertNumberToTargetClass(1, Short.class))
			.isEqualTo(Short.valueOf("1"));
		assertToNumberOverflow(Short.MAX_VALUE + 1, Short.class);
		assertToNumberOverflow(Short.MIN_VALUE - 1, Short.class);
	}

	@Test
	void convertToInteger() {
		assertThat((Integer) NumberUtils.convertNumberToTargetClass(Short.valueOf("1"), Integer.class))
			.isEqualTo(Integer.valueOf("1"));
		assertThat((Integer) NumberUtils.convertNumberToTargetClass(1, Integer.class))
			.isEqualTo(Integer.valueOf("1"));
		assertToNumberOverflow(Integer.MAX_VALUE + 1L, Integer.class);
		assertToNumberOverflow(Integer.MIN_VALUE - 1L, Integer.class);
	}

	@Test
	void convertToLong() {
		assertThat((Long) NumberUtils.convertNumberToTargetClass(Integer.valueOf("1"), Long.class))
			.isEqualTo(Long.valueOf("1"));
		assertThat((Long) NumberUtils.convertNumberToTargetClass(BigInteger.valueOf(1), Long.class))
			.isEqualTo(Long.valueOf("1"));
		assertToNumberOverflow(BigInteger.valueOf(Long.MAX_VALUE).add(BigInteger.ONE), Long.class);
		assertToNumberOverflow(BigInteger.valueOf(Long.MIN_VALUE).subtract(BigInteger.ONE), Long.class);
	}

	@Test
	void convertToBigInteger() {
		assertThat((BigInteger) NumberUtils.convertNumberToTargetClass(Long.MAX_VALUE, BigInteger.class))
			.isEqualTo(BigInteger.valueOf(Long.MAX_VALUE));
		String number = "987459837583750387355346";
		assertThat((BigInteger) NumberUtils.convertNumberToTargetClass(new BigDecimal(number), BigInteger.class))
			.isEqualTo(new BigInteger(number));
	}

	@Test
	void convertToBigDecimal() {
		assertThat((BigDecimal) NumberUtils.convertNumberToTargetClass(1.2222, BigDecimal.class))
			.isEqualTo(BigDecimal.valueOf(12222, 4));
		assertThat((BigDecimal) NumberUtils.convertNumberToTargetClass(BigInteger.valueOf(Long.MAX_VALUE), BigDecimal.class))
			.isEqualTo(BigDecimal.valueOf(Long.MAX_VALUE));
	}

	private void assertToNumberOverflow(Number number, Class<? extends Number> targetClass) {
		String msg = "overflow: from=" + number + ", toClass=" + targetClass;
		assertThatIllegalArgumentException().as(msg).isThrownBy(() ->
			NumberUtils.convertNumberToTargetClass(number, targetClass))
			.withMessageEndingWith("overflow");
	}
}
