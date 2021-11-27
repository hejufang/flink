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

package org.apache.flink.util;

import org.junit.Assert;
import org.junit.Test;

/**
 * Tests for {@link TokenExpirationUtils}.
 */
public class TokenExpirationUtilsTest {

	@Test
	public void testTokenExpire() {
		// UnsupportedOperationException
		UnsupportedOperationException ex1 = new UnsupportedOperationException("token expire!");
		Assert.assertTrue(TokenExpirationUtils.isTokenProblemInTraces(ex1));

		// Exception -> UnsupportedOperationException
		Exception ex2 = new Exception("ex2", ex1);
		Assert.assertTrue(TokenExpirationUtils.isTokenProblemInTraces(ex2));

		// SerializedThrowable -> Exception -> UnsupportedOperationException
		SerializedThrowable ex3 = new SerializedThrowable(ex2);
		Assert.assertFalse(ex3.getMessage().contains("token expire!"));
		Assert.assertTrue(TokenExpirationUtils.isTokenProblemInTraces(ex3));

		// SerializedThrowable -> SerializedThrowable -> Exception -> UnsupportedOperationException
		SerializedThrowable ex4 = new SerializedThrowable(ex3);
		Assert.assertFalse(ex4.getMessage().contains("token expire!"));
		Assert.assertTrue(TokenExpirationUtils.isTokenProblemInTraces(ex4));

		// SerializedThrowable -> UnsupportedOperationException
		SerializedThrowable ex5 = new SerializedThrowable(ex1);
		Assert.assertTrue(TokenExpirationUtils.isTokenProblemInTraces(ex5));
	}
}
