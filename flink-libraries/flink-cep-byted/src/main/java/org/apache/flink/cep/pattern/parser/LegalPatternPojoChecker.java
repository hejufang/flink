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

package org.apache.flink.cep.pattern.parser;

import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Checker for the legality of pattern. We may add more rules here.
 */
public class LegalPatternPojoChecker implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(LegalPatternPojoChecker.class);

	public static boolean isPatternPojoLegal(PatternPojo pojo) {
		try {
			Preconditions.checkArgument(pojo.getEvents().size() > 0);
			Preconditions.checkArgument(pojo.getBeginEvent() != null);
			Preconditions.checkArgument(pojo.getEvents().stream().allMatch(event -> event.getConditions().size() > 0));
			return true;
		} catch (Throwable t) {
			LOG.warn("PatternPojo {} is illegal.", pojo, t);
			return false;
		}
	}
}
