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

import org.apache.flink.cep.pattern.pojo.AbstractCondition;
import org.apache.flink.cep.pattern.pojo.AbstractPatternPojo;
import org.apache.flink.cep.pattern.pojo.Condition;
import org.apache.flink.cep.pattern.pojo.PatternPojo;
import org.apache.flink.cep.pattern.v2.ConditionGroup;
import org.apache.flink.cep.pattern.v2.LeafCondition;
import org.apache.flink.util.Preconditions;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Serializable;

/**
 * Checker for the legality of pattern. We may add more rules here.
 */
public class LegalPatternPojoChecker implements Serializable {

	private static final Logger LOG = LoggerFactory.getLogger(LegalPatternPojoChecker.class);

	public static boolean isPatternPojoLegal(AbstractPatternPojo pojo) {
		try {
			Preconditions.checkArgument(pojo.getId() != null);
			if (pojo.getStatus().equals(PatternPojo.StatusType.DISABLED)) {
				// ignore all checks if the pattern is disabled
				return true;
			}

			Preconditions.checkArgument(pojo.getEvents().size() > 0);
			Preconditions.checkArgument(pojo.getBeginEvent() != null);
			Preconditions.checkArgument(pojo.getEvents().stream().allMatch(event -> event.getConditions().size() > 0));
			Preconditions.checkArgument(pojo.getEvents().stream().allMatch(
					event -> event.getConditions().stream().allMatch(LegalPatternPojoChecker::isConditionLegal)));
			return true;
		} catch (Throwable t) {
			LOG.warn("PatternPojo {} is illegal.", pojo, t);
			return false;
		}
	}

	private static boolean isConditionLegal(AbstractCondition abstractCondition) {
		if (abstractCondition instanceof Condition) {
			Condition condition = (Condition) abstractCondition;
			Preconditions.checkArgument(condition.getFilters().isEmpty()
					|| (condition.getFilters().size() > 0 && !condition.getAggregation().equals(Condition.AggregationType.NONE)));
		} else if (abstractCondition instanceof LeafCondition) {
			LeafCondition condition = (LeafCondition) abstractCondition;
			Preconditions.checkArgument(condition.getFilters().isEmpty()
					|| (condition.getFilters().size() > 0 && !condition.getAggregation().equals(LeafCondition.AggregationType.NONE)));
		} else if (abstractCondition instanceof ConditionGroup) {
			ConditionGroup group = (ConditionGroup) abstractCondition;
			Preconditions.checkArgument(group.getConditions().size() > 0 || group.getGroups().size() > 0);
			Preconditions.checkArgument(!(group.getConditions().size() > 0 && group.getGroups().size() > 0));
		}
		return true;
	}
}
