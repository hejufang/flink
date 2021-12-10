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

package org.apache.flink.metrics.databus;

import org.apache.flink.yarn.YarnConfigKeys;

import com.bytedance.log4j2.agent.Log4j2AgentAppender;
import com.bytedance.log4j2.utils.LoggerHelper;
import org.apache.logging.log4j.ThreadContext;
import org.apache.logging.log4j.core.Filter;
import org.apache.logging.log4j.core.Layout;
import org.apache.logging.log4j.core.LogEvent;
import org.apache.logging.log4j.core.config.plugins.Plugin;
import org.apache.logging.log4j.core.config.plugins.PluginAttribute;
import org.apache.logging.log4j.core.config.plugins.PluginElement;
import org.apache.logging.log4j.core.config.plugins.PluginFactory;
import org.apache.logging.log4j.core.layout.PatternLayout;

import java.io.Serializable;

/**
 * A simple wrapper for Log4j2AgentAppender.
 */
@Plugin(name = "org.apache.flink.metrics.databus.LogAgentAppender", category = "Core", elementType = "appender", printObject = true)
public class LogAgentAppender extends Log4j2AgentAppender {

	private static final String STREAMLOG_PSM_KEY = "log.streamlog.psm";
	private String psm;

	public LogAgentAppender(String name, Filter filter, PatternLayout patternLayout) {
		super(name, patternLayout, true);
		addFilter(filter);
	}

	@PluginFactory
	public static synchronized LogAgentAppender createAppender(@PluginAttribute("name") String name,
			@PluginElement("Layout") Layout<? extends Serializable> layout,
			@PluginElement("Filter") Filter filter,
			@PluginAttribute(defaultBoolean = true, value = "secMarksEnabled") Boolean secMarksEnabled) {
		if (name == null) {
			LOGGER.error("no name defined in conf for log4j2.");
			return null;
		}
		if (layout == null) {
			layout = PatternLayout.createDefaultLayout();
		}
		LoggerHelper.setSecMarksEnabled(secMarksEnabled == null || secMarksEnabled);
		return new LogAgentAppender(name, filter, (PatternLayout) layout);
	}

	@Override
	public void start() {
		String psmOrDefault = System.getProperty(STREAMLOG_PSM_KEY);
		if (psmOrDefault != null) {
			this.psm = psmOrDefault.equals("default") ? System.getenv(YarnConfigKeys.ENV_LOAD_SERVICE_PSM) : psmOrDefault;
			if (psm != null){
				System.out.println("init LogAgentAppender using psm : " + psm);
			} else {
				System.err.println("psm was not set when init LogAgentAppender");
			}
		}
		ThreadContext.put("PSM", psm);
		super.start();
	}

	@Override
	public void append(LogEvent event) {
		super.append(event);
	}
}

