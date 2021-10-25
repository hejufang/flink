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

package org.apache.flink.runtime.shuffle;

import org.apache.flink.configuration.Configuration;

import com.bytedance.css.common.CssConf;
import org.junit.Assert;
import org.junit.Test;

import java.util.Map;

/**
 * Tests for {@link CloudShuffleOptions}.
 */
public class CloudShuffleOptionsTest {

	@Test
	public void testFromConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setString(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_ADDRESS, "address");
		configuration.setString(CloudShuffleOptions.CLOUD_SHUFFLE_SERVICE_PORT, "port");
		configuration.setString("flink.cloud-shuffle-service.heartbeat-interval", "10s");

		CssConf cssConf = CloudShuffleOptions.fromConfiguration(configuration);
		Assert.assertEquals("css://address:port", cssConf.get("css.master.address"));
		Assert.assertEquals("10s", cssConf.get("heartbeat-interval"));
	}

	@Test
	public void testPropertiesFromConfiguration() {
		Configuration configuration = new Configuration();
		configuration.setString("flink.cloud-shuffle-service.heartbeat-interval", "10s");
		configuration.setString("flink.cloud-shuffle-service.heartbeat-timeout", "1min");

		Map<String, String> properties = CloudShuffleOptions.propertiesFromConfiguration(configuration);
		Assert.assertEquals(2, properties.size());
		Assert.assertEquals("10s", properties.get("heartbeat-interval"));
		Assert.assertEquals("1min", properties.get("heartbeat-timeout"));
	}
}
