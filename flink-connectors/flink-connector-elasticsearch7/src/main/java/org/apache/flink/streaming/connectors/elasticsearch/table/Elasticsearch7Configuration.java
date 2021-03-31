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

package org.apache.flink.streaming.connectors.elasticsearch.table;

import org.apache.flink.annotation.Internal;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.table.api.TableException;
import org.apache.flink.table.api.ValidationException;

import org.apache.http.HttpHost;

import java.net.URI;
import java.net.URISyntaxException;
import java.util.Collections;
import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.CONNECT_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.HOSTS_OPTION;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.SOCKET_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.table.ElasticsearchOptions.URI;
import static org.apache.flink.table.factories.FactoryUtil.PARALLELISM;
import static org.apache.flink.table.factories.FactoryUtil.RATE_LIMIT_NUM;

/**
 * Elasticsearch 7 specific configuration.
 */
@Internal
final class Elasticsearch7Configuration extends ElasticsearchConfiguration {
	Elasticsearch7Configuration(ReadableConfig config, ClassLoader classLoader) {
		super(config, classLoader);
	}

	public List<HttpHost> getHosts() {
		if (config.getOptional(URI).isPresent()) {
			// if we support psm to lookup hosts, we need to fake a schema://host:port
			// to ensure underlying RestHighLevelClient not to complain about it.
			// psm example: psm://byte.es.flink_connector_test.service.lq$data
			String psm = config.get(URI);
			try {
				java.net.URI uri = new URI(psm);
				String host = uri.getAuthority();
				if (uri.getHost() != null) {
					host = uri.getHost();
				}
				return Collections.singletonList(new HttpHost(host, uri.getPort(), uri.getScheme()));
			} catch (URISyntaxException e) {
				throw new TableException(e.getMessage());
			}
		} else {
			return config.get(HOSTS_OPTION).stream()
				.map(Elasticsearch7Configuration::validateAndParseHostsString)
				.collect(Collectors.toList());
		}
	}

	public int getParallelism() {
		return config.get(PARALLELISM);
	}

	public Optional<Integer> getConnectTimeout() {
		return config.getOptional(CONNECT_TIMEOUT).map(t -> (int) t.toMillis());
	}

	public Optional<Integer> getSocketTimeout() {
		return config.getOptional(SOCKET_TIMEOUT).map(t -> (int) t.toMillis());
	}

	public Optional<Long> getRateLimitNum() {
		return config.getOptional(RATE_LIMIT_NUM);
	}

	private static HttpHost validateAndParseHostsString(String host) {
		try {
			HttpHost httpHost = HttpHost.create(host);
			if (httpHost.getPort() < 0) {
				throw new ValidationException(String.format(
					"Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing port.",
					host,
					HOSTS_OPTION.key()));
			}

			if (httpHost.getSchemeName() == null) {
				throw new ValidationException(String.format(
					"Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'. Missing scheme.",
					host,
					HOSTS_OPTION.key()));
			}
			return httpHost;
		} catch (Exception e) {
			throw new ValidationException(String.format(
				"Could not parse host '%s' in option '%s'. It should follow the format 'http://host_name:port'.",
				host,
				HOSTS_OPTION.key()), e);
		}
	}
}
