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

package org.apache.flink.streaming.connectors.elasticsearch7;

import org.apache.flink.annotation.Internal;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchApiCallBridge;
import org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase;
import org.apache.flink.streaming.connectors.elasticsearch.RequestIndexer;
import org.apache.flink.streaming.connectors.elasticsearch7.util.ConsulNodesSniffer;
import org.apache.flink.util.Preconditions;

import org.apache.flink.shaded.byted.com.bytedance.commons.consul.Discovery;

import org.apache.http.HttpHost;
import org.elasticsearch.action.bulk.BackoffPolicy;
import org.elasticsearch.action.bulk.BulkItemResponse;
import org.elasticsearch.action.bulk.BulkProcessor;
import org.elasticsearch.client.RequestOptions;
import org.elasticsearch.client.RestClient;
import org.elasticsearch.client.RestClientBuilder;
import org.elasticsearch.client.RestHighLevelClient;
import org.elasticsearch.client.sniff.NodesSniffer;
import org.elasticsearch.client.sniff.SniffOnFailureListener;
import org.elasticsearch.client.sniff.Sniffer;
import org.elasticsearch.common.unit.TimeValue;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicLong;
import java.util.stream.Collectors;

import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.CONFIG_CONNECT_TIMEOUT;
import static org.apache.flink.streaming.connectors.elasticsearch.ElasticsearchSinkBase.CONFIG_SOCKET_TIMEOUT;

/**
 * Implementation of {@link ElasticsearchApiCallBridge} for Elasticsearch 7 and later versions.
 */
@Internal
public class Elasticsearch7ApiCallBridge implements ElasticsearchApiCallBridge<RestHighLevelClient> {

	private static final long serialVersionUID = -5222683870097809633L;

	private static final Logger LOG = LoggerFactory.getLogger(Elasticsearch7ApiCallBridge.class);

	private static final Discovery DISCOVERY = new Discovery();

	private static final String ES_HTTP_SCHEMA = "http";

	private static final String ES_PSM_SCHEMA = "psm";

	/**
	 * User-provided HTTP Host.
	 */
	private final List<HttpHost> httpHosts;

	/**
	 * The factory to configure the rest client.
	 */
	private final RestClientFactory restClientFactory;

	Elasticsearch7ApiCallBridge(List<HttpHost> httpHosts, RestClientFactory restClientFactory) {
		Preconditions.checkArgument(httpHosts != null && !httpHosts.isEmpty());
		this.httpHosts = httpHosts;
		this.restClientFactory = Preconditions.checkNotNull(restClientFactory);
	}

	@Override
	public RestHighLevelClient createClient(Map<String, String> clientConfig) throws IOException {
		if (httpHosts.size() == 1 && ES_PSM_SCHEMA.equals(httpHosts.get(0).getSchemeName())) {
			return createSnifferClientByPsm(httpHosts, clientConfig);
		}

		RestClientBuilder builder =
			RestClient.builder(httpHosts.toArray(new HttpHost[httpHosts.size()]))
			.setRequestConfigCallback(
				requestConfigBuilder -> {
					if (clientConfig.containsKey(CONFIG_CONNECT_TIMEOUT)) {
						requestConfigBuilder
							.setConnectTimeout(Integer.parseInt(clientConfig.get(CONFIG_CONNECT_TIMEOUT)));
					}
					if (clientConfig.containsKey(CONFIG_SOCKET_TIMEOUT)) {
						requestConfigBuilder
							.setSocketTimeout(Integer.parseInt(clientConfig.get(CONFIG_SOCKET_TIMEOUT)));
					}
					return requestConfigBuilder;
				});
		restClientFactory.configureRestClientBuilder(builder);

		RestHighLevelClient rhlClient = new RestHighLevelClient(builder);

		if (LOG.isInfoEnabled()) {
			LOG.info("Pinging Elasticsearch cluster via hosts {} ...", httpHosts);
		}

		if (!rhlClient.ping(RequestOptions.DEFAULT)) {
			throw new RuntimeException("There are no reachable Elasticsearch nodes!");
		}

		if (LOG.isInfoEnabled()) {
			LOG.info("Created Elasticsearch RestHighLevelClient connected to {}", httpHosts.toString());
		}

		return rhlClient;
	}

	private RestHighLevelClient createSnifferClientByPsm(List<HttpHost> httpHosts, Map<String, String> clientConfig) {
		if (httpHosts.size() != 1 || !httpHosts.get(0).getSchemeName().equals(ES_PSM_SCHEMA)) {
			throw new RuntimeException("Create psm sniffer client must have only one host and psm schema!");
		}
		// 截取psm
		String psm = null;
		String cluster = "default";
		String hostName = httpHosts.get(0).getHostName();

		int userInfoIdx = hostName.indexOf('@');
		if (userInfoIdx >= 0) {
			hostName = hostName.substring(userInfoIdx + 1);
		}

		int clusterIdx = hostName.indexOf('$');
		if (clusterIdx > 0) {
			psm = hostName.substring(0, clusterIdx);
			cluster = hostName.substring(clusterIdx + 1);
		}

		// find all nodes by consul
		List<HttpHost> nodes = DISCOVERY.translateOne(psm).stream()
			.map(serviceNode -> new HttpHost(serviceNode.getHost(), serviceNode.getPort(), ES_HTTP_SCHEMA))
			.collect(Collectors.toList());
		if (nodes.size() <= 0) {
			throw new RuntimeException("There are no reachable Elasticsearch nodes!");
		}

		// create client
		RestClientBuilder builder = RestClient.builder(nodes.toArray(new HttpHost[0]));

		builder.setRequestConfigCallback(requestConfigBuilder -> {
			if (clientConfig.containsKey(CONFIG_CONNECT_TIMEOUT)) {
				requestConfigBuilder
					.setConnectTimeout(Integer.parseInt(clientConfig.get(CONFIG_CONNECT_TIMEOUT)));
			}
			if (clientConfig.containsKey(CONFIG_SOCKET_TIMEOUT)) {
				requestConfigBuilder
					.setSocketTimeout(Integer.parseInt(clientConfig.get(CONFIG_SOCKET_TIMEOUT)));
			}
			return requestConfigBuilder;
		});

		// create and config sniffer
		SniffOnFailureListener listener = new SniffOnFailureListener();
		builder.setFailureListener(listener);

		restClientFactory.configureRestClientBuilder(builder);
		RestHighLevelClient rhlClient = new RestHighLevelClient(builder);
		NodesSniffer nodesSniffer = new ConsulNodesSniffer(rhlClient.getLowLevelClient(), psm, cluster, ES_HTTP_SCHEMA);
		Sniffer sniffer = Sniffer.builder(rhlClient.getLowLevelClient()).setNodesSniffer(nodesSniffer).build();
		listener.setSniffer(sniffer);

		LOG.info("Create client successful, psm: {}, hosts: {}", psm, nodes.stream().map(HttpHost::toHostString).collect(Collectors.joining(";")));

		return rhlClient;
	}

	@Override
	public BulkProcessor.Builder createBulkProcessorBuilder(RestHighLevelClient client, BulkProcessor.Listener listener) {
		return BulkProcessor.builder((request, bulkListener) -> client.bulkAsync(request, RequestOptions.DEFAULT, bulkListener), listener);
	}

	@Override
	public Throwable extractFailureCauseFromBulkItemResponse(BulkItemResponse bulkItemResponse) {
		if (!bulkItemResponse.isFailed()) {
			return null;
		} else {
			return bulkItemResponse.getFailure().getCause();
		}
	}

	@Override
	public void configureBulkProcessorBackoff(
		BulkProcessor.Builder builder,
		@Nullable ElasticsearchSinkBase.BulkFlushBackoffPolicy flushBackoffPolicy) {

		BackoffPolicy backoffPolicy;
		if (flushBackoffPolicy != null) {
			switch (flushBackoffPolicy.getBackoffType()) {
				case CONSTANT:
					backoffPolicy = BackoffPolicy.constantBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
					break;
				case EXPONENTIAL:
				default:
					backoffPolicy = BackoffPolicy.exponentialBackoff(
						new TimeValue(flushBackoffPolicy.getDelayMillis()),
						flushBackoffPolicy.getMaxRetryCount());
			}
		} else {
			backoffPolicy = BackoffPolicy.noBackoff();
		}

		builder.setBackoffPolicy(backoffPolicy);
	}

	@Override
	public RequestIndexer createBulkProcessorIndexer(
			BulkProcessor bulkProcessor,
			boolean flushOnCheckpoint,
			AtomicLong numPendingRequestsRef) {
		return new Elasticsearch7BulkProcessorIndexer(
			bulkProcessor,
			flushOnCheckpoint,
			numPendingRequestsRef);
	}
}
