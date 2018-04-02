package org.apache.flink.monitor.utils;

import java.io.IOException;
import java.util.Map;
import org.apache.http.client.methods.CloseableHttpResponse;
import org.apache.http.client.methods.HttpPost;
import org.apache.http.entity.StringEntity;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;

/**
 * Provides http methods.
 * */
public class HttpUtil {
	public static CloseableHttpResponse sendPost(String url, String jsonStr, Map<String, String> headers) throws IOException {
		HttpPost httpPost = new HttpPost(url);
		for (Map.Entry<String, String> entry : headers.entrySet()) {
			httpPost.addHeader(entry.getKey(), entry.getValue());
		}
		StringEntity entity = new StringEntity(jsonStr, "UTF-8");
		httpPost.setEntity(entity);
		HttpClientBuilder httpClientBuilder = HttpClientBuilder.create();
		CloseableHttpClient closeableHttpClient = httpClientBuilder.build();
		CloseableHttpResponse response = closeableHttpClient
				.execute(httpPost);
		return response;
	}
}
