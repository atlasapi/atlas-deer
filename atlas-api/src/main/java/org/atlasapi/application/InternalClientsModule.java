package org.atlasapi.application;

import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.representative.client.RepIdClient;
import com.metabroadcast.representative.client.http.HttpExecutor;

import org.apache.http.HeaderElement;
import org.apache.http.HeaderElementIterator;
import org.apache.http.HttpResponse;
import org.apache.http.client.ServiceUnavailableRetryStrategy;
import org.apache.http.client.config.RequestConfig;
import org.apache.http.conn.ConnectionKeepAliveStrategy;
import org.apache.http.impl.client.CloseableHttpClient;
import org.apache.http.impl.client.HttpClientBuilder;
import org.apache.http.impl.conn.PoolingHttpClientConnectionManager;
import org.apache.http.message.BasicHeaderElementIterator;
import org.apache.http.protocol.HTTP;
import org.apache.http.protocol.HttpContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import static com.google.common.base.Preconditions.checkNotNull;

@Configuration
public class InternalClientsModule {

    @Bean
    public RepIdClient repIdClient() {
        String host = checkNotNull(Configurer.get("representative-id-service.host").get());
        Integer port = Integer.parseInt(checkNotNull(Configurer.get("representative-id-service.port").get()));

        HttpExecutor httpExecutor = HttpExecutor.create(getHttpClient(),host,port);
        return new RepIdClient(httpExecutor);
    }

    private static CloseableHttpClient getHttpClient() {
        int connectionTimeoutMs = 10 * 1000;
        int sockerTimeoutMs = 120 * 1000;
        //REQUEST CONFIG
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(connectionTimeoutMs)
                .setConnectTimeout(connectionTimeoutMs)
                .setSocketTimeout(sockerTimeoutMs)
                .build();
        //RETRY STRATEGY
        ServiceUnavailableRetryStrategy serviceUnavailableRetryStrategy = new ServiceUnavailableRetryStrategy() {

            @Override
            public boolean retryRequest(HttpResponse response,
                    int executionCount, HttpContext context) {
                int statusCode = response.getStatusLine().getStatusCode();
                return statusCode >= 500 && executionCount < 5;
            }

            @Override
            public long getRetryInterval() {
                return 1000L;
            }
        };
        //CONNECTION MANAGER
        PoolingHttpClientConnectionManager connManager
                = new PoolingHttpClientConnectionManager();
        connManager.setMaxTotal(500); //open connections allowed
        connManager.setDefaultMaxPerRoute(500); //concurrent connections allowed to the same host
        //KEEP ALIVE STRATEGY
        ConnectionKeepAliveStrategy dieQuickly = (response, context) -> {
            HeaderElementIterator it = new BasicHeaderElementIterator(
                    response.headerIterator(HTTP.CONN_KEEP_ALIVE)
            );
            while (it.hasNext()) {
                HeaderElement he = it.nextElement();
                String param = he.getName();
                String value = he.getValue();
                if (value != null && param.equalsIgnoreCase("timeout")) {
                    return Long.parseLong(value) * 1000;
                }
            }
            return 5 * 1000; //default timeout 5 seconds
        };

        return HttpClientBuilder.create()
                .setDefaultRequestConfig(requestConfig)
                .setServiceUnavailableRetryStrategy(serviceUnavailableRetryStrategy)
                .setConnectionManager(connManager)
                .setKeepAliveStrategy(dieQuickly).build();
    }
}
