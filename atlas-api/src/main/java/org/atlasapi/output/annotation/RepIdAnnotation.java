package org.atlasapi.output.annotation;

import java.io.IOException;
import java.math.BigInteger;
import java.util.concurrent.ExecutionException;

import org.atlasapi.content.Content;
import org.atlasapi.output.FieldWriter;
import org.atlasapi.output.OutputContext;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.properties.Configurer;
import com.metabroadcast.representative.api.RepresentativeIdResponse;
import com.metabroadcast.representative.client.RepIdClient;
import com.metabroadcast.representative.client.http.HttpExecutor;

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
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
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.atlasapi.application.ApplicationAccessRole.REP_ID_SERVICE;

public class RepIdAnnotation extends OutputAnnotation<Content> {

    private final SubstitutionTableNumberCodec codec = SubstitutionTableNumberCodec.lowerCaseOnly();
    private static final int CONNECTION_TIMEOUT_MS = 10 * 1000;
    private static final int SOCKET_TIMEOUT_MS = 120 * 1000;
    HttpExecutor httpExecutor = HttpExecutor.create(
            getHttpClient(), //probably an overkill
            Configurer.get("representative-id-service.host").get(),
            Integer.parseInt(Configurer.get("representative-id-service.port").get())
    );

    RepIdClient repIdClient = new RepIdClient(httpExecutor);

    CacheLoader<Long, String> loader = new CacheLoader<Long, String>() {
        @Override
        public String load(Long id) {
            return codec.encode(BigInteger.valueOf(id));
        }
    };

    LoadingCache<Long, String> appCache = CacheBuilder.newBuilder().build(loader);

    private static Logger log = LoggerFactory.getLogger(RepIdAnnotation.class);

    @Override
    public void write(Content entity, FieldWriter writer, OutputContext ctxt) throws IOException {


        String repId = null;

        if (!ctxt.getApplication().getAccessRoles().hasRole(REP_ID_SERVICE.getRole())) {
            throw new IllegalArgumentException(
                    "This application does not have access to the Representative ID service. "
                    + "Please ask MB about enabling access to the service.");
        }

        String appId;
        try {
            appId = appCache.get(ctxt.getApplication().getId());
            RepresentativeIdResponse repIdResponse;
            repIdResponse = repIdClient.getRepId(appId, entity.getId().longValue());
            repId = repIdResponse.getRepresentative().getId();
        } catch (ExecutionException e) {
            log.error(
                    "Rep ID could not be fetched because the app Long ID could not be encoded. app:{} id:{} {}",
                    ctxt.getApplication().getId(),
                    codec.encode(BigInteger.valueOf(entity.getId().longValue())),
                    e
            );
        }

        writer.writeField("rep_id", repId);
    }

    private CloseableHttpClient getHttpClient() {
        //REQUEST CONFIG
        RequestConfig requestConfig = RequestConfig.custom()
                .setConnectionRequestTimeout(CONNECTION_TIMEOUT_MS)
                .setConnectTimeout(CONNECTION_TIMEOUT_MS)
                .setSocketTimeout(SOCKET_TIMEOUT_MS)
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
