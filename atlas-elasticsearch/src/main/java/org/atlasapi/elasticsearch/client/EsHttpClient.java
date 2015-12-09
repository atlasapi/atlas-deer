package org.atlasapi.elasticsearch.client;

import java.io.IOException;
import java.util.Arrays;
import java.util.Map;

import org.apache.commons.lang.NotImplementedException;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.Action;
import org.elasticsearch.action.ActionFuture;
import org.elasticsearch.action.ActionListener;
import org.elasticsearch.action.ActionRequest;
import org.elasticsearch.action.ActionRequestBuilder;
import org.elasticsearch.action.ActionResponse;
import org.elasticsearch.action.search.SearchAction;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.action.support.PlainActionFuture;
import org.elasticsearch.client.AdminClient;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.support.AbstractClient;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.threadpool.ThreadPool;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.smile.SmileFactory;
import com.google.common.base.Throwables;

import io.searchbox.client.JestClient;
import io.searchbox.client.JestResultHandler;
import io.searchbox.core.Search;
import io.searchbox.core.SearchResult;

public class EsHttpClient extends AbstractClient {

    private static final Logger log = LoggerFactory.getLogger(EsHttpClient.class);

    private final JestClient jestClient;
    private final ObjectMapper fromSmileMapper;
    private final ObjectMapper toJsonMapper;

    private final ThreadPool threadPool;

    public EsHttpClient(JestClient jestClient) {
        this.jestClient = jestClient;

        this.fromSmileMapper = new ObjectMapper(new SmileFactory());
        this.toJsonMapper = new ObjectMapper();

        // create a threadpool for the ActionFuture::execute()
        this.threadPool = new ThreadPool(ThreadPool.Names.LISTENER);
    }

    public AdminClient admin() {
        return null;
    }

    public Settings settings() {
        return null;
    }

    public ThreadPool threadPool() {
        // threadpool is necessary for org.elasticsearch.action.support.AbstractListenableActionFuture.executeListener
        return threadPool;
    }

    public void close() throws ElasticsearchException {
        jestClient.shutdownClient();
    }

    public <Request extends ActionRequest,
            Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>>
    ActionFuture<Response> execute(
            Action<Request, Response, RequestBuilder, Client> action,
            Request request
    ) {

        final PlainActionFuture<Response> iou = new PlainActionFuture<Response>();

        execute(action, request, new ActionListener<Response>() {

            public void onResponse(Response response) {
                iou.onResponse(response);
            }

            public void onFailure(Throwable e) {
                iou.onFailure(e);
            }
        });

        return iou;

    }

    public <Request extends ActionRequest,
            Response extends ActionResponse,
            RequestBuilder extends ActionRequestBuilder<Request, Response, RequestBuilder, Client>>
    void execute(
            Action<Request, Response, RequestBuilder, Client> action,
            Request request,
            final ActionListener<Response> listener
    ) {

        if (SearchAction.INSTANCE.equals(action)) {

            SearchRequest searchRequest = (SearchRequest) request;
            String json = toJson(searchRequest);

            // types, window, post_filter are all within query
            Search.Builder queryBuilder = new Search.Builder(json)
                    .addIndex(Arrays.asList(searchRequest.indices()))
                    .addType(Arrays.asList(searchRequest.types()));

            jestClient.executeAsync(queryBuilder.build(),

                    new JestResultHandler<SearchResult>() {

                        public void completed(SearchResult result) {
                            Response response = (Response) jestToSearchResponse(result);
                            listener.onResponse(response);
                        }

                        public void failed(Exception e) {
                            // TODO need to map e to Elasticsearch exception
                            listener.onFailure(e);
                        }
                    });
        } else {
            listener.onFailure(new ElasticsearchException("Action not implemented"));
        }

    }

    private <Request extends ActionRequest> String toJson(Request genericRequest) {
        try {
            if (genericRequest instanceof SearchRequest) {
                SearchRequest request = (SearchRequest) genericRequest;
                Map otherValue = fromSmileMapper.readValue(request.source().toBytes(), Map.class);
                return toJsonMapper.writeValueAsString(otherValue);
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        throw new NotImplementedException(
                String.format("Unable to deserialize %s", genericRequest.getClass().toString()));
    }

    private SearchResponse jestToSearchResponse(SearchResult response) {
        // TODO SearchResponse internal only serializes from internaltransport
        //      using StatusToXContent et al need to be very cunning here
        //      plan is to insert FakeInternalSearchResponse as a trojan horse
        log.debug("JSON string {}", response.getJsonString());

        FakeInternalSearchResponse fakeInternalSearchResponse = new FakeInternalSearchResponse(response);

        String scrollId = "meh";

        return new SearchResponse(
                fakeInternalSearchResponse,
                scrollId,
                fakeInternalSearchResponse.getTotalShards(),
                fakeInternalSearchResponse.getSuccessfulShards(),
                fakeInternalSearchResponse.getTookInMillis(),
                fakeInternalSearchResponse.getShardFailures());
    }
}
