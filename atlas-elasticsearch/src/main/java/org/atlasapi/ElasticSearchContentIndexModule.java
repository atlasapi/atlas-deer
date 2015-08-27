package org.atlasapi;

import java.util.concurrent.TimeUnit;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.EsContentIndex;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.content.InstrumentedEsContentIndex;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.elasticsearch.client.Client;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.elasticsearch.node.Node;
import org.elasticsearch.node.NodeBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.Service.State;

public class ElasticSearchContentIndexModule implements IndexModule {

    private final Logger log = LoggerFactory.getLogger(ElasticSearchContentIndexModule.class);

    private final EsContentIndex contentIndex;
    private final EsTopicIndex topicIndex;
    private final EsPopularTopicIndex popularTopicsIndex;
    private final EsContentTitleSearcher contentSearcher;

    public ElasticSearchContentIndexModule(String seeds, String clusterName, String indexName, long requestTimeout, ContentResolver resolver, MetricRegistry metrics, ChannelGroupResolver channelGroupResolver) {
        Settings settings = ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .build();
        TransportClient client = new TransportClient(settings);
        for (String host : Splitter.on(",").splitToList(seeds)) {
            client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
        }
        this.contentIndex = new InstrumentedEsContentIndex(client, indexName, requestTimeout, resolver, metrics, channelGroupResolver);
        this.popularTopicsIndex = new EsPopularTopicIndex(client);
        this.topicIndex = new EsTopicIndex(client, EsSchema.TOPICS_INDEX, 60, TimeUnit.SECONDS);
        this.contentSearcher = new EsContentTitleSearcher(client);
    }

    public void init() {
        //Investigate service manager?
        Futures.addCallback(contentIndex.start(), new FutureCallback<State>() {

            @Override
            public void onSuccess(State result) {
                log.info("Started content index module");
            }

            @Override
            public void onFailure(Throwable t) {
                log.info("Failed to start index module:", t);
            }
        });
        Futures.addCallback(topicIndex.start(), new FutureCallback<State>() {
            
            @Override
            public void onSuccess(State result) {
                log.info("Started topic index module");
            }
            
            @Override
            public void onFailure(Throwable t) {
                log.info("Failed to start index module:", t);
            }
        });
    }

    public EsContentIndex contentIndex() {
        return contentIndex;
    }
    
    public EsTopicIndex topicIndex() {
        return topicIndex;
    }

    public EsPopularTopicIndex topicSearcher() {
        return popularTopicsIndex;
    }
    
    public EsContentTitleSearcher contentTitleSearcher() {
        return contentSearcher;
    }
}
