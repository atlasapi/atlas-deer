package org.atlasapi;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Service.State;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.content.EsContentTranslator;
import org.atlasapi.content.EsUnequivalentContentIndex;
import org.atlasapi.content.InstrumentedContentIndex;
import org.atlasapi.content.PseudoEquivalentContentIndex;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.TimeUnit;

public class ElasticSearchContentIndexModule implements IndexModule {

    private final Logger log = LoggerFactory.getLogger(ElasticSearchContentIndexModule.class);

    private final EsUnequivalentContentIndex unequivIndex;
    private final ContentIndex equivContentIndex;
    private final EsTopicIndex topicIndex;
    private final EsPopularTopicIndex popularTopicsIndex;
    private final EsContentTitleSearcher contentSearcher;
    private final EsContentTranslator translator;

    public ElasticSearchContentIndexModule(
            String seeds,
            String clusterName,
            String indexName,
            Long requestTimeout,
            ContentResolver resolver,
            MetricRegistry metrics,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivContentIndex
    ) {

        Settings settings = createSettings(clusterName);
        TransportClient esClient = new TransportClient(settings);
        registerSeeds(esClient, seeds);

        unequivIndex = new EsUnequivalentContentIndex(
                esClient,
                indexName,
                resolver,
                channelGroupResolver,
                equivContentIndex,
                requestTimeout.intValue()
        );
        this.translator = new EsContentTranslator(
                indexName,
                esClient,
                equivContentIndex,
                requestTimeout.longValue(),
                resolver
        );

        PseudoEquivalentContentIndex equivalentEsIndex =
                new PseudoEquivalentContentIndex(unequivIndex);

        this.equivContentIndex = new InstrumentedContentIndex(equivalentEsIndex, metrics);
        this.popularTopicsIndex = new EsPopularTopicIndex(esClient);
        this.topicIndex = new EsTopicIndex(esClient, EsSchema.TOPICS_INDEX, 60, TimeUnit.SECONDS);
        this.contentSearcher = new EsContentTitleSearcher(esClient);
    }

    private Settings createSettings(String clusterName) {
        return ImmutableSettings.settingsBuilder()
                    .put("client.transport.sniff", true)
                    .put("cluster.name", clusterName)
                    .build();
    }

    private void registerSeeds(TransportClient client, String seeds) {
        for (String host : Splitter.on(",").splitToList(seeds)) {
            client.addTransportAddress(new InetSocketTransportAddress(host, 9300));
        }
    }

    public void init() {
        try {
            State contentIndexState = unequivIndex.start().get();
            log.info("Started content index in state {}", contentIndexState.toString());
            State topicIndexState = topicIndex.start().get();
            log.info("Started topic index in state {}", topicIndexState.toString());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    public ContentIndex equivContentIndex() {
        return equivContentIndex;
    }

    public ContentIndex unequivContentIndex() {
        return unequivIndex;
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

    public EsContentTranslator translator() {
        return translator;
    }
}
