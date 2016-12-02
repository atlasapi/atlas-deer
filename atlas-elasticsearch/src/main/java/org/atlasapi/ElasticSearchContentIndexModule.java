package org.atlasapi;

import java.util.concurrent.TimeUnit;

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

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Splitter;
import com.google.common.base.Throwables;
import org.elasticsearch.client.transport.TransportClient;
import org.elasticsearch.common.settings.ImmutableSettings;
import org.elasticsearch.common.settings.Settings;
import org.elasticsearch.common.transport.InetSocketTransportAddress;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

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
            int port,
            boolean ssl,
            String clusterName,
            String indexName,
            Long requestTimeout,
            ContentResolver resolver,
            MetricRegistry metrics,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivContentIndex
    ) {
        Settings settings = createSettings(clusterName, ssl);
        TransportClient esClient = new TransportClient(settings);
        registerSeeds(esClient, seeds, port);

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

    private Settings createSettings(String clusterName, boolean ssl) {
        return ImmutableSettings.settingsBuilder()
                .put("client.transport.sniff", true)
                .put("cluster.name", clusterName)
                .put("shield.transport.ssl", ssl ? "true" : "false")
                .build();
    }

    private void registerSeeds(TransportClient client, String seeds, int port) {

        for (String host : Splitter.on(",").splitToList(seeds)) {
            client.addTransportAddress(new InetSocketTransportAddress(host, port));
        }
    }

    public void init() {
        try {
            unequivIndex
                    .startAsync()
                    .awaitRunning();
            log.info("Started content index", unequivIndex.state());

            topicIndex
                    .startAsync()
                    .awaitRunning();
            log.info("Started topic index in state {}", topicIndex.state());
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
