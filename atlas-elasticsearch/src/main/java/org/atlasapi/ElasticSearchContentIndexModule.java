package org.atlasapi;

import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.EsContentTitleSearcher;
import org.atlasapi.content.EsContentTranslator;
import org.atlasapi.content.EsUnequivalentContentIndex;
import org.atlasapi.content.InstrumentedContentIndex;
import org.atlasapi.content.PseudoEquivalentContentIndex;
import org.atlasapi.topic.EsPopularTopicIndex;
import org.atlasapi.topic.EsTopicIndex;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

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

    public ElasticSearchContentIndexModule(
            ContentSearcher contentSearcher,
            MetricRegistry metrics,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivContentIndex
    ) {
        unequivIndex = EsUnequivalentContentIndex.create(
                contentSearcher,
                IndexMapping.getContent(),
                channelGroupResolver,
                equivContentIndex
        );

        PseudoEquivalentContentIndex equivalentEsIndex =
                PseudoEquivalentContentIndex.create(unequivIndex);

        this.equivContentIndex = InstrumentedContentIndex.create(equivalentEsIndex, metrics);
        this.popularTopicsIndex = new EsPopularTopicIndex(esClient);
        this.topicIndex = new EsTopicIndex(esClient, EsSchema.TOPICS_INDEX, 60, TimeUnit.SECONDS);
        this.contentSearcher = new EsContentTitleSearcher(contentSearcher);
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

}

