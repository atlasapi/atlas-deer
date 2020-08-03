package org.atlasapi.elasticsearch;

import org.atlasapi.SearchModule;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.elasticsearch.content.EsUnequivalentContentSearcher;
import org.atlasapi.elasticsearch.content.InstrumentedContentSearcher;
import org.atlasapi.elasticsearch.content.PseudoEquivalentContentSearcher;
import org.atlasapi.elasticsearch.content.SherlockContentTitleSearcher;
import org.atlasapi.elasticsearch.topic.SherlockPopularTopicSearcher;
import org.atlasapi.elasticsearch.topic.SherlockTopicSearcher;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.client.ElasticSearchProcessor;
import com.metabroadcast.sherlock.common.config.ElasticSearchConfig;
import com.metabroadcast.sherlock.common.health.ElasticsearchProbe;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.mapping.TopicMapping;

import com.codahale.metrics.MetricRegistry;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SherlockSearchModule implements SearchModule {

    private final SherlockSearcher sherlockSearcher;
    private final ContentSearcher equivContentSearcher;
    private final SherlockTopicSearcher topicSearcher;
    private final SherlockPopularTopicSearcher popularTopicsSearcher;
    private final SherlockContentTitleSearcher contentSearcher;
    private final ElasticsearchProbe sherlockProbe;

    public SherlockSearchModule(
            ElasticSearchConfig elasticSearchConfig,
            MetricRegistry metrics,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivContentSearcher
    ) {
        ContentMapping contentMapping = IndexMapping.getContentMapping();
        TopicMapping topicMapping = IndexMapping.getTopicMapping();

        sherlockSearcher = new SherlockSearcher(new ElasticSearchProcessor(
                elasticSearchConfig.getElasticSearchClient()
        ));

        EsUnequivalentContentSearcher unequivSearcher = EsUnequivalentContentSearcher.create(
                sherlockSearcher,
                contentMapping,
                channelGroupResolver,
                equivContentSearcher
        );

        PseudoEquivalentContentSearcher equivalentContentSearcher =
                PseudoEquivalentContentSearcher.create(unequivSearcher);

        this.equivContentSearcher = InstrumentedContentSearcher.create(equivalentContentSearcher, metrics);
        this.popularTopicsSearcher = new SherlockPopularTopicSearcher(sherlockSearcher, contentMapping);
        this.topicSearcher = new SherlockTopicSearcher(sherlockSearcher, topicMapping);
        this.contentSearcher = new SherlockContentTitleSearcher(sherlockSearcher, contentMapping);
        this.sherlockProbe = ElasticsearchProbe.create("sherlock", elasticSearchConfig.getElasticSearchClient());
    }

    public SherlockSearcher getSherlockSearcher() {
        return sherlockSearcher;
    }

    public ContentSearcher equivContentSearcher() {
        return equivContentSearcher;
    }

    public SherlockTopicSearcher topicSearcher() {
        return topicSearcher;
    }

    public SherlockPopularTopicSearcher popularTopicSearcher() {
        return popularTopicsSearcher;
    }

    public SherlockContentTitleSearcher contentTitleSearcher() {
        return contentSearcher;
    }

    public ElasticsearchProbe getSherlockProbe() {
        return sherlockProbe;
    }
}

