package org.atlasapi.elasticsearch;

import com.codahale.metrics.MetricRegistry;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.client.ElasticSearchProcessor;
import com.metabroadcast.sherlock.common.config.ElasticSearchConfig;
import com.metabroadcast.sherlock.common.health.ElasticsearchProbe;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.mapping.TopicMapping;
import org.atlasapi.SearchModule;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.elasticsearch.content.EsEquivalentContentSearcher;
import org.atlasapi.elasticsearch.content.InstrumentedContentSearcher;
import org.atlasapi.elasticsearch.topic.SherlockPopularTopicSearcher;
import org.atlasapi.elasticsearch.topic.SherlockTopicSearcher;
import org.atlasapi.query.v4.search.PseudoEsEquivalentContentSearcher;
import org.atlasapi.util.SecondaryIndex;

public class SherlockSearchModule implements SearchModule {

    private final SherlockSearcher sherlockSearcher;
    private final PseudoEsEquivalentContentSearcher pseudoEquivContentSearcher;
    private final ContentSearcher equivContentSearcher;
    private final SherlockTopicSearcher topicSearcher;
    private final SherlockPopularTopicSearcher popularTopicsSearcher;
    private final ElasticsearchProbe sherlockProbe;

    public SherlockSearchModule(
            ElasticSearchConfig elasticSearchConfig,
            MetricRegistry metrics,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex secondaryIndex
    ) {
        ContentMapping contentMapping = IndexMapping.getContentMapping();
        TopicMapping topicMapping = IndexMapping.getTopicMapping();

        sherlockSearcher = new SherlockSearcher(
                new ElasticSearchProcessor(
                        elasticSearchConfig.getElasticSearchClient()
                )
        );

        pseudoEquivContentSearcher = PseudoEsEquivalentContentSearcher.create(
                sherlockSearcher
        );

        EsEquivalentContentSearcher equivContentSearcher = EsEquivalentContentSearcher.create(
                pseudoEquivContentSearcher,
                contentMapping,
                channelGroupResolver,
                secondaryIndex
        );

        this.equivContentSearcher = InstrumentedContentSearcher.create(equivContentSearcher, metrics);
        this.popularTopicsSearcher = new SherlockPopularTopicSearcher(sherlockSearcher, contentMapping);
        this.topicSearcher = new SherlockTopicSearcher(sherlockSearcher, topicMapping);
        this.sherlockProbe = ElasticsearchProbe.create("sherlock", elasticSearchConfig.getElasticSearchClient());
    }

    public PseudoEsEquivalentContentSearcher getPseudoEquivContentSearcher() {
        return pseudoEquivContentSearcher;
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

    public ElasticsearchProbe getSherlockProbe() {
        return sherlockProbe;
    }
}

