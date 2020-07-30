package org.atlasapi.elasticsearch.topic;

import java.time.Instant;

import org.atlasapi.entity.Id;
import org.atlasapi.topic.PopularTopicSearcher;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;

import com.google.common.collect.FluentIterable;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.elasticsearch.search.aggregations.AggregationBuilder;
import org.elasticsearch.search.aggregations.AggregationBuilders;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public class SherlockPopularTopicSearcher implements PopularTopicSearcher {

    private static final String AGGREGATION_NAME = "topics";

    private final SherlockSearcher sherlockSearcher;
    private final ContentMapping contentMapping;

    public SherlockPopularTopicSearcher(
            SherlockSearcher sherlockSearcher,
            ContentMapping contentMapping
    ) {
        this.sherlockSearcher = checkNotNull(sherlockSearcher);
        this.contentMapping = checkNotNull(contentMapping);
    }

    @Override
    public ListenableFuture<FluentIterable<Id>> popularTopics(
            Interval interval,
            final Selection selection
    ) {
        RangeParameter<Instant> rangeParameter = RangeParameter.of(
                contentMapping.getBroadcasts().getTransmissionStartTime(),
                interval.getStart().toDate().toInstant(),
                interval.getEnd().toDate().toInstant()
        );

        AggregationBuilder topicIdAggregation = AggregationBuilders
                .nested(AGGREGATION_NAME, contentMapping.getTagsMapping().getFullyQualifiedName())
                .subAggregation(AggregationBuilders.terms(AGGREGATION_NAME)
                        .field(contentMapping.getTags().getId().getFullyQualifiedName())
                        .size(selection.getOffset() + selection.getLimit()));

        SearchQuery searchQuery = SearchQuery.builder()
                .addFilter(rangeParameter)
                .addAggregation(topicIdAggregation)
                .withIndex(SherlockIndex.CONTENT)
                .build();

        ListenableFuture<IdSearchQueryResponse> response = sherlockSearcher.searchForIds(searchQuery);


        return Futures.transform(
                response,
                (IdSearchQueryResponse input) ->
                        FluentIterable.from(input.getIds()).transform(Id::valueOf)
        );
    }
}
