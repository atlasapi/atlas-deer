package org.atlasapi.elasticsearch.topic;

import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.elasticsearch.util.EsQueryBuilder;
import org.atlasapi.elasticsearch.util.FiltersBuilder;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.TopicSearcher;

import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.TopicMapping;

import com.google.common.base.Objects;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sherlock_client_shaded.org.elasticsearch.search.sort.SortOrder;

public class SherlockTopicSearcher implements TopicSearcher {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(SherlockTopicSearcher.class);

    private final SherlockSearcher sherlockSearcher;
    private final TopicMapping topicMapping;
    private final EsQueryBuilder builder;

    public SherlockTopicSearcher(SherlockSearcher sherlockSearcher, TopicMapping topicMapping) {
        this.sherlockSearcher = sherlockSearcher;
        this.topicMapping = topicMapping;
        this.builder = EsQueryBuilder.create();
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        SearchQuery searchQuery = SearchQuery.builder()
                .addSearcher(builder.buildQuery(query))
                .addFilter(FiltersBuilder.buildForPublishers(topicMapping.getSource().getKey(), publishers))
                .addSort(topicMapping.getId(), SortOrder.ASC)
                .withIndex(SherlockIndex.TOPICS)
                .withOffset(selection.getOffset())
                .withLimit(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT))
                .build();

        ListenableFuture<IdSearchQueryResponse> response = sherlockSearcher.searchForIds(searchQuery);

        return Futures.transform(
                response,
                (IdSearchQueryResponse input) ->
                        IndexQueryResult.withIds(
                                MoreStreams.stream(input.getIds())
                                        .map(Id::valueOf)
                                        .collect(MoreCollectors.toImmutableList()),
                                input.getTotalResults()
                        )
        );
    }

}
