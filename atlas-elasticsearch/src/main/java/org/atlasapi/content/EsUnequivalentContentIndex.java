package org.atlasapi.content;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;
import org.atlasapi.channel.ChannelGroup;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.channel.ChannelNumbering;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.topic.EsTopic;
import org.atlasapi.util.ElasticsearchIndexCreator;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.AndFilterBuilder;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.NestedFilterBuilder;
import org.elasticsearch.index.query.OrFilterBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.RangeFilterBuilder;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsUnequivalentContentIndex extends AbstractIdleService implements ContentIndex {

    private static final Function<SearchHit, Id> HIT_TO_CANONICAL_ID = hit -> {
        if (hit == null || hit.field(EsContent.CANONICAL_ID) == null) {
            return null;
        }
        Long id = hit.field(EsContent.CANONICAL_ID).<Number>value().longValue();
        return Id.valueOf(id);
    };
    public static final Function<SearchHit, Id> SEARCH_HIT_ID_FUNCTION = hit -> Id.valueOf(hit.field(EsContent.ID).<Number>value().longValue());
    public static final Function<SearchHit, Id> HIT_TO_ID = SEARCH_HIT_ID_FUNCTION;

    private final Logger log = LoggerFactory.getLogger(EsUnequivalentContentIndex.class);

    private static final int DEFAULT_LIMIT = 50;

    private final Client esClient;
    private final String index;
    private final ChannelGroupResolver channelGroupResolver;

    private final EsQueryBuilder queryBuilderFactory = new EsQueryBuilder();

    private final SecondaryIndex equivIdIndex;
    private final EsUnequivalentContentIndexer indexer;

    public EsUnequivalentContentIndex(Client esClient, String indexName, ContentResolver resolver,
            ChannelGroupResolver channelGroupResolver, SecondaryIndex equivIdIndex, Integer requestTimeout) {
        this.esClient = checkNotNull(esClient);
        this.index = checkNotNull(indexName);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.equivIdIndex = checkNotNull(equivIdIndex);
        EsContentTranslator translator = new EsContentTranslator(
                indexName,
                esClient,
                equivIdIndex,
                requestTimeout.longValue(),
                resolver
        );
        this.indexer = new EsUnequivalentContentIndexer(
                esClient,
                resolver,
                indexName,
                requestTimeout,
                channelGroupResolver,
                equivIdIndex,
                translator
        );
    }

    @Override
    protected void startUp() throws IOException {
        if (ElasticsearchIndexCreator.createContentIndex(esClient, index)) {
            ElasticsearchIndexCreator.putTypeMapping(esClient, index);
        }
        log.info("Staring ElasticsearchUnequivalentContentIndex");
    }

    @Override
    protected void shutDown() throws Exception {
        log.info("Shutting down ElasticsearchUnequivalentContentIndex");
    }

    @Override
    public void index(Content content) throws IndexException {
        indexer.index(content);
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(AttributeQuerySet query,
            Iterable<Publisher> publishers, Selection selection, Optional<IndexQueryParams> queryParams) {
        SettableFuture<SearchResponse> response = SettableFuture.create();

        QueryBuilder queryBuilder = this.queryBuilderFactory.buildQuery(query);

        /* matchAllFilter as a bool filter with less than 1 clause is invalid */
        BoolFilterBuilder filterBuilder = FilterBuilders.boolFilter()
                .must(FilterBuilders.matchAllFilter());

        SearchRequestBuilder reqBuilder = esClient
                .prepareSearch(index)
                .addSort(SortBuilders.scoreSort().order(SortOrder.DESC))
                .addSort(EsContent.ID, SortOrder.ASC)
                .setTypes(EsContent.CHILD_ITEM, EsContent.TOP_LEVEL_CONTAINER, EsContent.TOP_LEVEL_ITEM)
                .addField(EsContent.CANONICAL_ID)
                .addField(EsContent.ID)
                .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT));

        if (queryParams.isPresent()) {

            if (queryParams.get().getFuzzyQueryParams().isPresent()) {
                queryBuilder = addTitleQuery(queryParams, queryBuilder);
            }

            if (queryParams.get().getBrandId().isPresent()) {
                filterBuilder.must(getBrandIdFilter(queryParams.get().getBrandId().get()));
            }

            if (queryParams.get().getSeriesId().isPresent()) {
                filterBuilder.must(getSeriesIdFilter(queryParams.get().getSeriesId().get()));
            }

            if (queryParams.get().getOrdering().isPresent()) {
                addSortOrder(queryParams, reqBuilder);
            }

            if (queryParams.get().getTopicFilterIds().isPresent()) {
                filterBuilder.must(getTopicIdFilters(queryParams.get().getTopicFilterIds().get()));
            }

            if (queryParams.get().getActionableFilterParams().isPresent()) {
                filterBuilder.must(getActionableFilter(queryParams.get().getActionableFilterParams().get()));
            }

            /* Temporarily disabled due to not working correctly
            if (queryParams.get().getRegionId().isPresent()) {
                queryBuilderFactory = addRegionFilter(queryParams, queryBuilderFactory);
            }
            */

        }

        if (queryParams.isPresent() && queryParams.get().getBroadcastWeighting().isPresent()) {
            queryBuilder = BroadcastQueryBuilder.build(
                    queryBuilder,
                    queryParams.get().getBroadcastWeighting().get()
            );
        } else {
            queryBuilder = BroadcastQueryBuilder.build(queryBuilder, 5f);
        }

        log.debug(queryBuilder.toString());
        reqBuilder.setQuery(QueryBuilders.filteredQuery(queryBuilder, filterBuilder));
        reqBuilder.execute(FutureSettingActionListener.setting(response));

        /* TODO
         * if selection.offset + selection.limit < totalHits
         * then we have more: return for use with response.
         */
        return Futures.transform(response, (SearchResponse input) -> {
            return new IndexQueryResult(
                    FluentIterable.from(input.getHits()).transform(HIT_TO_ID),
                    FluentIterable.from(input.getHits()).transform(HIT_TO_CANONICAL_ID),
                    input.getHits().getTotalHits()
            );
        });
    }

    private FilterBuilder getSeriesIdFilter(Id id) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.SERIES, ids);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    private FilterBuilder getActionableFilter(Map<String, String> actionableParams) {
        OrFilterBuilder orFilterBuilder = FilterBuilders.orFilter();
        if (actionableParams.get("location.available") != null) {
            orFilterBuilder.add(getAvailabilityFilter());
        }
        DateTime broadcastTimeGreaterThan = actionableParams.get("broadcast.time.gt") == null ? null
                : DateTime.parse(actionableParams.get("broadcast.time.gt"));
        DateTime broadcastTimeLessThan = actionableParams.get("broadcast.time.lt") == null ? null
                : DateTime.parse(actionableParams.get("broadcast.time.lt"));
        if (broadcastTimeGreaterThan != null || broadcastTimeLessThan != null) {
            orFilterBuilder.add(
                    broadcastRangeFilterFrom(broadcastTimeGreaterThan, broadcastTimeLessThan)
            );
        }
        return orFilterBuilder;
    }

    private FilterBuilder broadcastRangeFilterFrom(DateTime broadcastTimeGreaterThan, DateTime broadcastTimeLessThan) {
        RangeFilterBuilder parentFilter = FilterBuilders.rangeFilter("transmissionTimeInMillis");
        if (broadcastTimeGreaterThan != null) {
            parentFilter.gte(broadcastTimeGreaterThan.getMillis());
        }
        if (broadcastTimeLessThan != null) {
            parentFilter.lte(broadcastTimeLessThan.getMillis());
        }
        return FilterBuilders.orFilter(
                parentFilter,
                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, parentFilter)
        );
    }

    private FilterBuilder getBrandIdFilter(Id id) {
        try {
            ImmutableSet<Long> ids = Futures.get(equivIdIndex.reverseLookup(id), IOException.class);
            return FilterBuilders.termsFilter(EsContent.BRAND, ids);
        } catch (IOException ioe) {
            throw Throwables.propagate(ioe);
        }
    }

    private FilterBuilder getAvailabilityFilter() {
        NestedFilterBuilder rangeFilter = FilterBuilders.nestedFilter(
                EsContent.LOCATIONS,
                FilterBuilders.andFilter(
                        FilterBuilders.rangeFilter(EsLocation.AVAILABILITY_TIME)
                                .lte(DateTime.now().toString()),
                        FilterBuilders.rangeFilter(EsLocation.AVAILABILITY_END_TIME)
                                .gte(DateTime.now().toString()))
        );
        return FilterBuilders.orFilter(
                rangeFilter,
                FilterBuilders.hasChildFilter(EsContent.CHILD_ITEM, rangeFilter)
        );
    }

    public FilterBuilder getTopicIdFilters(List<List<InclusionExclusionId>> topicIdSets) {
        ImmutableList.Builder<FilterBuilder> topicIdFilters = ImmutableList.builder();
        for (List<InclusionExclusionId> idSet : topicIdSets) {
            BoolFilterBuilder filterForThisSet = FilterBuilders.boolFilter();
            for (InclusionExclusionId id : idSet) {
                addFilterForTopicId(filterForThisSet, id);
            }
            topicIdFilters.add(filterForThisSet);
        }
        AndFilterBuilder andFilterBuilder = FilterBuilders.andFilter();
        topicIdFilters.build().forEach(andFilterBuilder::add);
        return andFilterBuilder;    
    }

    private void addFilterForTopicId(BoolFilterBuilder filterBuilder, InclusionExclusionId id) {
        NestedFilterBuilder filterForId = FilterBuilders.nestedFilter(
                EsContent.TOPICS + "." + EsTopic.TYPE_NAME,
                FilterBuilders.termFilter(
                        EsContent.TOPICS + "." + EsTopic.TYPE_NAME + "." + EsContent.ID,
                        id.getId()
                )
        );
        if (id.isExcluded()) {
            filterBuilder.mustNot(filterForId);
        } else {
            filterBuilder.must(filterForId);
        }
    }

    private void addSortOrder(Optional<IndexQueryParams> queryParams, SearchRequestBuilder reqBuilder) {
        QueryOrdering order = queryParams.get().getOrdering().get();
        if ("relevance".equalsIgnoreCase(order.getPath())) {
            reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        } else {
            reqBuilder.addSort(
                    SortBuilders
                            .fieldSort(translateOrderField(order.getPath()))
                            .order(order.isAscending() ? SortOrder.ASC : SortOrder.DESC)
            );
        }
    }

    private String translateOrderField(String orderField) {
        if ("title".equalsIgnoreCase(orderField)) {
            return "flattenedTitle";
        }
        return orderField;
    }


    private QueryBuilder addTitleQuery(Optional<IndexQueryParams> queryParams, QueryBuilder queryBuilder) {
        FuzzyQueryParams searchParams = queryParams.get().getFuzzyQueryParams().get();
        queryBuilder = QueryBuilders.boolQuery()
                .must(queryBuilder)
                .must(TitleQueryBuilder.build(searchParams.getSearchTerm(), searchParams.getBoost().orElse(5F)));
        return queryBuilder;
    }

    @SuppressWarnings("unchecked")
    private FilterBuilder addRegionFilter(Optional<IndexQueryParams> queryParams) {
        Id regionId = queryParams.get().getRegionId().get();
        ChannelGroup region;
        try {
            Resolved<ChannelGroup<?>> resolved = Futures.get(
                    channelGroupResolver.resolveIds(ImmutableList.of(regionId)), IOException.class
            );
            region = resolved.getResources().first().get();
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        ImmutableList<ChannelNumbering> channels = ImmutableList.copyOf(region.<ChannelNumbering>getChannels());
        ImmutableList<Long> channelsIdsForRegion = channels.stream()
                .map(c -> c.getChannel().getId())
                .map(Id::longValue)
                .collect(ImmutableCollectors.toList());

        return FilterBuilders.termsFilter(
                EsContent.BROADCASTS + "." + EsBroadcast.CHANNEL, channelsIdsForRegion
        );
    }
}