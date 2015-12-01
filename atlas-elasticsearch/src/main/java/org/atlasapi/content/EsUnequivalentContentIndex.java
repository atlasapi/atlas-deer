package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.ElasticsearchIndexCreator;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.action.search.SearchRequestBuilder;
import org.elasticsearch.action.search.SearchResponse;
import org.elasticsearch.client.Client;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.FilteredQueryBuilder;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.search.SearchHit;
import org.elasticsearch.search.sort.SortBuilders;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableListMultimap;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.metabroadcast.common.query.Selection;

public class EsUnequivalentContentIndex extends AbstractIdleService implements ContentIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsUnequivalentContentIndex.class);

    private final Client esClient;
    private final String index;
    private final ChannelGroupResolver channelGroupResolver;

    private final EsQueryBuilder queryBuilderFactory = new EsQueryBuilder();

    private final SecondaryIndex equivIdIndex;
    private final EsUnequivalentContentIndexer indexer;

    public EsUnequivalentContentIndex(
            Client esClient,
            String indexName,
            ContentResolver resolver,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex,
            Integer requestTimeout
    ) {
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
    public void updateCanonicalIds(Id canonicalId, Iterable<Id> setIds) throws IndexException {
        indexer.updateCanonicalIds(canonicalId, setIds);
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
                .setTypes(EsContent.CHILD_ITEM, EsContent.TOP_LEVEL_CONTAINER, EsContent.TOP_LEVEL_ITEM)
                .addField(EsContent.CANONICAL_ID)
                .addField(EsContent.ID)
                .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT));

        if (queryParams.isPresent()) {

            addOrdering(queryParams, reqBuilder);

            queryBuilder = addFuzzyQuery(queryParams, queryBuilder, reqBuilder);

            addBrandId(queryParams, filterBuilder);
            addSeriesId(queryParams, filterBuilder);
            addTopicFilter(queryParams, filterBuilder);
            addActionableFilter(queryParams, filterBuilder);
        }

        reqBuilder.addSort(EsContent.ID, SortOrder.ASC);

        FilteredQueryBuilder finalQuery = QueryBuilders.filteredQuery(queryBuilder, filterBuilder);
        reqBuilder.setQuery(finalQuery);
        log.debug(reqBuilder.internalBuilder().toString());
        reqBuilder.execute(FutureSettingActionListener.setting(response));
        /* TODO
         * if selection.offset + selection.limit < totalHits
         * then we have more: return for use with response.
         */
        return Futures.transform(response, (SearchResponse input) -> {
            ImmutableListMultimap<Id, Id> canonicalIdToIdMultiMap = StreamSupport
                    .stream(input.getHits().spliterator(), false)
                    .filter(hit -> getCanonicalId(hit) != null)
                    .collect(ImmutableCollectors.toListMultiMap(this::getCanonicalId, this::getId));

            return IndexQueryResult.withIdsAndCanonicalIds(
                    canonicalIdToIdMultiMap,
                    input.getHits().getTotalHits()
            );
        });
    }

    private void addOrdering(Optional<IndexQueryParams> queryParams,
            SearchRequestBuilder reqBuilder) {
        if (queryParams.get().getOrdering().isPresent()) {
            addSortOrder(queryParams.get().getOrdering(), reqBuilder);
        }
    }

    private QueryBuilder addFuzzyQuery(Optional<IndexQueryParams> queryParams,
            QueryBuilder queryBuilder, SearchRequestBuilder reqBuilder) {
        if (queryParams.get().getFuzzyQueryParams().isPresent()) {
            queryBuilder = addTitleQuery(queryParams, queryBuilder);
            if (queryParams.isPresent() && queryParams.get().getBroadcastWeighting().isPresent()) {
                queryBuilder = BroadcastQueryBuilder.build(
                        queryBuilder,
                        queryParams.get().getBroadcastWeighting().get()
                );
            } else {
                queryBuilder = BroadcastQueryBuilder.build(queryBuilder, 5f);
            }
            reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        }
        return queryBuilder;
    }

    private void addBrandId(Optional<IndexQueryParams> queryParams,
            BoolFilterBuilder filterBuilder) {
        if (queryParams.get().getBrandId().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.getBrandIdFilter(
                            queryParams.get().getBrandId().get(),
                            equivIdIndex
                    )
            );
        }
    }

    private void addSeriesId(Optional<IndexQueryParams> queryParams,
            BoolFilterBuilder filterBuilder) {
        if (queryParams.get().getSeriesId().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.getSeriesIdFilter(
                            queryParams.get().getSeriesId().get(), equivIdIndex
                    )
            );
        }
    }

    private void addTopicFilter(Optional<IndexQueryParams> queryParams,
            BoolFilterBuilder filterBuilder) {
        if (queryParams.get().getTopicFilterIds().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.buildTopicIdFilter(queryParams.get().getTopicFilterIds().get())
            );
        }
    }

    private void addActionableFilter(Optional<IndexQueryParams> queryParams,
            BoolFilterBuilder filterBuilder) {
        if (queryParams.get().getActionableFilterParams().isPresent()) {
            Optional<Id> maybeRegionId = queryParams.get().getRegionId();
            FilterBuilder actionableFilter = FiltersBuilder.buildActionableFilter(
                    queryParams.get().getActionableFilterParams().get(),
                    maybeRegionId,
                    channelGroupResolver
            );
            filterBuilder.must(actionableFilter);
        }
    }

    private void addSortOrder(Optional<QueryOrdering> ordering, SearchRequestBuilder reqBuilder) {
        QueryOrdering order = ordering.get();
        for (QueryOrdering.Clause clause : order.getSortOrder()) {
            if ("relevance".equalsIgnoreCase(clause.getPath())) {
                reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
            } else {
                reqBuilder.addSort(
                        SortBuilders
                                .fieldSort(translateOrderField(clause.getPath()))
                                .missing("_last")
                                .order(clause.isAscending() ? SortOrder.ASC : SortOrder.DESC)
                );
            }
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

    private Id getId(SearchHit hit) {
        return Id.valueOf(hit.field(EsContent.ID).<Number>value().longValue());
    }

    private Id getCanonicalId(SearchHit hit) {
        if (hit == null || hit.field(EsContent.CANONICAL_ID) == null) {
            return null;
        }
        Long id = hit.field(EsContent.CANONICAL_ID).<Number>value().longValue();
        return Id.valueOf(id);
    }
}