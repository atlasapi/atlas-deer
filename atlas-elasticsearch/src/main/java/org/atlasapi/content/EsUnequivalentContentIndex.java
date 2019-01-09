package org.atlasapi.content;

import java.io.IOException;
import java.util.List;
import java.util.Optional;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.EsQueryParser;
import org.atlasapi.query.IndexQueryParams;
import org.atlasapi.query.QueryOrdering;
import org.atlasapi.util.ElasticsearchIndexCreator;
import org.atlasapi.util.EsQueryBuilder;
import org.atlasapi.util.FiltersBuilder;
import org.atlasapi.util.FutureSettingActionListener;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
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

import static com.google.common.base.Preconditions.checkNotNull;

public class EsUnequivalentContentIndex extends AbstractIdleService
        implements ContentIndex, DelegateContentIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsUnequivalentContentIndex.class);

    private final Client esClient;
    private final String index;
    private final ChannelGroupResolver channelGroupResolver;

    private final SecondaryIndex equivIdIndex;
    private final EsUnequivalentContentIndexer indexer;

    private final EsQueryParser esQueryParser;
    private final EsQueryBuilder queryBuilderFactory;

    private EsUnequivalentContentIndex(
            Client esClient,
            String indexName,
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
                requestTimeout.longValue()
        );
        this.indexer = new EsUnequivalentContentIndexer(
                esClient,
                indexName,
                requestTimeout,
                translator
        );

        this.esQueryParser = EsQueryParser.create();
        this.queryBuilderFactory = EsQueryBuilder.create();
    }

    public static EsUnequivalentContentIndex create(
            Client esClient,
            String indexName,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex,
            Integer requestTimeout
    ) {
        return new EsUnequivalentContentIndex(
                esClient,
                indexName,
                channelGroupResolver,
                equivIdIndex,
                requestTimeout
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
    public ListenableFuture<IndexQueryResult> query(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        SettableFuture<SearchResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(response, (SearchResponse input) -> {
            ImmutableList<Id> ids = StreamSupport.stream(
                    input.getHits().spliterator(),
                    false
            )
                    .map(this::getId)
                    .collect(MoreCollectors.toImmutableList());

            return IndexQueryResult.withIds(ids, input.getHits().getTotalHits());
        });
    }

    @Override
    public ListenableFuture<DelegateIndexQueryResult> delegateQuery(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        SettableFuture<SearchResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(response, (SearchResponse input) -> {
            DelegateIndexQueryResult.Builder resultBuilder = DelegateIndexQueryResult.builder(
                    input.getHits().getTotalHits()
            );

            StreamSupport.stream(input.getHits().spliterator(), false)
                    .filter(hit -> getCanonicalId(hit).isPresent() && getSource(hit).isPresent())
                    .forEach(hit -> resultBuilder.add(
                            getId(hit), getCanonicalId(hit).get(), getSource(hit).get()
                    ));

            return resultBuilder.build();
        });
    }

    private SettableFuture<SearchResponse> queryInternal(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        SettableFuture<SearchResponse> response = SettableFuture.create();

        EsQueryParser.EsQuery esQuery = esQueryParser.parse(query);

        QueryBuilder queryBuilder = queryBuilderFactory.buildQuery(esQuery.getAttributeQuerySet());

        /* matchAllFilter as a bool filter with less than 1 clause is invalid */
        BoolFilterBuilder filterBuilder = FilterBuilders.boolFilter()
                .must(FilterBuilders.matchAllFilter());

        SearchRequestBuilder reqBuilder = esClient
                .prepareSearch(index)
                .setTypes(
                        EsContent.CHILD_ITEM,
                        EsContent.TOP_LEVEL_CONTAINER,
                        EsContent.TOP_LEVEL_ITEM
                )
                .addField(EsContent.CANONICAL_ID)
                .addField(EsContent.ID)
                .addField(EsContent.SOURCE)
                .addField(EsContent.BROADCASTS)
                .addField(EsContent.LOCATIONS)
                .setPostFilter(FiltersBuilder.buildForPublishers(EsContent.SOURCE, publishers))
                .setFrom(selection.getOffset())
                .setSize(Objects.firstNonNull(selection.getLimit(), DEFAULT_LIMIT));

        addOrdering(esQuery.getIndexQueryParams(), reqBuilder);

        queryBuilder = addFuzzyQuery(esQuery.getIndexQueryParams(), queryBuilder, reqBuilder);

        addBrandId(esQuery.getIndexQueryParams(), filterBuilder);
        addSeriesId(esQuery.getIndexQueryParams(), filterBuilder);
        addTopicFilter(esQuery.getIndexQueryParams(), filterBuilder);
        addActionableFilter(esQuery.getIndexQueryParams(), filterBuilder);

        reqBuilder.addSort(EsContent.ID, SortOrder.ASC);

        FilteredQueryBuilder finalQuery = QueryBuilders.filteredQuery(queryBuilder, filterBuilder);
        reqBuilder.setQuery(finalQuery);
        log.debug(reqBuilder.internalBuilder().toString());
        reqBuilder.execute(FutureSettingActionListener.setting(response));

        return response;
    }

    private void addOrdering(
            IndexQueryParams queryParams,
            SearchRequestBuilder reqBuilder
    ) {
        if (queryParams.getOrdering().isPresent()) {
            addSortOrder(queryParams.getOrdering(), reqBuilder);
        }
    }

    private QueryBuilder addFuzzyQuery(
            IndexQueryParams queryParams,
            QueryBuilder queryBuilder,
            SearchRequestBuilder reqBuilder
    ) {
        if (queryParams.getFuzzyQueryParams().isPresent()) {
            queryBuilder = addTitleQuery(queryParams, queryBuilder);
            if (queryParams.getBroadcastWeighting().isPresent()) {
                queryBuilder = BroadcastQueryBuilder.build(
                        queryBuilder,
                        queryParams.getBroadcastWeighting().get()
                );
            } else {
                queryBuilder = BroadcastQueryBuilder.build(queryBuilder, 5f);
            }
            reqBuilder.addSort(SortBuilders.scoreSort().order(SortOrder.DESC));
        }
        return queryBuilder;
    }

    private void addBrandId(
            IndexQueryParams queryParams,
            BoolFilterBuilder filterBuilder
    ) {
        if (queryParams.getBrandId().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.getBrandIdFilter(
                            queryParams.getBrandId().get(),
                            equivIdIndex
                    )
            );
        }
    }

    private void addSeriesId(
            IndexQueryParams queryParams,
            BoolFilterBuilder filterBuilder
    ) {
        if (queryParams.getSeriesId().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.getSeriesIdFilter(
                            queryParams.getSeriesId().get(), equivIdIndex
                    )
            );
        }
    }

    private void addTopicFilter(
            IndexQueryParams queryParams,
            BoolFilterBuilder filterBuilder
    ) {
        if (queryParams.getTopicFilterIds().isPresent()) {
            filterBuilder.must(
                    FiltersBuilder.buildTopicIdFilter(queryParams.getTopicFilterIds().get())
            );
        }
    }

    private void addActionableFilter(
            IndexQueryParams queryParams,
            BoolFilterBuilder filterBuilder
    ) {
        if (queryParams.getActionableFilterParams().isPresent()) {
            FilterBuilder actionableFilter = FiltersBuilder.buildActionableFilter(
                    queryParams.getActionableFilterParams().get(),
                    queryParams.getRegionIds(),
                    queryParams.getPlatformIds(),
                    queryParams.getDttIds(),
                    queryParams.getIpIds(),
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

    private QueryBuilder addTitleQuery(
            IndexQueryParams queryParams,
            QueryBuilder queryBuilder
    ) {
        FuzzyQueryParams searchParams = queryParams.getFuzzyQueryParams().get();
        queryBuilder = QueryBuilders.boolQuery()
                .must(queryBuilder)
                .must(TitleQueryBuilder.build(
                        searchParams.getSearchTerm(),
                        searchParams.getBoost().orElse(5F)
                ));
        return queryBuilder;
    }

    private Id getId(SearchHit hit) {
        return Id.valueOf(hit.field(EsContent.ID).<Number>value().longValue());
    }

    private Optional<Id> getCanonicalId(SearchHit hit) {
        if (hit == null || hit.field(EsContent.CANONICAL_ID) == null) {
            return Optional.empty();
        }
        Long id = hit.field(EsContent.CANONICAL_ID).<Number>value().longValue();
        return Optional.of(Id.valueOf(id));
    }

    private Optional<Publisher> getSource(SearchHit hit) {
        if (hit == null || hit.field(EsContent.SOURCE) == null) {
            return Optional.empty();
        }
        String source = hit.field(EsContent.SOURCE).value();
        Maybe<Publisher> publisherMaybe = Publisher.fromKey(source);

        return Optional.ofNullable(publisherMaybe.valueOrNull());
    }
}
