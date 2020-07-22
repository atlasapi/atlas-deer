package org.atlasapi.content;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;
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
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.search.ContentResult;
import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SearchQueryResponse;
import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;

import com.google.common.collect.ImmutableList;
import com.google.common.util.concurrent.AbstractIdleService;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
//import org.elasticsearch.action.search.SearchRequestBuilder;
//import org.elasticsearch.action.search.SearchResponse;
//import org.elasticsearch.client.Client;
//import org.elasticsearch.index.query.BoolFilterBuilder;
//import org.elasticsearch.index.query.FilterBuilder;
//import org.elasticsearch.index.query.FilterBuilders;
//import org.elasticsearch.index.query.FilteredQueryBuilder;
//import org.elasticsearch.index.query.QueryBuilder;
//import org.elasticsearch.index.query.QueryBuilders;
//import org.elasticsearch.search.SearchHit;
//import org.elasticsearch.search.sort.SortBuilders;
//import org.elasticsearch.search.sort.SortOrder;
import joptsimple.internal.Strings;
import org.elasticsearch.index.query.BoolFilterBuilder;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import sherlock_client_shaded.org.elasticsearch.action.search.SearchRequestBuilder;
import sherlock_client_shaded.org.elasticsearch.action.search.SearchResponse;
import sherlock_client_shaded.org.elasticsearch.index.query.QueryBuilders;
import sherlock_client_shaded.org.elasticsearch.search.SearchHit;
import sherlock_client_shaded.org.elasticsearch.search.sort.SortBuilders;
import sherlock_client_shaded.org.elasticsearch.search.sort.SortOrder;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsUnequivalentContentIndex extends AbstractIdleService
        implements ContentIndex, DelegateContentIndex {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsUnequivalentContentIndex.class);

    private final ContentSearcher contentSearcher;
    private final ContentMapping contentMapping;
    private final ChannelGroupResolver channelGroupResolver;
    private final SecondaryIndex equivIdIndex;

    private final EsQueryParser esQueryParser;
    private final EsQueryBuilder queryBuilderFactory;

    private EsUnequivalentContentIndex(
            ContentSearcher contentSearcher,
            ContentMapping contentMapping,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex
    ) {
        this.contentSearcher = checkNotNull(contentSearcher);
        this.contentMapping = checkNotNull(contentMapping);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.equivIdIndex = checkNotNull(equivIdIndex);
        this.esQueryParser = EsQueryParser.create();
        this.queryBuilderFactory = EsQueryBuilder.create();
    }

    public static EsUnequivalentContentIndex create(
            ContentSearcher contentSearcher,
            ContentMapping contentMapping,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex
    ) {
        return new EsUnequivalentContentIndex(
                contentSearcher,
                contentMapping,
                channelGroupResolver,
                equivIdIndex
        );
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        ListenableFuture<SearchQueryResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(
                response,
                (SearchQueryResponse input) ->
                        IndexQueryResult.withIds(
                                MoreStreams.stream(input.getIds())
                                        .map(Id::valueOf)
                                        .collect(MoreCollectors.toImmutableList()),
                                input.getTotalResults()
                        )
        );
    }

    @Override
    public ListenableFuture<DelegateIndexQueryResult> delegateQuery(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        ListenableFuture<SearchQueryResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(response, (SearchQueryResponse input) -> {
            DelegateIndexQueryResult.Builder resultBuilder = DelegateIndexQueryResult.builder(
                    input.getTotalResults()
            );

            StreamSupport.stream(input.getResults().spliterator(), false)
                    .filter(Objects::nonNull)
                    .forEach(hit -> resultBuilder.add(
                            Id.valueOf(hit.getId()),
                            Id.valueOf(hit.getCanonicalId()),
                            Publisher.fromKey(hit.getSource()).requireValue()
                    ));

            return resultBuilder.build();
        });
    }

    private ListenableFuture<SearchQueryResponse> queryInternal(
            AttributeQuerySet query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        SearchQuery.Builder searchQueryBuilder = SearchQuery.builder()

                // TODO This lists all types that were indexed, so may not be necessary anymore
//                .setTypes(
//                        EsContent.CHILD_ITEM,
//                        EsContent.TOP_LEVEL_CONTAINER,
//                        EsContent.TOP_LEVEL_ITEM
//                )


                .addFilter(
                        FiltersBuilder.buildForPublishers(
                                contentMapping.getSource().getKey(),
                                publishers)
                )
                .withOffset(selection.getOffset())
                .withLimit(selection.getLimit() == null ? DEFAULT_LIMIT : selection.getLimit());

        EsQueryParser.EsQuery esQuery = esQueryParser.parse(query);
        addOrdering(esQuery.getIndexQueryParams(), reqBuilder);

        // TODO add fuzzy query
        QueryBuilder queryBuilder = queryBuilderFactory.buildQuery(esQuery.getAttributeQuerySet());
        queryBuilder = addFuzzyQuery(esQuery.getIndexQueryParams(), queryBuilder, reqBuilder);
        searchQueryBuilder.addSearcher(queryBuilder);

        addBrandId(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addSeriesId(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addTopicFilter(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addActionableFilter(esQuery.getIndexQueryParams(), searchQueryBuilder);

        // TODO add sorting
        reqBuilder.addSort(EsContent.ID, SortOrder.ASC);

        return contentSearcher.searchForContent(searchQueryBuilder.build());
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
            SearchQuery.Builder searchQueryBuilder
    ) {
        if (queryParams.getBrandId().isPresent()) {
            searchQueryBuilder.addFilter(
                    FiltersBuilder.getBrandIdFilter(
                            queryParams.getBrandId().get(),
                            equivIdIndex
                    )
            );
        }
    }

    private void addSeriesId(
            IndexQueryParams queryParams,
            SearchQuery.Builder searchQueryBuilder
    ) {
        if (queryParams.getSeriesId().isPresent()) {
            searchQueryBuilder.addFilter(
                    FiltersBuilder.getSeriesIdFilter(
                            queryParams.getSeriesId().get(), equivIdIndex
                    )
            );
        }
    }

    private void addTopicFilter(
            IndexQueryParams queryParams,
            SearchQuery.Builder searchQueryBuilder
    ) {
        if (queryParams.getTopicFilterIds().isPresent()) {
            searchQueryBuilder.addFilter(
                    FiltersBuilder.buildTopicIdFilter(queryParams.getTopicFilterIds().get())
            );
        }
    }

    private void addActionableFilter(
            IndexQueryParams queryParams,
            SearchQuery.Builder searchQueryBuilder
    ) {
        if (queryParams.getActionableFilterParams().isPresent()) {
            searchQueryBuilder.addFilter(
                    FiltersBuilder.buildActionableFilter(
                            queryParams.getActionableFilterParams().get(),
                            queryParams.getRegionIds(),
                            queryParams.getPlatformIds(),
                            queryParams.getDttIds(),
                            queryParams.getIpIds(),
                            channelGroupResolver
                    )
            );
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

}
