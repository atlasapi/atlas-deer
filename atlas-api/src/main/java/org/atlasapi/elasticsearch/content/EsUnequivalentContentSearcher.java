package org.atlasapi.elasticsearch.content;

import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.query.Selection;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.response.ContentSearchQueryResponse;
import com.metabroadcast.sherlock.client.scoring.ConstantValueWeighting;
import com.metabroadcast.sherlock.client.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.scoring.Weightings;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.type.ChildTypeMapping;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentSearcher;
import org.atlasapi.content.FuzzyQueryParams;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.legacy.LegacyContentFieldTranslator;
import org.atlasapi.criteria.legacy.LegacyTranslation;
import org.atlasapi.elasticsearch.query.EsQueryParser;
import org.atlasapi.elasticsearch.query.IndexQueryParams;
import org.atlasapi.elasticsearch.query.QueryOrdering;
import org.atlasapi.elasticsearch.util.EsQueryBuilder;
import org.atlasapi.elasticsearch.util.FiltersBuilder;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.SecondaryIndex;
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.search.sort.SortOrder;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsUnequivalentContentSearcher implements ContentSearcher, DelegateContentSearcher {

    private static final int DEFAULT_LIMIT = 50;

    private final Logger log = LoggerFactory.getLogger(EsUnequivalentContentSearcher.class);

    private final SherlockSearcher contentSearcher;
    private final ContentMapping contentMapping;
    private final ChannelGroupResolver channelGroupResolver;
    private final SecondaryIndex equivIdIndex;

    private final EsQueryParser esQueryParser;
    private final EsQueryBuilder queryBuilderFactory;

    private EsUnequivalentContentSearcher(
            SherlockSearcher contentSearcher,
            ContentMapping contentMapping,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex
    ) {
        this.contentSearcher = checkNotNull(contentSearcher);
        this.contentMapping = checkNotNull(contentMapping);
        this.channelGroupResolver = checkNotNull(channelGroupResolver);
        this.equivIdIndex = checkNotNull(equivIdIndex);
        this.esQueryParser = EsQueryParser.create();
        this.queryBuilderFactory = EsQueryBuilder.create(LegacyContentFieldTranslator::translate);
    }

    public static EsUnequivalentContentSearcher create(
            SherlockSearcher contentSearcher,
            ContentMapping contentMapping,
            ChannelGroupResolver channelGroupResolver,
            SecondaryIndex equivIdIndex
    ) {
        return new EsUnequivalentContentSearcher(
                contentSearcher,
                contentMapping,
                channelGroupResolver,
                equivIdIndex
        );
    }

    @Override
    public ListenableFuture<IndexQueryResult> query(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        ListenableFuture<ContentSearchQueryResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(
                response,
                (ContentSearchQueryResponse input) ->
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
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {
        ListenableFuture<ContentSearchQueryResponse> response = queryInternal(
                query, publishers, selection
        );

        return Futures.transform(response, (ContentSearchQueryResponse input) -> {
            DelegateIndexQueryResult.Builder resultBuilder = DelegateIndexQueryResult.builder(
                    input.getTotalResults()
            );

            input.getResults().stream()
                    .filter(Objects::nonNull)
                    .forEach(hit -> resultBuilder.add(
                            Id.valueOf(hit.getId()),
                            hit.getScore(),
                            Id.valueOf(hit.getCanonicalId()),
                            Publisher.fromKey(hit.getSource()).requireValue(),
                            hit.getTitle()
                    ));

            return resultBuilder.build();
        });
    }

    private ListenableFuture<ContentSearchQueryResponse> queryInternal(
            Iterable<AttributeQuery<?>> query,
            Iterable<Publisher> publishers,
            Selection selection
    ) {

        Set<String> sources = MoreStreams.stream(publishers)
                .map(Publisher::key)
                .collect(MoreCollectors.toImmutableSet());

        for (AttributeQuery<?> attributeQuery : query) {
            if (Attributes.SOURCE.equals(attributeQuery.getAttribute())) {
                List<String> sourcesFromAttribute = ((StringAttributeQuery) attributeQuery).getValue();
                sources = Sets.intersection(ImmutableSet.copyOf(sourcesFromAttribute), sources);
            }
        }

        SearchQuery.Builder searchQueryBuilder = SearchQuery.builder()
                .addFilter(
                        FiltersBuilder.buildForPublishers(
                                contentMapping.getSource().getKey(),
                                publishers)
                )
                .withIndex(
                        SherlockIndex.CONTENT,
                        sources
                )
                .withOffset(selection.getOffset())
                .withLimit(selection.getLimit() == null ? DEFAULT_LIMIT : selection.getLimit());

        EsQueryParser.EsQuery esQuery = esQueryParser.parse(query);
        addOrdering(esQuery.getIndexQueryParams(), searchQueryBuilder);

        BoolParameter queryBuilder = queryBuilderFactory.buildQuery(esQuery.getAttributeQuerySet());
        searchQueryBuilder.addSearcher(queryBuilder);

        QueryWeighting.Builder queryWeightingBuilder = QueryWeighting.builder()
                .withScoreMode(FunctionScoreQuery.ScoreMode.SUM)
                .withCombineFunction(CombineFunction.MULTIPLY)
                .withMaxBoost(3.0F)
                .withWeighting(ConstantValueWeighting.of(1f));

        addFuzzyQueryAndBroadcastWeighting(
                searchQueryBuilder,
                queryWeightingBuilder,
                esQuery.getIndexQueryParams()
        );

        searchQueryBuilder.withQueryWeighting(queryWeightingBuilder.build());

        addBrandId(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addSeriesId(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addTopicFilter(esQuery.getIndexQueryParams(), searchQueryBuilder);
        addActionableFilter(esQuery.getIndexQueryParams(), searchQueryBuilder);

        searchQueryBuilder.addSort(contentMapping.getId(), SortOrder.ASC);

        return contentSearcher.searchForContent(searchQueryBuilder.build());
    }

    private void addOrdering(
            IndexQueryParams queryParams,
            SearchQuery.Builder searchQueryBuilder
    ) {
        if (queryParams.getOrdering().isPresent()) {
            addSortOrder(queryParams.getOrdering(), searchQueryBuilder);
        }
    }

    private void addFuzzyQueryAndBroadcastWeighting(
            SearchQuery.Builder searchQueryBuilder,
            QueryWeighting.Builder queryWeightingBuilder,
            IndexQueryParams queryParams
    ) {
        if (queryParams.getFuzzyQueryParams().isPresent()) {
            addTitleQuery(searchQueryBuilder, queryParams.getFuzzyQueryParams().get());
            float broadcastWeighting = queryParams.getBroadcastWeighting().isPresent()
                    ? queryParams.getBroadcastWeighting().get()
                    : 0.2f;
            queryWeightingBuilder.withWeighting(Weightings.recentBroadcast(broadcastWeighting));
            searchQueryBuilder.addScoreSort(SortOrder.DESC);
        }
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

    private void addSortOrder(Optional<QueryOrdering> ordering, SearchQuery.Builder searchQueryBuilder) {
        QueryOrdering order = ordering.get();
        for (QueryOrdering.Clause clause : order.getSortOrder()) {
            if ("relevance".equalsIgnoreCase(clause.getPath())) {
                searchQueryBuilder.addScoreSort(SortOrder.DESC);
            } else {
                LegacyTranslation translation = LegacyContentFieldTranslator.translate(clause.getPath());
                if (translation.shouldThrowException()) {
                    throw new IllegalArgumentException(clause.getPath() + " is not a known field.");
                } else if (!translation.shouldSilentlyIgnore()) {
                    ChildTypeMapping<?> mapping = translation.getMapping();
                    searchQueryBuilder.addSort(
                            mapping,
                            clause.isAscending() ? SortOrder.ASC : SortOrder.DESC
                    );
                }
            }
        }
    }

    private void addTitleQuery(
            SearchQuery.Builder searchQueryBuilder,
            FuzzyQueryParams searchParams
    ) {
        searchQueryBuilder.addSearcher(
                TitleQueryBuilder.build(
                        searchParams.getSearchTerm(),
                        searchParams.getBoost().orElse(5F))
        );
    }

}
