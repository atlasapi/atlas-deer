package org.atlasapi.content;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.atlasapi.util.FiltersBuilder;

import com.metabroadcast.sherlock.client.search.ContentSearcher;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SearchQueryResponse;
import com.metabroadcast.sherlock.client.search.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.search.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.search.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.search.scoring.Weighting;
import com.metabroadcast.sherlock.client.search.scoring.Weightings;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import sherlock_client_shaded.org.elasticsearch.common.lucene.search.function.CombineFunction;
import sherlock_client_shaded.org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import sherlock_client_shaded.org.elasticsearch.search.sort.SortOrder;

public class EsContentTitleSearcher implements ContentTitleSearcher {

    private static final ContentMapping CONTENT_MAPPING = IndexMapping.getContent();

    private final ContentSearcher searcher;

    public EsContentTitleSearcher(ContentSearcher searcher) {
        this.searcher = searcher;
    }

    @Override
    public final ListenableFuture<SearchQueryResponse> search(org.atlasapi.search.SearchQuery search) {

        Preconditions.checkArgument(
                !Strings.isNullOrEmpty(search.getTerm()),
                "query term null or empty"
        );

        SearchQuery.Builder searchQueryBuilder = SearchQuery.builder();

        TitleQueryBuilder.addTitleQueryToBuilder(
                searchQueryBuilder,
                search.getTerm(),
                search.getTitleWeighting()
        );

        if (search.getIncludedPublishers() != null
            && !search.getIncludedPublishers().isEmpty()) {
            searchQueryBuilder.addFilter(FiltersBuilder.buildForPublishers(
                    CONTENT_MAPPING.getSource().getKey(),
                    search.getIncludedPublishers()
            ));
        }
        if (search.getIncludedSpecializations() != null
            && !search.getIncludedSpecializations().isEmpty()) {
            searchQueryBuilder.addFilter(
                    FiltersBuilder.buildForSpecializations(search.getIncludedSpecializations())
            );
        }

        List<Weighting> weightings = new ArrayList<>();

        if (search.getBroadcastWeighting() != 0.0f) {
            weightings.add(Weightings.broadcastWithin30Days(1f));
        }

        if (search.getCatchupWeighting() != 0.0f) {

            Instant now = Instant.now().truncatedTo(ChronoUnit.SECONDS);

            BoolParameter availabilityParameter = SingleClauseBoolParameter.must(
                    RangeParameter.from(CONTENT_MAPPING.getLocations().getAvailabilityStart(), now),
                    RangeParameter.to(CONTENT_MAPPING.getLocations().getAvailabilityEnd(), now)
            ).boost(search.getCatchupWeighting());

            searchQueryBuilder.addSearcher(availabilityParameter);
        }

        searchQueryBuilder.withQueryWeighting(QueryWeighting.builder()
                .withWeightings(weightings)
                .withScoreMode(FunctionScoreQuery.ScoreMode.SUM)
                .withCombineFunction(CombineFunction.MULTIPLY)
                .withMaxBoost(3f)
                .build());

        // TODO do child query
//        QueryBuilder finalQuery = QueryBuilders.boolQuery()
//                .should(
//                        filteredQuery(
//                                contentQuery,
//                                andFilter(
//                                        typeFilter(EsContent.TOP_LEVEL_ITEM),
//                                        termFilter(EsContent.HAS_CHILDREN, Boolean.FALSE)
//                                )
//                        )
//                )
//                .should(
//                        hasChildQuery(EsContent.CHILD_ITEM, contentQuery)
//                                .scoreType("sum")
//                );

        SearchQuery searchQuery = searchQueryBuilder
                .addScoreSort(SortOrder.DESC)
                .withLimit(search.getSelection().limitOrDefaultValue(10))
                .withOffset(search.getSelection().getOffset())
                .build();

        return searcher.searchForContent(searchQuery);
    }
}
