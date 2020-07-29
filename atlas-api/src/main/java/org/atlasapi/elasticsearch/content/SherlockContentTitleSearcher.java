package org.atlasapi.elasticsearch.content;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.elasticsearch.util.FiltersBuilder;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.response.IdSearchQueryResponse;
import com.metabroadcast.sherlock.client.scoring.QueryWeighting;
import com.metabroadcast.sherlock.client.scoring.Weighting;
import com.metabroadcast.sherlock.client.scoring.Weightings;
import com.metabroadcast.sherlock.client.search.SearchQuery;
import com.metabroadcast.sherlock.client.search.SherlockSearcher;
import com.metabroadcast.sherlock.common.SherlockIndex;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import com.google.common.util.concurrent.ListenableFuture;
import sherlock_client_shaded.org.elasticsearch.common.lucene.search.function.CombineFunction;
import sherlock_client_shaded.org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import sherlock_client_shaded.org.elasticsearch.search.sort.SortOrder;

import static com.google.common.base.Preconditions.checkNotNull;

public class SherlockContentTitleSearcher implements ContentTitleSearcher {

    private final SherlockSearcher searcher;
    private final ContentMapping contentMapping;

    public SherlockContentTitleSearcher(SherlockSearcher searcher, ContentMapping contentMapping) {
        this.searcher = checkNotNull(searcher);
        this.contentMapping = checkNotNull(contentMapping);
    }

    @Override
    public final ListenableFuture<IdSearchQueryResponse> search(org.atlasapi.search.SearchQuery search) {

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
                    contentMapping.getSource().getKey(),
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
                    RangeParameter.from(contentMapping.getLocations().getAvailabilityStart(), now),
                    RangeParameter.to(contentMapping.getLocations().getAvailabilityEnd(), now)
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
                .withIndex(SherlockIndex.CONTENT)
                .build();

        return searcher.searchForIds(searchQuery);
    }
}
