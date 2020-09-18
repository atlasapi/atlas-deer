package org.atlasapi.elasticsearch.content;

import java.time.Instant;
import java.time.temporal.ChronoUnit;
import java.util.ArrayList;
import java.util.List;

import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.elasticsearch.util.FiltersBuilder;

import com.metabroadcast.sherlock.client.parameter.BoolParameter;
import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.RangeParameter;
import com.metabroadcast.sherlock.client.parameter.SearchParameter;
import com.metabroadcast.sherlock.client.parameter.SingleClauseBoolParameter;
import com.metabroadcast.sherlock.client.parameter.TermParameter;
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
import org.elasticsearch.common.lucene.search.function.CombineFunction;
import org.elasticsearch.common.lucene.search.function.FunctionScoreQuery;
import org.elasticsearch.search.sort.SortOrder;

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

        // Search on content title
        searchQueryBuilder.addSearcher(
                SearchParameter.builder()
                        .withValue(search.getTerm())
                        .withMapping(contentMapping.getTitle())
                        .withExactMapping(contentMapping.getTitleExact())
                        .withFuzziness()
                        .withFuzzinessPrefixLength(2)
                        .withFuzzinessBoost(50F)
                        .withPhraseBoost(100F)
                        .withExactMatchBoost(200F)
                        .withBoost(search.getTitleWeighting())
                        .build()
        );

        // Filter to top level items, or content with children scoring on children title
        searchQueryBuilder.addSearcher(
                SingleClauseBoolParameter.should(
                        TermParameter.of(contentMapping.getTopLevel(), true),
                        SingleClauseBoolParameter.must(
                                ExistParameter.exists(contentMapping.getChildren().getId()),
                                SearchParameter.builder()
                                        .withValue(search.getTerm())
                                        .withMapping(contentMapping.getTitle())
                                        .withExactMapping(contentMapping.getTitleExact())
                                        .withFuzziness()
                                        .withFuzzinessPrefixLength(2)
                                        .withFuzzinessBoost(50F)
                                        .withPhraseBoost(100F)
                                        .withExactMatchBoost(200F)
                                        .withBoost(search.getTitleWeighting())
                                        .build()
                        )
                )
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

        if (search.getCatchupWeighting() != 0.0f) {

            Instant now = Instant.now().truncatedTo(ChronoUnit.MINUTES);

            BoolParameter availabilityParameter = SingleClauseBoolParameter.must(
                    RangeParameter.to(contentMapping.getLocations().getAvailabilityStart(), now),
                    RangeParameter.from(contentMapping.getLocations().getAvailabilityEnd(), now)
            ).boost(search.getCatchupWeighting());

            searchQueryBuilder.addSearcher(availabilityParameter);
        }

        List<Weighting> weightings = new ArrayList<>();
        if (search.getBroadcastWeighting() != 0.0f) {
            weightings.add(Weightings.broadcastWithin30Days(1f));
        }

        searchQueryBuilder.withQueryWeighting(QueryWeighting.builder()
                .withWeightings(weightings)
                .withScoreMode(FunctionScoreQuery.ScoreMode.SUM)
                .withCombineFunction(CombineFunction.MULTIPLY)
                .withMaxBoost(3f)
                .build());

        SearchQuery searchQuery = searchQueryBuilder
                .addScoreSort(SortOrder.DESC)
                .withLimit(search.getSelection().limitOrDefaultValue(10))
                .withOffset(search.getSelection().getOffset())
                .withIndex(SherlockIndex.CONTENT)
                .build();

        return searcher.searchForIds(searchQuery);
    }
}
