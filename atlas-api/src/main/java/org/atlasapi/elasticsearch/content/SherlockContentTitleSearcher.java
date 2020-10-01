package org.atlasapi.elasticsearch.content;

import java.util.ArrayList;
import java.util.List;

import org.atlasapi.content.ContentTitleSearcher;
import org.atlasapi.elasticsearch.util.FiltersBuilder;

import com.metabroadcast.sherlock.client.parameter.ExistParameter;
import com.metabroadcast.sherlock.client.parameter.ParentExistParameter;
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
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
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

        // filter to top level items, or series
        searchQueryBuilder.addFilter(
                SingleClauseBoolParameter.should(
                        TermParameter.of(contentMapping.getTopLevel(), true),
                        TermParameter.of(contentMapping.getType(), "series")
                )
        );

        searchQueryBuilder.addSearcher(
                TitleQueryBuilder.build(search.getTerm(), search.getTitleWeighting())
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
            weightings.add(Weightings.availability(search.getCatchupWeighting()));
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
