package org.atlasapi.query;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;

import com.google.common.collect.ImmutableList;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import static org.atlasapi.criteria.attribute.Attributes.ACTIONABLE_FILTER_PARAMETERS;
import static org.atlasapi.criteria.attribute.Attributes.BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.BROADCAST_WEIGHT;
import static org.atlasapi.criteria.attribute.Attributes.EPISODE_BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.ORDER_BY;
import static org.atlasapi.criteria.attribute.Attributes.Q;
import static org.atlasapi.criteria.attribute.Attributes.REGION;
import static org.atlasapi.criteria.attribute.Attributes.SEARCH_TOPIC_ID;
import static org.atlasapi.criteria.attribute.Attributes.SERIES_ID;
import static org.atlasapi.criteria.attribute.Attributes.TITLE_BOOST;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class IndexQueryParamsTest {

    private static final SubstitutionTableNumberCodec codec =
            SubstitutionTableNumberCodec.lowerCaseOnly();

    @Rule
    public ExpectedException exception = ExpectedException.none();

    @Test
    public void parseWithNoSupportedAttributesReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of());

        assertThat(params.getFuzzyQueryParams().isPresent(), is(false));
        assertThat(params.getOrdering().isPresent(), is(false));
        assertThat(params.getRegionIds().isPresent(), is(false));
        assertThat(params.getBroadcastWeighting().isPresent(), is(false));
        assertThat(params.getTopicFilterIds().isPresent(), is(false));
        assertThat(params.getBrandId().isPresent(), is(false));
        assertThat(params.getActionableFilterParams().isPresent(), is(false));
        assertThat(params.getSeriesId().isPresent(), is(false));
    }

    @Test
    public void parseWithTitleSearchWithoutBoostReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(Q, "title")
        ));

        assertThat(
                params.getFuzzyQueryParams().get().getSearchTerm(),
                is("title")
        );
    }

    @Test
    public void parseWithTitleSearchWithBoostReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(Q, "title"),
                query(TITLE_BOOST, 5.0F)
        ));

        assertThat(
                params.getFuzzyQueryParams().get().getSearchTerm(),
                is("title")
        );
        assertThat(
                params.getFuzzyQueryParams().get().getBoost().get(),
                is(5.0F)
        );
    }

    @Test
    public void parseWithMissingTitlesReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(Q)
        ));

        assertThat(
                params.getFuzzyQueryParams().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleTitlesThrowsException() throws Exception {
        exception.expect(IllegalArgumentException.class);
        IndexQueryParams.parse(ImmutableList.of(
                query(Q, "title", "anotherTitle")
        ));
    }

    @Test
    public void parseWithMultipleTitleBoostsThrowsException() throws Exception {
        exception.expect(IllegalArgumentException.class);
        IndexQueryParams.parse(ImmutableList.of(
                query(Q, "title"),
                query(TITLE_BOOST, 5.0F, 6.0F)
        ));
    }

    @Test
    public void parseWithOrderByReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(ORDER_BY, "title.desc")
        ));

        ImmutableList<QueryOrdering.Clause> sortOrder = params.getOrdering().get().getSortOrder();
        assertThat(
                sortOrder.size(),
                is(1)
        );
        assertThat(
                sortOrder.get(0).isAscending(),
                is(false)
        );
        assertThat(
                sortOrder.get(0).getPath(),
                is("title")
        );
    }

    @Test
    public void parseWithMissingOrderByReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(ORDER_BY)
        ));

        assertThat(
                params.getOrdering().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleOrderByReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(ORDER_BY, "title", "episodeNumber.desc")
        ));

        assertThat(
                params.getOrdering().isPresent(),
                is(true)
        );

        ImmutableList<QueryOrdering.Clause> sortOrder = params.getOrdering().get().getSortOrder();
        assertThat(
                sortOrder.size(),
                is(2)
        );
        assertThat(
                sortOrder.get(0).getPath(),
                is("title")
        );
        assertThat(
                sortOrder.get(0).isAscending(),
                is(true)
        );
        assertThat(
                sortOrder.get(1).getPath(),
                is("episodeNumber")
        );
        assertThat(
                sortOrder.get(1).isAscending(),
                is(false)
        );
    }

    @Test
    public void parseWithRegionReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(REGION, Id.valueOf(1L))
        ));

        assertThat(
                params.getRegionIds().get().get(0),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithMissingRegionReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(REGION)
        ));

        assertThat(
                params.getRegionIds().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleRegionsReturnsTheFirstOne() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(REGION, Id.valueOf(1L), Id.valueOf(2L))
        ));

        assertThat(
                params.getRegionIds().get().get(0),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithBroadcastWeightReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BROADCAST_WEIGHT, 5.0F)
        ));

        assertThat(
                params.getBroadcastWeighting().get(),
                is(5.0F)
        );
    }

    @Test
    public void parseWithMissingBroadcastWeightReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BROADCAST_WEIGHT)
        ));

        assertThat(
                params.getBroadcastWeighting().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleBroadcastWeightsReturnsTheFirstOne() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BROADCAST_WEIGHT, 5.0F, 6.0F)
        ));

        assertThat(
                params.getBroadcastWeighting().get(),
                is(5.0F)
        );
    }

    @Test
    public void parseWithTopicIdReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SEARCH_TOPIC_ID, "cf2^!hk9")
        ));

        assertThat(
                params.getTopicFilterIds().get().size(),
                is(1)
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).size(),
                is(2)
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(0).getId().longValue(),
                is(codec.decode("cf2").longValue())
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(0).isIncluded(),
                is(true)
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(1).getId().longValue(),
                is(codec.decode("hk9").longValue())
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(1).isIncluded(),
                is(false)
        );
    }

    @Test
    public void parseWithMissingTopicIdsReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SEARCH_TOPIC_ID)
        ));

        assertThat(
                params.getTopicFilterIds().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleTopicIdsReturnsAll() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SEARCH_TOPIC_ID, "cf2", "!hk9")
        ));

        assertThat(
                params.getTopicFilterIds().get().size(),
                is(2)
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).size(),
                is(1)
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(0).getId().longValue(),
                is(codec.decode("cf2").longValue())
        );
        assertThat(
                params.getTopicFilterIds().get().get(0).get(0).isIncluded(),
                is(true)
        );
        assertThat(
                params.getTopicFilterIds().get().get(1).size(),
                is(1)
        );
        assertThat(
                params.getTopicFilterIds().get().get(1).get(0).getId().longValue(),
                is(codec.decode("hk9").longValue())
        );
        assertThat(
                params.getTopicFilterIds().get().get(1).get(0).isIncluded(),
                is(false)
        );
    }

    @Test
    public void parseWithBrandIdReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BRAND_ID, Id.valueOf(1L))
        ));

        assertThat(
                params.getBrandId().get(),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithMissingBrandIdReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BRAND_ID)
        ));

        assertThat(
                params.getBrandId().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleBrandIdsReturnsTheFirstOne() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(BRAND_ID, Id.valueOf(1L), Id.valueOf(2L))
        ));

        assertThat(
                params.getBrandId().get(),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithEpisodeBrandIdReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(EPISODE_BRAND_ID, Id.valueOf(1L))
        ));

        assertThat(
                params.getBrandId().get(),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithMissingEpisodeBrandIdReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(EPISODE_BRAND_ID)
        ));

        assertThat(
                params.getBrandId().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleEpisodeBrandIdsReturnsTheFirstOne() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(EPISODE_BRAND_ID, Id.valueOf(1L), Id.valueOf(2L))
        ));

        assertThat(
                params.getBrandId().get(),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithActionableFilterReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(
                        ACTIONABLE_FILTER_PARAMETERS,
                        "location.available:true"
                )
        ));

        assertThat(
                params.getActionableFilterParams().get().size(),
                is(1)
        );
        assertThat(
                params.getActionableFilterParams().get().get("location.available"),
                is("true")
        );
    }

    @Test
    public void parseWithMissingActionableFilterReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(ACTIONABLE_FILTER_PARAMETERS)
        ));

        assertThat(
                params.getActionableFilterParams().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleActionableFiltersReturnsAll() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(
                        ACTIONABLE_FILTER_PARAMETERS,
                        "location.available:true",
                        "broadcast.time.gt:2017-02-13T08:06:06Z",
                        "broadcast.time.lt:2017-02-13T09:00"
                )
        ));

        assertThat(
                params.getActionableFilterParams().get().size(),
                is(3)
        );
        assertThat(
                params.getActionableFilterParams().get().get("location.available"),
                is("true")
        );
        assertThat(
                params.getActionableFilterParams().get().get("broadcast.time.gt"),
                is("2017-02-13T08:06:06Z")
        );
        assertThat(
                params.getActionableFilterParams().get().get("broadcast.time.lt"),
                is("2017-02-13T09:00")
        );
    }

    @Test
    public void parseWithSeriesIdReturnsExpectedValues() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SERIES_ID, Id.valueOf(1L))
        ));

        assertThat(
                params.getSeriesId().get(),
                is(Id.valueOf(1L))
        );
    }

    @Test
    public void parseWithMissingSeriesIdReturnsEmpty() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SERIES_ID)
        ));

        assertThat(
                params.getSeriesId().isPresent(),
                is(false)
        );
    }

    @Test
    public void parseWithMultipleSeriesIdsReturnsTheFirstOne() throws Exception {
        IndexQueryParams params = IndexQueryParams.parse(ImmutableList.of(
                query(SERIES_ID, Id.valueOf(1L), Id.valueOf(2L))
        ));

        assertThat(
                params.getSeriesId().get(),
                is(Id.valueOf(1L))
        );
    }

    @SafeVarargs
    private final <T> AttributeQuery<T> query(
            Attribute<T> attribute,
            T... params
    ) {
        return attribute.createQuery(
                Operators.EQUALS,
                ImmutableList.copyOf(params)
        );
    }
}
