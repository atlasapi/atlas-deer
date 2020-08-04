package org.atlasapi.elasticsearch.query;

import org.atlasapi.content.ContentType;
import org.atlasapi.content.Specialization;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import org.junit.Before;
import org.junit.Test;

import static org.atlasapi.criteria.attribute.Attributes.ACTIONABLE_FILTER_PARAMETERS;
import static org.atlasapi.criteria.attribute.Attributes.ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.BROADCAST_WEIGHT;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_GROUP;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TITLE_PREFIX;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TYPE;
import static org.atlasapi.criteria.attribute.Attributes.EPISODE_BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.GENRE;
import static org.atlasapi.criteria.attribute.Attributes.ID;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.ORDER_BY;
import static org.atlasapi.criteria.attribute.Attributes.PLATFORM;
import static org.atlasapi.criteria.attribute.Attributes.Q;
import static org.atlasapi.criteria.attribute.Attributes.REGION;
import static org.atlasapi.criteria.attribute.Attributes.SEARCH_TOPIC_ID;
import static org.atlasapi.criteria.attribute.Attributes.SERIES_ID;
import static org.atlasapi.criteria.attribute.Attributes.SOURCE;
import static org.atlasapi.criteria.attribute.Attributes.SPECIALIZATION;
import static org.atlasapi.criteria.attribute.Attributes.TAG_RELATIONSHIP;
import static org.atlasapi.criteria.attribute.Attributes.TAG_SUPERVISED;
import static org.atlasapi.criteria.attribute.Attributes.TAG_WEIGHTING;
import static org.atlasapi.criteria.attribute.Attributes.TITLE_BOOST;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EsQueryParserTest {

    private EsQueryParser parser;

    @Before
    public void setUp() throws Exception {
        parser = EsQueryParser.create();
    }

    @Test
    public void parseQueryWithEsQueryBuilderParams() throws Exception {
        ImmutableList<AttributeQuery<?>> queries = ImmutableList
                .<AttributeQuery<?>>builder()
                .add(query(CONTENT_TYPE, ContentType.BRAND))
                .add(query(SOURCE, Publisher.METABROADCAST))
                .add(query(ALIASES_NAMESPACE, "ns"))
                .add(query(ALIASES_VALUE, "value"))
                .add(query(LOCATIONS_ALIASES_NAMESPACE, "ns"))
                .add(query(LOCATIONS_ALIASES_VALUE, "value"))
                .add(query(TAG_RELATIONSHIP, "rel"))
                .add(query(TAG_SUPERVISED, false))
                .add(query(TAG_WEIGHTING, 5.0F))
                .add(query(CONTENT_TITLE_PREFIX, "title"))
                .add(query(GENRE, "genre"))
                .add(query(CONTENT_GROUP, Id.valueOf(1L)))
                .add(query(SPECIALIZATION, Specialization.FILM))
                .build();

        EsQueryParser.EsQuery query = parser.parse(queries);

        assertThat(
                query.getAttributeQuerySet().size(),
                is(13)
        );

        IndexQueryParams params = query.getIndexQueryParams();

        assertThat(params.getFuzzyQueryParams().isPresent(), is(false));
        assertThat(params.getOrdering().isPresent(), is(false));
        assertThat(params.getRegionIds().isPresent(), is(false));
        assertThat(params.getPlatformIds().isPresent(), is(false));
        assertThat(params.getBroadcastWeighting().isPresent(), is(false));
        assertThat(params.getTopicFilterIds().isPresent(), is(false));
        assertThat(params.getBrandId().isPresent(), is(false));
        assertThat(params.getActionableFilterParams().isPresent(), is(false));
        assertThat(params.getSeriesId().isPresent(), is(false));
    }

    @Test
    public void parseQueryWithEsQueryBuilderParamsWithSuffixes() throws Exception {
        ImmutableList<AttributeQuery<?>> queries = ImmutableList
                .<AttributeQuery<?>>builder()
                .add(CONTENT_TITLE_PREFIX.createQuery(
                        Operators.BEGINNING,
                        ImmutableList.copyOf(new String[] { "title" })
                ))
                .build();

        EsQueryParser.EsQuery query = parser.parse(queries);

        assertThat(
                query.getAttributeQuerySet().size(),
                is(1)
        );
        assertThat(
                query.getAttributeQuerySet()
                        .iterator()
                        .next()
                        .getOperator(),
                is(Operators.BEGINNING)
        );
    }

    @Test
    public void parseQueryWithIndexQueryParams() throws Exception {
        ImmutableList<AttributeQuery<?>> queries = ImmutableList
                .<AttributeQuery<?>>builder()
                .add(query(Q, "title"))
                .add(query(TITLE_BOOST, 5.0F))
                .add(query(ORDER_BY, "title"))
                .add(query(REGION, Id.valueOf(1L)))
                .add(query(PLATFORM, Id.valueOf(1L)))
                .add(query(BROADCAST_WEIGHT, 5.0F))
                .add(query(SEARCH_TOPIC_ID, "hk7"))
                .add(query(EPISODE_BRAND_ID, Id.valueOf(1L)))
                .add(query(BRAND_ID, Id.valueOf(1L)))
                .add(query(ACTIONABLE_FILTER_PARAMETERS, "locations.available:true"))
                .add(query(SERIES_ID, Id.valueOf(1L)))
                .build();

        EsQueryParser.EsQuery query = parser.parse(queries);

        assertThat(query.getAttributeQuerySet().isEmpty(), is(true));

        IndexQueryParams params = query.getIndexQueryParams();

        assertThat(params.getFuzzyQueryParams().isPresent(), is(true));
        assertThat(params.getOrdering().isPresent(), is(true));
        assertThat(params.getRegionIds().isPresent(), is(true));
        assertThat(params.getPlatformIds().isPresent(), is(true));
        assertThat(params.getBroadcastWeighting().isPresent(), is(true));
        assertThat(params.getTopicFilterIds().isPresent(), is(true));
        assertThat(params.getBrandId().isPresent(), is(true));
        assertThat(params.getActionableFilterParams().isPresent(), is(true));
        assertThat(params.getSeriesId().isPresent(), is(true));
    }

    @Test
    public void parseQueryWithUnsupportedParams() throws Exception {
        ImmutableList<AttributeQuery<?>> queries = ImmutableList
                .<AttributeQuery<?>>builder()
                .add(query(ID, Id.valueOf(1L)))
                .build();

        EsQueryParser.EsQuery query = parser.parse(queries);

        assertThat(
                query.getAttributeQuerySet().size(),
                is(0)
        );

        IndexQueryParams params = query.getIndexQueryParams();

        assertThat(params.getFuzzyQueryParams().isPresent(), is(false));
        assertThat(params.getOrdering().isPresent(), is(false));
        assertThat(params.getRegionIds().isPresent(), is(false));
        assertThat(params.getPlatformIds().isPresent(), is(false));
        assertThat(params.getBroadcastWeighting().isPresent(), is(false));
        assertThat(params.getTopicFilterIds().isPresent(), is(false));
        assertThat(params.getBrandId().isPresent(), is(false));
        assertThat(params.getActionableFilterParams().isPresent(), is(false));
        assertThat(params.getSeriesId().isPresent(), is(false));
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
