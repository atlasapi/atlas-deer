package org.atlasapi.content;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

import java.util.List;

import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.criteria.SortAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attributes;
import org.atlasapi.criteria.operator.Operators;
import org.atlasapi.util.EsSortQueryBuilder;
import org.elasticsearch.search.sort.SortBuilder;
import org.junit.Test;

import com.google.common.collect.ImmutableList;

public class EsSortQueryBuilderTest {

    @Test
    public void testBuildsSortQueryFromAttributeQuerySet() {
        AttributeQuerySet querySet = new AttributeQuerySet(
                ImmutableList.of(
                        new SortAttributeQuery(
                                Attributes.ORDER_BY_IDENTIFIED,
                                Operators.EQUALS,
                                ImmutableList.of("price.value")
                        )
                )
        );
        EsSortQueryBuilder builder = new EsSortQueryBuilder();
        List<SortBuilder> sorts = builder.buildSortQuery(querySet);
        assertThat(sorts.size(), is(1));
    }

}
