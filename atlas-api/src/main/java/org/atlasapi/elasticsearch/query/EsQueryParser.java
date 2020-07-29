package org.atlasapi.elasticsearch.query;

import java.util.Set;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.elasticsearch.util.EsQueryBuilder;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsQueryParser {

    public static EsQueryParser create() {
        return new EsQueryParser();
    }

    public EsQuery parse(Iterable<AttributeQuery<?>> querySet) {
        ImmutableSet.Builder<AttributeQuery<?>> esQueryBuilderQueries = ImmutableSet.builder();
        ImmutableSet.Builder<AttributeQuery<?>> indexQueries = ImmutableSet.builder();

        for (AttributeQuery<?> query : querySet) {
            if (IndexQueryParams.SUPPORTED_ATTRIBUTES.contains(query.getAttribute())) {
                indexQueries.add(query);
            } else if (EsQueryBuilder.SUPPORTED_ATTRIBUTES.contains(query.getAttribute())) {
                esQueryBuilderQueries.add(query);
            }
        }

        return EsQuery.create(
                esQueryBuilderQueries.build(),
                IndexQueryParams.parse(
                        indexQueries.build()
                )
        );
    }

    public static class EsQuery {

        private final Set<AttributeQuery<?>> attributeQuerySet;
        private final IndexQueryParams indexQueryParams;

        private EsQuery(
                Set<AttributeQuery<?>> attributeQuerySet,
                IndexQueryParams indexQueryParams
        ) {
            this.attributeQuerySet = checkNotNull(attributeQuerySet);
            this.indexQueryParams = checkNotNull(indexQueryParams);
        }

        public static EsQuery create(
                Set<AttributeQuery<?>> attributeQuerySet,
                IndexQueryParams indexQueryParams
        ) {
            return new EsQuery(attributeQuerySet, indexQueryParams);
        }

        public Set<AttributeQuery<?>> getAttributeQuerySet() {
            return attributeQuerySet;
        }

        public IndexQueryParams getIndexQueryParams() {
            return indexQueryParams;
        }
    }
}
