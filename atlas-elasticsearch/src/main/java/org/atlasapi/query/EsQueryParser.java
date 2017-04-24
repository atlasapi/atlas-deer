package org.atlasapi.query;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.util.EsQueryBuilder;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;

public class EsQueryParser {

    private EsQueryParser() {
    }

    public static EsQueryParser create() {
        return new EsQueryParser();
    }

    public EsQuery parse(AttributeQuerySet querySet) {
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
                AttributeQuerySet.create(
                        esQueryBuilderQueries.build()
                ),
                IndexQueryParams.parse(
                        indexQueries.build()
                )
        );
    }

    public static class EsQuery {

        private final AttributeQuerySet attributeQuerySet;
        private final IndexQueryParams indexQueryParams;

        private EsQuery(
                AttributeQuerySet attributeQuerySet,
                IndexQueryParams indexQueryParams
        ) {
            this.attributeQuerySet = checkNotNull(attributeQuerySet);
            this.indexQueryParams = checkNotNull(indexQueryParams);
        }

        public static EsQuery create(
                AttributeQuerySet attributeQuerySet,
                IndexQueryParams indexQueryParams
        ) {
            return new EsQuery(attributeQuerySet, indexQueryParams);
        }

        public AttributeQuerySet getAttributeQuerySet() {
            return attributeQuerySet;
        }

        public IndexQueryParams getIndexQueryParams() {
            return indexQueryParams;
        }
    }
}
