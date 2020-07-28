package org.atlasapi.query;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.util.EsQueryBuilder;

import com.metabroadcast.sherlock.client.search.parameter.Parameter;
import com.metabroadcast.sherlock.client.search.parameter.TermParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;

import com.google.common.collect.ImmutableSet;

import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.criteria.attribute.Attributes.ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_GROUP;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TITLE_PREFIX;
import static org.atlasapi.criteria.attribute.Attributes.CONTENT_TYPE;
import static org.atlasapi.criteria.attribute.Attributes.GENRE;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_NAMESPACE;
import static org.atlasapi.criteria.attribute.Attributes.LOCATIONS_ALIASES_VALUE;
import static org.atlasapi.criteria.attribute.Attributes.SOURCE;
import static org.atlasapi.criteria.attribute.Attributes.SPECIALIZATION;
import static org.atlasapi.criteria.attribute.Attributes.TAG_RELATIONSHIP;
import static org.atlasapi.criteria.attribute.Attributes.TAG_SUPERVISED;
import static org.atlasapi.criteria.attribute.Attributes.TAG_WEIGHTING;

public class EsQueryParser {

    private final ContentMapping contentMapping;

    private EsQueryParser(ContentMapping contentMapping) {
        this.contentMapping = contentMapping;
    }

    public static EsQueryParser create(ContentMapping contentMapping) {
        return new EsQueryParser(contentMapping);
    }



    public EsQuery parse(Set<AttributeQuery<?>> querySet) {
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
