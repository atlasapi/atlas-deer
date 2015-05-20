package org.atlasapi.content;

import java.util.Date;

import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;

public class AvailabilityQueryBuilder {

    public static QueryBuilder build(Date when, float boost) {
        return QueryBuilders.nestedQuery(EsContent.LOCATIONS,
                QueryBuilders.boolQuery()
                        .boost(boost)
                        .must(QueryBuilders.rangeQuery(EsLocation.AVAILABILITY_TIME).gte(when))
                        .must(QueryBuilders.rangeQuery(EsLocation.AVAILABILITY_END_TIME).lt(when))
        );
    }
}
