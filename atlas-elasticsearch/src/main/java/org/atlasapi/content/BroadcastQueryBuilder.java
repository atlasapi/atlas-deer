package org.atlasapi.content;

import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.joda.time.DateTime;

public class BroadcastQueryBuilder {


    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost, Float firstBroadcastBoost) {
        DateTime minusThirtyDays = DateTime.now().minusDays(30);
        DateTime plusThirtyDays = DateTime.now().plusDays(30);

        return QueryBuilders.functionScoreQuery(
                QueryBuilders.filteredQuery(
                        childQuery,
                        FilterBuilders.nestedFilter(
                                EsContent.BROADCASTS,
                                FilterBuilders.rangeFilter(EsBroadcast.TRANSMISSION_TIME)
                                        .from(plusThirtyDays)
                                        .to(minusThirtyDays)
                        )
                )
        ).add(ScoreFunctionBuilders.gaussDecayFunction(EsBroadcast.TRANSMISSION_TIME_IN_MILLIS, DateTime.now(), minusThirtyDays)).boost(timeBoost);
    }
}
