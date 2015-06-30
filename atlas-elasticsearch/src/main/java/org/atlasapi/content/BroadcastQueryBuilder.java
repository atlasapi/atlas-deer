package org.atlasapi.content;

import com.metabroadcast.common.time.DateTimeZones;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.joda.time.DateTime;

import java.util.Date;

public class BroadcastQueryBuilder {


    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost) {
        Date minusThirtyDays = DateTime.now().minusDays(30).toDateTime(DateTimeZones.UTC).toDate();
        Date plusThirtyDays = DateTime.now().plusDays(30).toDateTime(DateTimeZones.UTC).toDate();

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
        ).add(ScoreFunctionBuilders.gaussDecayFunction(
                        EsBroadcast.TRANSMISSION_TIME_IN_MILLIS,
                        new Date().toInstant().getEpochSecond() * 1000,
                        minusThirtyDays.toInstant().getEpochSecond() * 1000
                )
        ).boost(timeBoost);
    }
}
