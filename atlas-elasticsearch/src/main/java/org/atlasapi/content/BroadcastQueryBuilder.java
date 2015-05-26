package org.atlasapi.content;

import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableMap;
import org.elasticsearch.common.xcontent.ToXContent;
import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilder;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.joda.time.DateTime;

public class BroadcastQueryBuilder {

    public static final String SCRIPT = "score_script";
    public static final String SCRIPT_LANG = "groovy";

    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost, Float firstBroadcastBoost) {
        return QueryBuilders.functionScoreQuery(
                QueryBuilders.filteredQuery(childQuery,
                        FilterBuilders.nestedFilter(EsContent.BROADCASTS,
                                FilterBuilders.rangeFilter(EsBroadcast.TRANSMISSION_TIME).from(new DateTime().minusDays(30)).to(new DateTime().plusDays(30)))))
                .add(ScoreFunctionBuilders.scriptFunction(SCRIPT, SCRIPT_LANG, ImmutableMap.of(
                        "firstBroadcastBoost", firstBroadcastBoost,
                        "timeBoost", timeBoost,
                        "oneWeek", TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)
                )));
    }
}
