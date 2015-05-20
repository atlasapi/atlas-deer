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

    public static final String SCRIPT = ""
            + "if (_source.broadcasts != null) {"
            + "  now = time();"
            + "  t = Long.MAX_VALUE;"
            + "  f = 1;"
            + "  foreach (b : _source.broadcasts) {"
            + "    candidate = abs(now - b.transmissionTimeInMillis);"
            + "    if (candidate < t) t = candidate;"
            + "    if (b.repeat = false) f = firstBroadcastBoost;"
            + "  }"
            + "  _score + (_score * f * timeBoost * (1 / (1 + (t / (t < oneWeek ? 50 : 1)))));"
            + "} else _score;";

    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost, Float firstBroadcastBoost) {
        return QueryBuilders.functionScoreQuery(
                QueryBuilders.filteredQuery(childQuery,
                        FilterBuilders.nestedFilter(EsContent.BROADCASTS,
                                FilterBuilders.rangeFilter(EsBroadcast.TRANSMISSION_TIME).from(new DateTime().minusDays(30)).to(new DateTime().plusDays(30)))))
                .add(ScoreFunctionBuilders.scriptFunction(SCRIPT, ImmutableMap.of(
                        "firstBroadcastBoost", firstBroadcastBoost,
                        "timeBoost", timeBoost,
                        "oneWeek", TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)
                )));
    }
}
