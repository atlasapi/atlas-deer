package org.atlasapi.content;

import java.util.concurrent.TimeUnit;

import org.elasticsearch.index.query.FilterBuilders;
import org.elasticsearch.index.query.QueryBuilder;
import org.elasticsearch.index.query.QueryBuilders;
import org.elasticsearch.index.query.functionscore.ScoreFunctionBuilders;
import org.joda.time.DateTime;

import com.google.common.collect.ImmutableMap;

public class BroadcastQueryBuilder {

    public static final String SCRIPT = ""
            + "if (_source.broadcasts != null) {"
            + "  now = System.currentTimeMillis()"
            + "  t = java.lang.Long.MAX_VALUE"
            + "  f = 1"
            + "  for (b in _source.broadcasts) {"
            + "    candidate = (now - b.transmissionTimeInMillis).abs()"
            + "    if (candidate < t) t = candidate"
            + "    if (b.repeat = false) f = firstBroadcastBoost"
            + "  }"
            + "  g = 1"
            + "  if (t < 604800000) {"
            + "    g = 50"
            + "  }"
            + "  _score + (_score * f * timeBoost * (1 / (1 + (t / g))))"
            + "} else { _score }";

    public static final String SCRIPT_LANG = "groovy";

    public static QueryBuilder build(QueryBuilder childQuery, Float timeBoost,
            Float firstBroadcastBoost) {
        return QueryBuilders.functionScoreQuery(
                QueryBuilders.filteredQuery(childQuery,
                        FilterBuilders.nestedFilter(EsContent.BROADCASTS,
                                FilterBuilders.rangeFilter(EsBroadcast.TRANSMISSION_TIME)
                                        .from(new DateTime().minusDays(30))
                                        .to(new DateTime().plusDays(30)))))
                .add(ScoreFunctionBuilders.scriptFunction(SCRIPT, SCRIPT_LANG, ImmutableMap.of(
                        "firstBroadcastBoost", firstBroadcastBoost,
                        "timeBoost", timeBoost,
                        "oneWeek", TimeUnit.MILLISECONDS.convert(7, TimeUnit.DAYS)
                )));
    }
}
