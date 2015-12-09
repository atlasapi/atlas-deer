package org.atlasapi.elasticsearch.client;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.util.ImmutableCollectors;
import org.elasticsearch.ElasticsearchException;
import org.elasticsearch.action.search.SearchRequest;
import org.elasticsearch.action.search.ShardSearchFailure;
import org.elasticsearch.common.text.StringText;
import org.elasticsearch.common.unit.TimeValue;
import org.elasticsearch.rest.RestStatus;
import org.elasticsearch.search.SearchHitField;
import org.elasticsearch.search.SearchHits;
import org.elasticsearch.search.aggregations.Aggregations;
import org.elasticsearch.search.facet.Facets;
import org.elasticsearch.search.internal.InternalSearchHit;
import org.elasticsearch.search.internal.InternalSearchHitField;
import org.elasticsearch.search.internal.InternalSearchHits;
import org.elasticsearch.search.internal.InternalSearchResponse;
import org.elasticsearch.search.suggest.Suggest;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.common.collect.ImmutableList;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonPrimitive;

import io.searchbox.core.SearchResult;

class FakeInternalSearchResponse extends InternalSearchResponse {

    private static final Logger log = LoggerFactory.getLogger(FakeInternalSearchResponse.class);

    private final JsonObject responseJson;
    private final SearchResult response;

    public FakeInternalSearchResponse(SearchResult response) {
        super(InternalSearchHits.empty(), null, null, null, false, null);
        this.response = response;
        this.responseJson = response.getJsonObject();
    }

    private Optional<JsonElement> getResultElementByPath(String path) {
        JsonElement ptr = responseJson;
        for (String part: path.split("/")) {
            if (null == ptr) {
                return Optional.empty();
            }

            ptr = ((JsonObject) ptr).get(part);
        }

        return Optional.of(ptr);
    }

    public int getTotalShards() {
        return getResultElementByPath("_shards/total")
                .flatMap(o -> Optional.of(o.getAsInt()))
                .orElse(-1);
    }

    public long duration() {
        return responseJson.get("took").getAsLong();
    }

    public RestStatus status() {
        return RestStatus.status(getSuccessfulShards(), getTotalShards(), getShardFailures());
    }

    /**
     * The search hits.
     */
    @Override
    public SearchHits hits() {
        List<InternalSearchHit> hits = StreamSupport.stream(
                getResultElementByPath("hits/hits").get().getAsJsonArray().spliterator(), false
        ).map(obj -> {
            JsonObject jsonObject = obj.getAsJsonObject();
            return new InternalSearchHit(
                    Integer.parseInt(jsonObject.get("_id").getAsString()),
                    jsonObject.get("_id").getAsString(),
                    new StringText(jsonObject.get("_type").getAsString()),
                    (Map<String, SearchHitField>) convertToFields(jsonObject.get("fields"))
            );
        }).collect(Collectors.toList());

        Optional<JsonElement> maxScoreOptional = getResultElementByPath("hits/max_score");
        float maxScore = -1.0f;
        if (maxScoreOptional.isPresent()) {
            JsonElement elem = maxScoreOptional.get();
            if (!elem.isJsonNull()) {
                maxScore = elem.getAsNumber().floatValue();
            }
        }
        return new InternalSearchHits(
                hits.toArray(new InternalSearchHit[hits.size()]),
                response.getTotal(),
                maxScore
        );
    }

    private static Object convertToFields(JsonElement elem) {
        if (elem == null) {
            return null;
        } else if (elem.isJsonObject()) {
            JsonObject obj = elem.getAsJsonObject();
            return obj.entrySet().stream().collect(Collectors.toMap(
                    Map.Entry::getKey,
                    e -> new InternalSearchHitField(e.getKey(), ImmutableList.of(convertToFields(e.getValue())))
            ));
        } else if (elem.isJsonArray()) {
            return StreamSupport.stream(elem.getAsJsonArray().spliterator(), false)
                    .map(FakeInternalSearchResponse::convertToFields)
                    .collect(ImmutableCollectors.toList());
        } else if (elem.isJsonNull()) {
            return null;
        } else if (elem.isJsonPrimitive()) {
            JsonPrimitive primitive = elem.getAsJsonPrimitive();
            if (primitive.isBoolean()) {
                return primitive.getAsBoolean();
            } else if (primitive.isString()) {
                return primitive.getAsString();
            } else if (primitive.isNumber()) {
                Number number = primitive.getAsNumber();
                if (number instanceof Integer) {
                    return number.intValue();
                } else if (number instanceof Float) {
                    return number.floatValue();
                } else if (number instanceof Double) {
                    return number.doubleValue();
                } else {
                    // TODO: hax, we're defaulting to Long because GSON has a concept of LazyNumber
                    return number.longValue();
                }
            }
        }

        throw new IllegalArgumentException("should never get here " + elem.toString());
    }

    /**
     * The search facets.
     */
    @Override
    public Facets facets() {
        log.warn("getFacets is not implemented and only returns a dummy");
        return null;
    }

    @Override
    public Aggregations aggregations() {
        log.warn("getAggregations is not implemented and only returns a dummy");
        return null;
    }

    @Override
    public Suggest suggest() {
        log.warn("getSuggest is not implemented and only returns a dummy");
        return null;
    }

    /**
     * Has the search operation timed out.
     */
    @Override
    public boolean timedOut() {
        return responseJson.get("timed_out").getAsBoolean();
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    @Override
    public Boolean terminatedEarly() {
        log.warn("isTerminatedEarly is not implemented and only returns a dummy");
        return Boolean.FALSE;
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(getTookInMillis());
    }

    /**
     * How long the search took in milliseconds.
     */
    public long getTookInMillis() {
        return responseJson.get("took").getAsLong();
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return getResultElementByPath("_shards/successful")
                .flatMap(o -> Optional.of(o.getAsInt()))
                .orElse(-1);
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        return getResultElementByPath("_shards/failed")
                .flatMap(o -> Optional.of(o.getAsInt()))
                .orElse(-1);
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        log.warn("getShardFailures is not implemented and only returns a dummy");
        return new ShardSearchFailure[0];
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.elasticsearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        log.warn("getScrollId is not implemented and only returns a dummy");
        return "nope";
    }

    public void scrollId(String scrollId) {
        throw new ElasticsearchException("not actually implemented");
    }
}
