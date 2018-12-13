package org.atlasapi.query;

import java.util.List;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.FuzzyQueryParams;
import org.atlasapi.content.InclusionExclusionId;
import org.atlasapi.criteria.AttributeQuery;
import org.atlasapi.criteria.FloatAttributeQuery;
import org.atlasapi.criteria.IdAttributeQuery;
import org.atlasapi.criteria.StringAttributeQuery;
import org.atlasapi.criteria.attribute.Attribute;
import org.atlasapi.criteria.attribute.IdAttribute;
import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.SubstitutionTableNumberCodec;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import static java.lang.String.format;
import static org.atlasapi.criteria.attribute.Attributes.ACTIONABLE_FILTER_PARAMETERS;
import static org.atlasapi.criteria.attribute.Attributes.BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.BROADCAST_WEIGHT;
import static org.atlasapi.criteria.attribute.Attributes.CHANNEL_GROUP_DTT_CHANNELS;
import static org.atlasapi.criteria.attribute.Attributes.CHANNEL_GROUP_IP_CHANNELS;
import static org.atlasapi.criteria.attribute.Attributes.EPISODE_BRAND_ID;
import static org.atlasapi.criteria.attribute.Attributes.ORDER_BY;
import static org.atlasapi.criteria.attribute.Attributes.PLATFORM;
import static org.atlasapi.criteria.attribute.Attributes.Q;
import static org.atlasapi.criteria.attribute.Attributes.REGION;
import static org.atlasapi.criteria.attribute.Attributes.SEARCH_TOPIC_ID;
import static org.atlasapi.criteria.attribute.Attributes.SERIES_ID;
import static org.atlasapi.criteria.attribute.Attributes.TITLE_BOOST;

/***
 * This is more or less a hack to allow parsing query params for ES. It should be removed and
 * replaced once a refactor of Deer's query parsing allows this sort of thing to be done easier
 * and in a more general fashion.
 */
public class IndexQueryParams {

    /**
     * These are attributes that are not supported by {@link org.atlasapi.util.EsQueryBuilder},
     * but for which we have custom support in the ES code.
     */
    public static final ImmutableSet<Attribute> SUPPORTED_ATTRIBUTES = ImmutableSet
            .<Attribute>builder()
            .add(Q)
            .add(TITLE_BOOST)
            .add(ORDER_BY)
            .add(REGION)
            .add(PLATFORM)
            .add(CHANNEL_GROUP_DTT_CHANNELS)
            .add(CHANNEL_GROUP_IP_CHANNELS)
            .add(BROADCAST_WEIGHT)
            .add(SEARCH_TOPIC_ID)
            .add(EPISODE_BRAND_ID)
            .add(BRAND_ID)
            .add(ACTIONABLE_FILTER_PARAMETERS)
            .add(SERIES_ID)
            .build();

    private static final SubstitutionTableNumberCodec codec =
            SubstitutionTableNumberCodec.lowerCaseOnly();

    private static final String NOT_OPERATOR = "!";
    private static final Splitter TOPIC_ID_SPLITTER = Splitter.on("^");
    private static final Splitter ACTIONABLE_FILTER_PARAM_SPLITTER = Splitter.on(":").limit(2);
    private static final int MAX_NUMBER_OF_IDS = 10;

    private final Optional<FuzzyQueryParams> fuzzyQueryParams;
    private final Optional<QueryOrdering> ordering;
    private final Optional<List<Id>> regionIds;
    private final Optional<List<Id>> platformIds;
    private final Optional<List<Id>> dttIds;
    private final Optional<List<Id>> ipIds;
    private final Optional<Float> broadcastWeighting;
    private final Optional<ImmutableList<ImmutableList<InclusionExclusionId>>> topicFilterIds;
    private final Optional<Id> brandId;
    private final Optional<ImmutableMap<String, String>> actionableFilterParams;
    private final Optional<Id> seriesId;

    private IndexQueryParams(Builder builder) {
        fuzzyQueryParams = Optional.ofNullable(builder.fuzzyQueryParams);
        ordering = Optional.ofNullable(builder.ordering);
        regionIds = Optional.ofNullable(builder.regionIds);
        platformIds = Optional.ofNullable(builder.platformIds);
        dttIds = Optional.ofNullable(builder.dttIds);
        ipIds = Optional.ofNullable(builder.ipIds);
        broadcastWeighting = Optional.ofNullable(builder.broadcastWeighting);
        topicFilterIds = Optional.ofNullable(builder.topicFilterIds);
        brandId = Optional.ofNullable(builder.brandId);
        actionableFilterParams = Optional.ofNullable(builder.actionableFilterParams);
        seriesId = Optional.ofNullable(builder.seriesId);
    }

    public static IndexQueryParams parse(Iterable<AttributeQuery<?>> queries) {
        ImmutableMap<String, AttributeQuery<?>> attributes = StreamSupport.stream(
                queries.spliterator(),
                false
        )
                .collect(MoreCollectors.toImmutableMap(
                        query -> query.getAttribute().externalName(),
                        query -> query
                ));

        Builder builder = new Builder();

        if (attributes.containsKey(Q.externalName())) {
            if (attributes.containsKey(TITLE_BOOST.externalName())) {
                parseTitleQuery(
                        builder,
                        (StringAttributeQuery) attributes.get(Q.externalName()),
                        (FloatAttributeQuery) attributes.get(TITLE_BOOST.externalName())
                );
            } else {
                parseTitleQuery(
                        builder,
                        (StringAttributeQuery) attributes.get(Q.externalName())
                );
            }
        }

        if (attributes.containsKey(ORDER_BY.externalName())) {
            parseOrderBy(
                    builder,
                    (StringAttributeQuery) attributes.get(ORDER_BY.externalName())
            );
        }

        if (attributes.containsKey(REGION.externalName())) {
            parseRegion(
                    builder,
                    (IdAttributeQuery) attributes.get(REGION.externalName())
            );
        }

        if (attributes.containsKey(PLATFORM.externalName())) {
            parsePlatform(
                    builder,
                    (IdAttributeQuery) attributes.get(PLATFORM.externalName())
            );
        }

        if (attributes.containsKey(CHANNEL_GROUP_DTT_CHANNELS.externalName())) {
            parseDttIds(
                    builder,
                    (IdAttributeQuery) attributes.get(CHANNEL_GROUP_DTT_CHANNELS.externalName())
            );
        }
        if (attributes.containsKey(CHANNEL_GROUP_IP_CHANNELS.externalName())) {
            parseIpIds(
                    builder,
                    (IdAttributeQuery) attributes.get(CHANNEL_GROUP_IP_CHANNELS.externalName())
            );
        }

        if (attributes.containsKey(BROADCAST_WEIGHT.externalName())) {
            parseBroadcastWeight(
                    builder,
                    (FloatAttributeQuery) attributes.get(BROADCAST_WEIGHT.externalName())
            );
        }

        // The instanceof check is because we have two topic ID attributes with the same name.
        // One which is parsed as a string and one as an ID. The one that is parsed as an ID
        // should never be passed here, but this is defensive code to ensure we don't accidentally
        // try to parse it if it does.
        if (attributes.containsKey(SEARCH_TOPIC_ID.externalName())
                && attributes.get(SEARCH_TOPIC_ID.externalName()) instanceof StringAttributeQuery) {
            parseTopicIds(
                    builder,
                    (StringAttributeQuery) attributes.get(SEARCH_TOPIC_ID.externalName())
            );
        }

        if (attributes.containsKey(EPISODE_BRAND_ID.externalName())) {
            parseBrandId(
                    builder,
                    (IdAttributeQuery) attributes.get(EPISODE_BRAND_ID.externalName())
            );
        } else if (attributes.containsKey(BRAND_ID.externalName())) {
            parseBrandId(
                    builder,
                    (IdAttributeQuery) attributes.get(BRAND_ID.externalName())
            );
        }

        if (attributes.containsKey(ACTIONABLE_FILTER_PARAMETERS.externalName())) {
            parseActionableFilterParams(
                    builder,
                    (StringAttributeQuery) attributes.get(
                            ACTIONABLE_FILTER_PARAMETERS.externalName()
                    )
            );
        }

        if (attributes.containsKey(SERIES_ID.externalName())) {
            parseSeriesId(
                    builder,
                    (IdAttributeQuery) attributes.get(SERIES_ID.externalName())
            );
        }

        return builder.build();
    }

    private static void parseTitleQuery(
            Builder builder,
            StringAttributeQuery titleAttributeQuery
    ) {
        extractOnlyValue(titleAttributeQuery).ifPresent(
                searchTerm -> builder.withFuzzyQueryParams(
                        new FuzzyQueryParams(
                                searchTerm,
                                Optional.empty()
                        )
                )
        );
    }

    private static void parseTitleQuery(
            Builder builder,
            StringAttributeQuery titleAttributeQuery,
            FloatAttributeQuery titleBoostAttributeQuery
    ) {
        extractOnlyValue(titleAttributeQuery).ifPresent(
                searchTerm -> builder.withFuzzyQueryParams(
                        new FuzzyQueryParams(
                                searchTerm,
                                extractOnlyValue(titleBoostAttributeQuery)
                        )
                )
        );
    }

    private static void parseOrderBy(
            Builder builder,
            StringAttributeQuery orderByQuery
    ) {
        if (orderByQuery.getValue().isEmpty()) {
            return;
        }

        builder.withOrdering(
                QueryOrdering.fromOrderBy(orderByQuery.getValue())
        );
    }

    private static void parseRegion(
            Builder builder,
            IdAttributeQuery regionQuery
    ) {
        Optional<List<Id>> regionIds = extractListValues(regionQuery);
        regionIds.ifPresent(
                ids -> {
                    validateNumberOfQueryIds(regionQuery, ids);
                    builder.withRegionIds(ids);
                }
        );

    }

    private static void parsePlatform(
            Builder builder,
            IdAttributeQuery platformQuery
    ) {
        Optional<List<Id>> platformIds = extractListValues(platformQuery);
        platformIds.ifPresent(
                ids -> {
                    validateNumberOfQueryIds(platformQuery, ids);
                    builder.withPlatformIds(ids);
                }
        );
    }

    // We currently allow a maximum of 10 IDs to be searched for per channel group type as a default
    // precaution to avoid any potential performance issues when querying ES. No testing has been
    // carried in order to confirm that, so make sure to test it properly before increasing it.
    private static void validateNumberOfQueryIds(
            IdAttributeQuery platformQuery,
            List<Id> ids
    ) {
        if (ids.size() > MAX_NUMBER_OF_IDS) {
            throw new IllegalArgumentException(format(
                    "You cannot query more than 10 IDs for param %s",
                    platformQuery.getAttributeName()
            ));
        }
    }

    private static void parseDttIds(
            Builder builder,
            IdAttributeQuery dttIdsQuery
    ) {
        Optional<List<Id>> dttIds = extractListValues(dttIdsQuery);
        dttIds.ifPresent(
                ids -> {
                    validateNumberOfQueryIds(dttIdsQuery, ids);
                    builder.withDttIds(ids);
                }
        );
    }

    private static void parseIpIds(
            Builder builder,
            IdAttributeQuery ipIdsQuery
    ) {
        Optional<List<Id>> ipIds = extractListValues(ipIdsQuery);
        ipIds.ifPresent(
                ids -> {
                    validateNumberOfQueryIds(ipIdsQuery, ids);
                    builder.withIpIds(ids);
                }
        );
    }

    private static void parseBroadcastWeight(
            Builder builder,
            FloatAttributeQuery broadcastWeightQuery
    ) {
        extractFirstValue(broadcastWeightQuery).ifPresent(
                builder::withBroadcastWeighting
        );
    }

    private static void parseTopicIds(
            Builder builder,
            StringAttributeQuery topicIdsQuery
    ) {
        if (topicIdsQuery.getValue().isEmpty()) {
            return;
        }

        ImmutableList<ImmutableList<InclusionExclusionId>> parsedIds = topicIdsQuery.getValue()
                .stream()
                .map(topicIds ->
                        TOPIC_ID_SPLITTER.splitToList(topicIds)
                                .stream()
                                .map(IndexQueryParams::parseInclusiveExclusiveId)
                                .collect(MoreCollectors.toImmutableList())
                )
                .collect(MoreCollectors.toImmutableList());

        builder.withTopicFilterIds(parsedIds);
    }

    private static InclusionExclusionId parseInclusiveExclusiveId(String id) {
        if (!Strings.isNullOrEmpty(id) && id.startsWith(NOT_OPERATOR)) {
            return InclusionExclusionId.valueOf(
                    Id.valueOf(codec.decode(id.substring(NOT_OPERATOR.length()))),
                    Boolean.FALSE
            );
        }
        return InclusionExclusionId.valueOf(Id.valueOf(codec.decode(id)), Boolean.TRUE);
    }

    private static void parseBrandId(
            Builder builder,
            IdAttributeQuery brandIdQuery
    ) {
        extractFirstValue(brandIdQuery).ifPresent(
                builder::withBrandId
        );
    }

    private static void parseActionableFilterParams(
            Builder builder,
            StringAttributeQuery actionableFilterQuery
    ) {
        if (actionableFilterQuery.getValue().isEmpty()) {
            return;
        }

        ImmutableMap<String, String> actionableParams = actionableFilterQuery.getValue()
                .stream()
                .map(ACTIONABLE_FILTER_PARAM_SPLITTER::splitToList)
                .collect(MoreCollectors.toImmutableMap(
                        splitParam -> splitParam.get(0),
                        splitParam -> splitParam.get(1)
                ));

        builder.withActionableFilterParams(actionableParams);
    }

    private static void parseSeriesId(
            Builder builder,
            IdAttributeQuery seriesIdQuery
    ) {
        extractFirstValue(seriesIdQuery).ifPresent(
                builder::withSeriesId
        );
    }

    private static <T> Optional<T> extractOnlyValue(
            AttributeQuery<T> query
    ) {
        List<T> values = query.getValue();

        if (values.isEmpty()) {
            return Optional.empty();
        }

        if (values.size() > 1) {
            throw new IllegalArgumentException(format(
                    "More than one value has been specified for param %s",
                    query.getAttributeName()
            ));
        }

        return Optional.of(values.get(0));
    }

    /**
     * This is by design more permissive than
     * {@link IndexQueryParams#extractOnlyValue(AttributeQuery)} in that it will not fail if
     * multiple parameters are passed to it, but rather silently grab the first one.
     *
     * This is to preserve API backwards compatibility with existing code and ensure we don't
     * fail any calls that might have been accidentally doing this until now.
     */
    private static <T> Optional<T> extractFirstValue(
            AttributeQuery<T> query
    ) {
        List<T> values = query.getValue();

        if (values.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(values.get(0));
    }

    private static <T> Optional<List<T>> extractListValues(
            AttributeQuery<T> query
    ) {
        List<T> values = query.getValue();

        if (values.isEmpty()) {
            return Optional.empty();
        }

        return Optional.of(values);
    }

    public Optional<Id> getBrandId() {
        return brandId;
    }

    public Optional<FuzzyQueryParams> getFuzzyQueryParams() {
        return fuzzyQueryParams;
    }

    public Optional<QueryOrdering> getOrdering() {
        return ordering;
    }

    public Optional<List<Id>> getRegionIds() {
        return regionIds;
    }

    public Optional<List<Id>> getPlatformIds() {
        return platformIds;
    }

    public Optional<List<Id>> getDttIds() {
        return dttIds;
    }

    public Optional<List<Id>> getIpIds() {
        return ipIds;
    }

    public Optional<Float> getBroadcastWeighting() {
        return broadcastWeighting;
    }

    public Optional<ImmutableList<ImmutableList<InclusionExclusionId>>> getTopicFilterIds() {
        return topicFilterIds;
    }

    public Optional<ImmutableMap<String, String>> getActionableFilterParams() {
        return actionableFilterParams;
    }

    public Optional<Id> getSeriesId() {
        return seriesId;
    }

    public static final class Builder {

        private FuzzyQueryParams fuzzyQueryParams;
        private QueryOrdering ordering;
        private List<Id> regionIds;
        private List<Id> platformIds;
        private List<Id> dttIds;
        private List<Id> ipIds;
        private Float broadcastWeighting;
        private ImmutableList<ImmutableList<InclusionExclusionId>> topicFilterIds;
        private Id brandId;
        private ImmutableMap<String, String> actionableFilterParams;
        private Id seriesId;

        private Builder() {
        }

        public Builder withFuzzyQueryParams(FuzzyQueryParams fuzzyQueryParams) {
            this.fuzzyQueryParams = fuzzyQueryParams;
            return this;
        }

        public Builder withOrdering(QueryOrdering ordering) {
            this.ordering = ordering;
            return this;
        }

        public Builder withRegionIds(List<Id> regionIds) {
            this.regionIds = regionIds;
            return this;
        }

        public Builder withPlatformIds(List<Id> platformIds) {
            this.platformIds = platformIds;
            return this;
        }

        public Builder withDttIds(List<Id> dttIds) {
            this.dttIds = dttIds;
            return this;
        }

        public Builder withIpIds(List<Id> ipIds) {
            this.ipIds = ipIds;
            return this;
        }

        public Builder withBroadcastWeighting(Float broadcastWeighting) {
            this.broadcastWeighting = broadcastWeighting;
            return this;
        }

        public Builder withTopicFilterIds(
                ImmutableList<ImmutableList<InclusionExclusionId>> topicFilterIds
        ) {
            this.topicFilterIds = topicFilterIds;
            return this;
        }

        public Builder withBrandId(Id brandId) {
            this.brandId = brandId;
            return this;
        }

        public Builder withActionableFilterParams(
                ImmutableMap<String, String> actionableFilterParams
        ) {
            this.actionableFilterParams = actionableFilterParams;
            return this;
        }

        public Builder withSeriesId(Id seriesId) {
            this.seriesId = seriesId;
            return this;
        }

        public IndexQueryParams build() {
            return new IndexQueryParams(this);
        }
    }
}
