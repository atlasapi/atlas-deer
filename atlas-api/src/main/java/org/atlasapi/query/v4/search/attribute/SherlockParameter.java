package org.atlasapi.query.v4.search.attribute;

import com.metabroadcast.common.stream.MoreCollectors;

import java.util.Arrays;
import java.util.Set;

public enum SherlockParameter {

    // Content
    TITLE(Type.SEARCH, "title"),
    DESCRIPTION(Type.SEARCH, "description"),
    MEDIA_TYPE(Type.FILTER, "media_type"),
    SPECIALIZATION(Type.FILTER, "specialization"),
    GENRES(Type.FILTER, "genre"),
    TYPE(Type.FILTER, "type"),
    EPISODE_NUMBER(Type.FILTER, "episode_number"),
    SERIES_NUMBER(Type.FILTER, "series_number"),
    TOTAL_EPISODES(Type.FILTER, "total_episodes"),
    COUNTRIES_OF_ORIGIN(Type.FILTER, "country_of_origin"),
    YEAR(Type.FILTER, "year"),
    TOP_LEVEL(Type.FILTER, "top_level"),
    SOURCE(Type.FILTER, "source"),
    SOURCE_COUNTRY(Type.FILTER, "source.country"),
    LANGUAGES_CODE(Type.FILTER, "language"),

    // Alias
    ALIASES_VALUE(Type.FILTER, Group.ALIASES, "value"),
    ALIASES_NAMESPACE(Type.FILTER, Group.ALIASES, "namespace"),

    // Restrictions
    RESTRICTIONS_AUTHORITY(Type.FILTER, Group.RESTRICTIONS, "authority"),
    RESTRICTIONS_RATING(Type.FILTER, Group.RESTRICTIONS, "rating"),
    RESTRICTIONS_MINIMUM_AGE(Type.FILTER, Group.RESTRICTIONS, "minimum_age"),

    // Certificates
    CERTIFICATES_CLASSIFICATION(Type.FILTER, Group.CERTIFICATES, "classification"),
    CERTIFICATES_CODE(Type.FILTER, Group.CERTIFICATES, "code"),

    // Release dates
    RELEASE_DATE(Type.FILTER, "release_date"),
    RELEASE_DATES_COUNTRY(Type.FILTER, Group.RELEASE_DATES, "country"),
    RELEASE_DATES_TYPE(Type.FILTER, Group.RELEASE_DATES, "type"),

    // Broadcast / Schedule
    BROADCASTS_UPCOMING(Type.FILTER, Group.BROADCASTS, "upcoming"),
    BROADCASTS_START_TIME(Type.FILTER, Group.BROADCASTS, "start_time"),
    BROADCASTS_END_TIME(Type.FILTER, Group.BROADCASTS, "end_time"),
    BROADCASTS_DURATION(Type.FILTER, Group.BROADCASTS, "duration"),
    BROADCASTS_CHANNEL(Type.FILTER, Group.BROADCASTS, "channel"),
    BROADCASTS_CHANNEL_GROUP(Type.FILTER, Group.BROADCASTS, "channel_group"),

    // People
    PEOPLE_URI(Type.FILTER, Group.PEOPLE, "uri"),
    PEOPLE_CURIE(Type.FILTER, Group.PEOPLE, "curie"),
    PEOPLE_TYPE(Type.FILTER, Group.PEOPLE, "type"),
    PEOPLE_NAME(Type.SEARCH, Group.PEOPLE, "name"),
    PEOPLE_NAME_EXACT(Type.FILTER, Group.PEOPLE, "name"),
    PEOPLE_ROLE(Type.FILTER, Group.PEOPLE, "role"),
    PEOPLE_CHARACTER(Type.SEARCH, Group.PEOPLE, "character"),
    PEOPLE_CHARACTER_EXACT(Type.FILTER, Group.PEOPLE, "character"),

    // Locations
    LOCATIONS_AVAILABLE(Type.FILTER, Group.LOCATIONS, "available"),
    LOCATIONS_AVAILABILITY_START(Type.FILTER, Group.LOCATIONS, "availability_start"),
    LOCATIONS_AVAILABILITY_END(Type.FILTER, Group.LOCATIONS, "availability_end"),
    LOCATIONS_AVAILABILITY_COUNTRIES(Type.FILTER, Group.LOCATIONS, "availability_country"),
    LOCATIONS_REVENUE_CONTRACT(Type.FILTER, Group.LOCATIONS, "revenue_contract"),
    LOCATIONS_SUBSCRIPTION_PACKAGES(Type.FILTER, Group.LOCATIONS, "subscription_packages"),
    LOCATIONS_SOURCE(Type.FILTER, Group.LOCATIONS, "source"),
    LOCATIONS_SOURCE_COUNTRY(Type.FILTER, Group.LOCATIONS, "source.country"),
    LOCATIONS_DISTRIBUTOR(Type.FILTER, Group.LOCATIONS, "distributor"),
    LOCATIONS_PROVIDER_NAME(Type.FILTER, Group.LOCATIONS, "provider_name"),

    // Data
    DATA_BIT_RATE(Type.FILTER, Group.DATA, "bit_rate"),
    DATA_SIZE(Type.FILTER, Group.DATA, "size"),
    DATA_CONTAINER_FORMAT(Type.FILTER, Group.DATA, "container_format"),

    // Audio
    AUDIO_BIT_RATE(Type.FILTER, Group.AUDIO, "bit_rate"),
    AUDIO_CODING(Type.FILTER, Group.AUDIO, "coding"),

    // Video
    VIDEO_DURATION(Type.FILTER, Group.VIDEO, "duration"),
    VIDEO_ASPECT_RATIO(Type.FILTER, Group.VIDEO, "aspect_ratio"),
    VIDEO_BIT_RATE(Type.FILTER, Group.VIDEO, "bit_rate"),
    VIDEO_CODING(Type.FILTER, Group.VIDEO, "coding"),
    VIDEO_FRAME_RATE(Type.FILTER, Group.VIDEO, "frame_rate"),
    VIDEO_HORIZONTAL_SIZE(Type.FILTER, Group.VIDEO, "horizontal_size"),
    VIDEO_PROGRESSIVE_SCAN(Type.FILTER, Group.VIDEO, "progressive_scan"),
    VIDEO_VERTICAL_SIZE(Type.FILTER, Group.VIDEO, "vertical_size"),
    VIDEO_QUALITY(Type.FILTER, Group.VIDEO, "quality"),
    VIDEO_IS_3D(Type.FILTER, Group.VIDEO, "is_3d"),
    VIDEO_HAS_DOG(Type.FILTER, Group.VIDEO, "has_dog"),

    // Pricing
    PRICING_CURRENCY(Type.FILTER, Group.PRICING, "currency"),
    PRICING_AMOUNT(Type.FILTER, Group.PRICING, "amount"),
    PRICING_START_TIME(Type.FILTER, Group.PRICING, "start_time"),
    PRICING_END_TIME(Type.FILTER, Group.PRICING, "end_time"),

    // Container
    CONTAINER_ID(Type.FILTER, Group.CONTAINER, "id"),
    CONTAINER_TITLE(Type.SEARCH, Group.CONTAINER, "title"),
    CONTAINER_DESCRIPTION(Type.SEARCH, Group.CONTAINER, "description"),
    CONTAINER_TYPE(Type.FILTER, Group.CONTAINER, "type"),

    // Series
    SERIES_ID(Type.FILTER, Group.SERIES, "id"),
    SERIES_TITLE(Type.SEARCH, Group.SERIES, "title"),
    SERIES_DESCRIPTION(Type.SEARCH, Group.SERIES, "description"),
    SERIES_SERIES_NUMBER(Type.FILTER, Group.SERIES, "series_number"),

    // Children
    CHILDREN_ID(Type.FILTER, Group.CHILDREN, "id"),
    CHILDREN_TITLE(Type.SEARCH, Group.CHILDREN, "title"),
    CHILDREN_DESCRIPTION(Type.SEARCH, Group.CHILDREN, "description"),
    CHILDREN_EPISODE_NUMBER(Type.FILTER, Group.CHILDREN, "episode_number"),

    // Awards
    AWARDS_OUTCOME(Type.FILTER, Group.AWARDS, "outcome"),
    AWARDS_TITLE(Type.FILTER, Group.AWARDS, "title"),
    AWARDS_DESCRIPTION(Type.FILTER, Group.AWARDS, "description"),
    AWARDS_YEAR(Type.FILTER, Group.AWARDS, "year"),

    // Ratings
    RATINGS_VALUE(Type.FILTER, Group.RATINGS, "value"),
    RATINGS_TYPE(Type.FILTER, Group.RATINGS, "type"),
    RATINGS_SOURCE_KEY(Type.FILTER, Group.RATINGS, "source.key"),
    RATINGS_SOURCE_COUNTRY(Type.FILTER, Group.RATINGS, "source.country"),

    // Reviews
    REVIEWS_REVIEW(Type.SEARCH, Group.REVIEWS, "review"),
    REVIEWS_LANGUAGE(Type.FILTER, Group.REVIEWS, "language"),
    REVIEWS_AUTHOR(Type.SEARCH, Group.REVIEWS, "author"),
    REVIEWS_AUTHOR_EXACT(Type.FILTER, Group.REVIEWS, "author"),
    REVIEWS_AUTHOR_INITIALS(Type.FILTER, Group.REVIEWS, "author_initials"),
    REVIEWS_RATING(Type.FILTER, Group.REVIEWS, "rating"),
    REVIEWS_DATE(Type.FILTER, Group.REVIEWS, "date"),
    REVIEWS_REVIEW_TYPE(Type.FILTER, Group.REVIEWS, "review_type"),
    REVIEWS_SOURCE_KEY(Type.FILTER, Group.REVIEWS, "source.key"),
    REVIEWS_SOURCE_COUNTRY(Type.FILTER, Group.REVIEWS, "source.country"),
    ;

    private final Type type;
    private final Group group;
    private final String name;

    SherlockParameter(Type type, String name) {
        this.type = type;
        this.name = name;
        this.group = null;
    }

    SherlockParameter(Type type, Group group, String name) {
        this.type = type;
        this.group = group;
        this.name = name;
    }

    public Type getType() {
        return type;
    }

    public String getParameterName() {
        if (group == null) {
            return String.join(
                    ".",
                    type.name().toLowerCase(),
                    name);
        } else {
            return String.join(
                    ".",
                    type.name().toLowerCase(),
                    group.name().toLowerCase(),
                    name);
        }
    }

    public static Set<String> getAllNames() {
        return Arrays.stream(SherlockParameter.values())
                .map(SherlockParameter::getParameterName)
                .collect(MoreCollectors.toImmutableSet());
    }

    public enum Type {
        FILTER,
        SEARCH,
    }

    public enum Group {
        ALIASES,
        RESTRICTIONS,
        CERTIFICATES,
        RELEASE_DATES,
        BROADCASTS,
        PEOPLE,
        LOCATIONS,
        DATA,
        AUDIO,
        VIDEO,
        PRICING,
        CONTAINER,
        SERIES,
        CHILDREN,
        AWARDS,
        RATINGS,
        REVIEWS,
    }
}