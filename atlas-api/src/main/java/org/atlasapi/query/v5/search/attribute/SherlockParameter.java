package org.atlasapi.query.v5.search.attribute;

import java.util.Arrays;
import java.util.Set;
import java.util.stream.Collectors;

public enum SherlockParameter {

    // Content
    TITLE(ParameterType.SEARCH, "title"),
    TITLE_EXACT(ParameterType.FILTER, "title"),
    DESCRIPTION(ParameterType.SEARCH, "description"),
    MEDIA_TYPE(ParameterType.FILTER, "media_type"),
    SPECIALIZATION(ParameterType.FILTER, "specialization"),
    GENRES(ParameterType.FILTER, "genre"),
    TYPE(ParameterType.FILTER, "type"),
    EPISODE_NUMBER(ParameterType.FILTER, "episode_number"),
    SERIES_NUMBER(ParameterType.FILTER, "series_number"),
    TOTAL_EPISODES(ParameterType.FILTER, "total_episodes"),
    COUNTRIES_OF_ORIGIN(ParameterType.FILTER, "country_of_origin"),
    YEAR(ParameterType.FILTER, "year"),
    TOP_LEVEL(ParameterType.FILTER, "top_level"),
    PUBLISHER(ParameterType.FILTER, "publisher"),
    PUBLISHER_COUNTRY(ParameterType.FILTER, "publisher_country"),

    // Alias
    ALIASES_VALUE(ParameterType.FILTER, "aliases.value"),
    ALIASES_NAMESPACE(ParameterType.FILTER, "aliases.namespace"),

    // Restrictions
    RESTRICTIONS_AUTHORITY(ParameterType.FILTER, "restrictions.authority"),
    RESTRICTIONS_RATING(ParameterType.FILTER, "restrictions.rating"),
    RESTRICTIONS_MINIMUM_AGE(ParameterType.FILTER, "restrictions.minimum_age"),
    RESTRICTIONS_MESSAGE(ParameterType.SEARCH, "restrictions.message"),

    // Certificates
    CERTIFICATES_CLASSIFICATION(ParameterType.FILTER, "certificates.classification"),
    CERTIFICATES_CODE(ParameterType.FILTER, "certificates.code"),

    // Languages
    LANGUAGES_CODE(ParameterType.FILTER, "languages"),

    // Release dates
    RELEASE_DATES(ParameterType.FILTER, "release_date"),
    RELEASE_DATES_COUNTRY(ParameterType.FILTER, "release_dates.country"),
    RELEASE_DATES_TYPE(ParameterType.FILTER, "release_dates.type"),

    // Broadcast / Schedule
    SCHEDULE_UPCOMING(ParameterType.FILTER, "schedule.upcoming"),
    SCHEDULE_START_TIME(ParameterType.FILTER, "schedule.start_time"),
    SCHEDULE_END_TIME(ParameterType.FILTER, "schedule.end_time"),
    SCHEDULE_DURATION(ParameterType.FILTER, "schedule.duration"),
    SCHEDULE_CHANNEL(ParameterType.FILTER, "schedule.channel"),
    SCHEDULE_CHANNEL_GROUP(ParameterType.FILTER, "schedule.channelGroup"),

    // People
    PEOPLE_URI(ParameterType.FILTER, "people.uri"),
    PEOPLE_CURIE(ParameterType.FILTER, "people.curie"),
    PEOPLE_TYPE(ParameterType.FILTER, "people.type"),
    PEOPLE_NAME(ParameterType.SEARCH, "people.name"),
    PEOPLE_NAME_EXACT(ParameterType.FILTER, "people.name"),
    PEOPLE_ROLE(ParameterType.FILTER, "people.role"),
    PEOPLE_CHARACTER(ParameterType.FILTER, "people.character"),

    // Locations
    LOCATIONS_DURATION(ParameterType.FILTER, "locations.duration"),
    LOCATIONS_AVAILABLE(ParameterType.FILTER, "locations.available"),
    LOCATIONS_AVAILABILITY_START(ParameterType.FILTER, "locations.availability_start"),
    LOCATIONS_AVAILABILITY_END(ParameterType.FILTER, "locations.availability_end"),
    LOCATIONS_AVAILABILITY_COUNTRIES(ParameterType.FILTER, "locations.availability_countries"),
    LOCATIONS_CURRENCY(ParameterType.FILTER, "locations.currency"),
    LOCATIONS_AMOUNT(ParameterType.FILTER, "locations.amount"),
    LOCATIONS_REVENUE_CONTRACT(ParameterType.FILTER, "locations.revenue_contract"),
    LOCATIONS_SUBSCRIPTION_PACKAGES(ParameterType.FILTER, "locations.subscription_packages"),
    LOCATIONS_BIT_RATE(ParameterType.FILTER, "locations.bit_rate"),
    LOCATIONS_AUDIO_BIT_RATE(ParameterType.FILTER, "locations.audio_bit_rate"),
    LOCATIONS_AUDIO_CODING(ParameterType.FILTER, "locations.audio_coding"),
    LOCATIONS_VIDEO_ASPECT_RATIO(ParameterType.FILTER, "locations.video_aspect_ratio"),
    LOCATIONS_VIDEO_BIT_RATE(ParameterType.FILTER, "locations.video_bit_rate"),
    LOCATIONS_VIDEO_CODING(ParameterType.FILTER, "locations.video_coding"),
    LOCATIONS_VIDEO_FRAME_RATE(ParameterType.FILTER, "locations.video_frame_rate"),
    LOCATIONS_VIDEO_HORIZONTAL_SIZE(ParameterType.FILTER, "locations.video_horizontal_size"),
    LOCATIONS_VIDEO_PROGRESSIVE_SCAN(ParameterType.FILTER, "locations.video_progressive_scan"),
    LOCATIONS_VIDEO_VERTICAL_SIZE(ParameterType.FILTER, "locations.video_vertical_size"),
    LOCATIONS_DATA_SIZE(ParameterType.FILTER, "locations.data_size"),
    LOCATIONS_DATA_CONTAINER_FORMAT(ParameterType.FILTER, "locations.data_container_format"),
    LOCATIONS_SOURCE(ParameterType.FILTER, "locations.source"),
    LOCATIONS_DISTRIBUTOR(ParameterType.FILTER, "locations.distributor"),
    LOCATIONS_HAS_DOG(ParameterType.FILTER, "locations.has_dog"),
    LOCATIONS_IS_3D(ParameterType.FILTER, "locations.is_3d"),
    LOCATIONS_QUALITY(ParameterType.FILTER, "locations.quality"),

    // Pricing
    PRICING_START_TIME(ParameterType.FILTER, "pricing.start_time"),
    PRICING_END_TIME(ParameterType.FILTER, "pricing.end_time"),
    PRICING_PRICE(ParameterType.FILTER, "pricing.price"),
    PRICING_CURRENCY(ParameterType.FILTER, "pricing.currency"),

    // Container
    CONTAINER_ID(ParameterType.FILTER, "container.id"),
    CONTAINER_TITLE(ParameterType.SEARCH, "container.title"),
    CONTAINER_TITLE_EXACT(ParameterType.FILTER, "container.title"),
    CONTAINER_DESCRIPTION(ParameterType.SEARCH, "container.description"),
    CONTAINER_TYPE(ParameterType.FILTER, "container.type"),

    // Series
    SERIES_ID(ParameterType.FILTER, "series.id"),
    SERIES_TITLE(ParameterType.SEARCH, "series.title"),
    SERIES_TITLE_EXACT(ParameterType.FILTER, "series.title"),
    SERIES_DESCRIPTION(ParameterType.SEARCH, "series.description"),
    SERIES_SERIES_NUMBER(ParameterType.FILTER, "series.series_number"),

    // Children
    CHILDREN_ID(ParameterType.FILTER, "children.id"),
    CHILDREN_TITLE(ParameterType.SEARCH, "children.title"),
    CHILDREN_TITLE_EXACT(ParameterType.FILTER, "children.title"),
    CHILDREN_DESCRIPTION(ParameterType.SEARCH, "children.description"),
    CHILDREN_EPISODE_NUMBER(ParameterType.FILTER, "children.episode_number"),

    // Awards
    AWARDS_OUTCOME(ParameterType.FILTER, "awards.outcome"),
    AWARDS_TITLE(ParameterType.SEARCH, "awards.title"),
    AWARDS_TITLE_EXACT(ParameterType.FILTER, "awards.title"),
    AWARDS_DESCRIPTION(ParameterType.SEARCH, "awards.description"),
    AWARDS_YEAR(ParameterType.FILTER, "awards.year"),

    // Ratings
    RATINGS_VALUE(ParameterType.FILTER, "ratings.value"),
    RATINGS_TYPE(ParameterType.FILTER, "ratings.type"),
    RATINGS_SOURCE_KEY(ParameterType.FILTER, "ratings.key"),
    RATINGS_SOURCE_COUNTRY(ParameterType.FILTER, "ratings.country"),

    // Reviews
    REVIEWS_REVIEW(ParameterType.SEARCH, "reviews.review"),
    REVIEWS_LOCALE(ParameterType.FILTER, "reviews.locale"),
    REVIEWS_AUTHOR(ParameterType.FILTER, "reviews.author"),
    REVIEWS_AUTHOR_INITIALS(ParameterType.FILTER, "reviews.author_initials"),
    REVIEWS_RATING(ParameterType.FILTER, "reviews.rating"),
    REVIEWS_DATE(ParameterType.FILTER, "reviews.date"),
    REVIEWS_REVIEW_TYPE(ParameterType.FILTER, "reviews.review_type"),
    REVIEWS_SOURCE_KEY(ParameterType.FILTER, "reviews.key"),
    REVIEWS_SOURCE_COUNTRY(ParameterType.FILTER, "reviews.country"),
    ;

    private final ParameterType type;
    private final String name;
    SherlockParameter(ParameterType type, String name) {
        this.type = type;
        this.name = name;
    }

    public String getParameterName() {
        return type.name().toLowerCase() + "." + name;
    }

    public static Set<String> getAllNames() {
        return Arrays.stream(SherlockParameter.values())
                .map(SherlockParameter::getParameterName)
                .collect(Collectors.toSet());
    }

    enum ParameterType {
        FILTER,
        SEARCH
    }
}
