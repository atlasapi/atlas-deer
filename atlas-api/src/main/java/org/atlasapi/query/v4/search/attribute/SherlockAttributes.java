package org.atlasapi.query.v4.search.attribute;

import com.google.common.collect.ImmutableList;
import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import org.atlasapi.channel.ChannelGroupResolver;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Specialization;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.common.coercers.IdCoercer;
import org.atlasapi.query.common.coercers.StringCoercer;
import org.atlasapi.query.v4.search.coercer.DateRangeCoercer;
import org.atlasapi.query.v4.search.coercer.InstantRangeCoercer;
import org.atlasapi.query.v4.search.coercer.NumberRangeCoercer;
import org.atlasapi.source.Sources;

import java.time.Instant;
import java.util.List;

public class SherlockAttributes {

    private final NumberToShortStringCodec idCodec;
    private final ChannelGroupResolver channelGroupResolver;
    private final ContentMapping content = IndexMapping.getContentMapping();

    public SherlockAttributes(NumberToShortStringCodec idCodec, ChannelGroupResolver channelGroupResolver) {
        this.idCodec = idCodec;
        this.channelGroupResolver = channelGroupResolver;
    }

    public List<SherlockAttribute<?, ?, ?, ?>> getAttributes() {
        return ImmutableList.<SherlockAttribute<?, ?, ?, ?>>builder()
                .addAll(getContentAttributes())
                .addAll(getAliasAttributes())
                .addAll(getRestrictionsAttributes())
                .addAll(getCertificatesAttributes())
                .addAll(getLanguagesAttributes())
                .addAll(getReleaseDatesAttributes())
                .addAll(getScheduleAttributes())
                .addAll(getPeopleAttributes())
                .addAll(getLocationsAttributes())
                .addAll(getVideoAttributes())
                .addAll(getAudioAttributes())
                .addAll(getDataAttributes())
                .addAll(getPricingAttributes())
                .addAll(getContainerAttributes())
                .addAll(getSeriesAttributes())
                .addAll(getChildrenAttributes())
                .addAll(getAwardsAttributes())
                .addAll(getRatingsAttributes())
                .addAll(getReviewsAttributes())
                .build();
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getContentAttributes() {
        return ImmutableList.of(
                new SearchAttribute(
                        SherlockParameter.TITLE,
                        content.getTitle()
                ),
                new SearchAttribute(
                        SherlockParameter.DESCRIPTION,
                        content.getDescription()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.MEDIA_TYPE,
                        content.getMediaType(),
                        StringCoercer.create()
                ),
                new EnumAttribute<>(
                        SherlockParameter.SPECIALIZATION,
                        content.getSpecialization(),
                        EnumCoercer.create(Specialization.FROM_KEY())
                ),
                new KeywordAttribute<>(
                        SherlockParameter.GENRES,
                        content.getGenres(),
                        StringCoercer.create()
                ),
                new EnumAttribute<>(
                        SherlockParameter.TYPE,
                        content.getType(),
                        EnumCoercer.create(ContentType.fromKey())
                ),
                new RangeAttribute<>(
                        SherlockParameter.EPISODE_NUMBER,
                        content.getEpisodeNumber(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new MultiFieldAttribute<>(
                        SherlockParameter.SERIES_NUMBER,
                        NumberRangeCoercer.createIntegerCoercer(),
                        RangeAttribute::getRangeOrTerm,
                        content.getSeriesNumber(),
                        content.getSeries().getSeriesNumber()
                ),
                new RangeAttribute<>(
                        SherlockParameter.TOTAL_EPISODES,
                        content.getTotalEpisodes(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.COUNTRIES_OF_ORIGIN,
                        content.getCountriesOfOrigin(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.YEAR,
                        content.getYear(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new BooleanAttribute(
                        SherlockParameter.TOP_LEVEL,
                        content.getTopLevel()
                ),
                new EnumAttribute<>(
                        SherlockParameter.SOURCE,
                        content.getSource().getKey(),
                        EnumCoercer.create(Sources.fromKey())
                ),
                new KeywordAttribute<>(
                        SherlockParameter.SOURCE_COUNTRY,
                        content.getSource().getCountry(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getAliasAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.ALIASES_VALUE,
                        content.getAliases().getValue(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.ALIASES_NAMESPACE,
                        content.getAliases().getNamespace(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getRestrictionsAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.RESTRICTIONS_AUTHORITY,
                        content.getRestrictions().getAuthority(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RESTRICTIONS_RATING,
                        content.getRestrictions().getRating(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.RESTRICTIONS_MINIMUM_AGE,
                        content.getRestrictions().getMinimumAge(),
                        NumberRangeCoercer.createIntegerCoercer()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getCertificatesAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.CERTIFICATES_CLASSIFICATION,
                        content.getCertificates().getClassification(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.CERTIFICATES_CODE,
                        content.getCertificates().getCode(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getLanguagesAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.LANGUAGES_CODE,
                        content.getLanguages().getCode(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getReleaseDatesAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.RELEASE_DATE,
                        content.getReleaseDates().getReleaseDate(),
                        DateRangeCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RELEASE_DATES_COUNTRY,
                        content.getReleaseDates().getCountry(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RELEASE_DATES_TYPE,
                        content.getReleaseDates().getType(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getScheduleAttributes() {
        return ImmutableList.of(
                new BeforeAfterAttribute(
                        SherlockParameter.SCHEDULE_UPCOMING,
                        content.getBroadcasts().getTransmissionStartTime()
                ),
                new RangeAttribute<>(
                        SherlockParameter.SCHEDULE_START_TIME,
                        content.getBroadcasts().getTransmissionStartTime(),
                        InstantRangeCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.SCHEDULE_END_TIME,
                        content.getBroadcasts().getTransmissionEndTime(),
                        InstantRangeCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.SCHEDULE_DURATION,
                        content.getBroadcasts().getBroadcastDuration(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new IdAttribute(
                        SherlockParameter.SCHEDULE_CHANNEL,
                        content.getBroadcasts().getBroadcastOn(),
                        IdCoercer.create(idCodec)
                ),
                new ChannelGroupAttribute(
                        SherlockParameter.SCHEDULE_CHANNEL_GROUP,
                        content.getBroadcasts().getBroadcastOn(),
                        IdCoercer.create(idCodec),
                        channelGroupResolver
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getPeopleAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_URI,
                        content.getPeople().getUri(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_CURIE,
                        content.getPeople().getCurie(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_TYPE,
                        content.getPeople().getType(),
                        StringCoercer.create()
                ),
                new SearchAttribute(
                        SherlockParameter.PEOPLE_NAME,
                        content.getPeople().getName()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_NAME_EXACT,
                        content.getPeople().getNameExact(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_ROLE,
                        content.getPeople().getRole(),
                        StringCoercer.create()
                ),
                new SearchAttribute(
                        SherlockParameter.PEOPLE_CHARACTER,
                        content.getPeople().getCharacter()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.PEOPLE_CHARACTER_EXACT,
                        content.getPeople().getCharacterExact(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getLocationsAttributes() {
        return ImmutableList.of(
                new BetweenRangeAttribute<>(
                        SherlockParameter.LOCATIONS_AVAILABLE,
                        content.getLocations().getAvailabilityStart(),
                        content.getLocations().getAvailabilityEnd(),
                        Instant.now()
                ),
                new RangeAttribute<>(
                        SherlockParameter.LOCATIONS_AVAILABILITY_START,
                        content.getLocations().getAvailabilityStart(),
                        InstantRangeCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.LOCATIONS_AVAILABILITY_END,
                        content.getLocations().getAvailabilityEnd(),
                        InstantRangeCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_AVAILABILITY_COUNTRIES,
                        content.getLocations().getAvailabilityCountries(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_REVENUE_CONTRACT,
                        content.getLocations().getRevenueContract(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_SUBSCRIPTION_PACKAGES,
                        content.getLocations().getSubscriptionPackages(),
                        StringCoercer.create()
                ),
                new EnumAttribute<>(
                        SherlockParameter.LOCATIONS_SOURCE,
                        content.getLocations().getSource().getKey(),
                        EnumCoercer.create(Sources.fromKey())
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_SOURCE_COUNTRY,
                        content.getLocations().getSource().getCountry(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_DISTRIBUTOR,
                        content.getLocations().getDistributor(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.LOCATIONS_PROVIDER_NAME,
                        content.getLocations().getProvider().getName(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getVideoAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_DURATION,
                        content.getLocations().getDuration(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.VIDEO_ASPECT_RATIO,
                        content.getLocations().getVideoAspectRatio(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_BIT_RATE,
                        content.getLocations().getVideoBitRate(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new EnumAttribute<>(
                        SherlockParameter.VIDEO_CODING,
                        content.getLocations().getVideoCoding(),
                        EnumCoercer.create(MimeType::possibleFromString)
                ),
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_FRAME_RATE,
                        content.getLocations().getVideoFrameRate(),
                        NumberRangeCoercer.createFloatCoercer()
                ),
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_HORIZONTAL_SIZE,
                        content.getLocations().getVideoHorizontalSize(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new BooleanAttribute(
                        SherlockParameter.VIDEO_PROGRESSIVE_SCAN,
                        content.getLocations().getVideoProgressiveScan()
                ),
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_VERTICAL_SIZE,
                        content.getLocations().getVideoVerticalSize(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new BooleanAttribute(
                        SherlockParameter.VIDEO_HAS_DOG,
                        content.getLocations().getHasDog()
                ),
                new BooleanAttribute(
                        SherlockParameter.VIDEO_IS_3D,
                        content.getLocations().getIs3d()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.VIDEO_QUALITY,
                        content.getLocations().getQuality(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getAudioAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.AUDIO_BIT_RATE,
                        content.getLocations().getAudioBitRate(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new EnumAttribute<>(
                        SherlockParameter.AUDIO_CODING,
                        content.getLocations().getAudioCoding(),
                        EnumCoercer.create(MimeType::possibleFromString)
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getDataAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.DATA_BIT_RATE,
                        content.getLocations().getBitRate(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new RangeAttribute<>(
                        SherlockParameter.DATA_SIZE,
                        content.getLocations().getDataSize(),
                        NumberRangeCoercer.createLongCoercer()
                ),
                new EnumAttribute<>(
                        SherlockParameter.DATA_CONTAINER_FORMAT,
                        content.getLocations().getDataContainerFormat(),
                        EnumCoercer.create(MimeType::possibleFromString)
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getPricingAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.PRICING_CURRENCY,
                        content.getLocations().getPricing().getCurrency(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.PRICING_AMOUNT,
                        content.getLocations().getPricing().getPrice(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new RangeAttribute<>(
                        SherlockParameter.PRICING_START_TIME,
                        content.getLocations().getPricing().getStartTime(),
                        InstantRangeCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.PRICING_END_TIME,
                        content.getLocations().getPricing().getEndTime(),
                        InstantRangeCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getContainerAttributes() {
        return ImmutableList.of(
                new IdAttribute(
                        SherlockParameter.CONTAINER_ID,
                        content.getContainer().getId(),
                        IdCoercer.create(idCodec)
                ),
                new SearchAttribute(
                        SherlockParameter.CONTAINER_TITLE,
                        content.getContainer().getTitle()
                ),
                new SearchAttribute(
                        SherlockParameter.CONTAINER_DESCRIPTION,
                        content.getContainer().getDescription()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.CONTAINER_TYPE,
                        content.getContainer().getType(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getSeriesAttributes() {
        return ImmutableList.of(
                new IdAttribute(
                        SherlockParameter.SERIES_ID,
                        content.getSeries().getId(),
                        IdCoercer.create(idCodec)
                ),
                new SearchAttribute(
                        SherlockParameter.SERIES_TITLE,
                        content.getSeries().getTitle()
                ),
                new SearchAttribute(
                        SherlockParameter.SERIES_DESCRIPTION,
                        content.getSeries().getDescription()
                ),
                new RangeAttribute<>(
                        SherlockParameter.SERIES_SERIES_NUMBER,
                        content.getSeries().getSeriesNumber(),
                        NumberRangeCoercer.createIntegerCoercer()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getChildrenAttributes() {
        return ImmutableList.of(
                new IdAttribute(
                        SherlockParameter.CHILDREN_ID,
                        content.getChildren().getId(),
                        IdCoercer.create(idCodec)
                ),
                new SearchAttribute(
                        SherlockParameter.CHILDREN_TITLE,
                        content.getChildren().getTitle()
                ),
                new SearchAttribute(
                        SherlockParameter.CHILDREN_DESCRIPTION,
                        content.getChildren().getDescription()
                ),
                new RangeAttribute<>(
                        SherlockParameter.CHILDREN_EPISODE_NUMBER,
                        content.getChildren().getEpisodeNumber(),
                        NumberRangeCoercer.createIntegerCoercer()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getAwardsAttributes() {
        return ImmutableList.of(
                new KeywordAttribute<>(
                        SherlockParameter.AWARDS_OUTCOME,
                        content.getAwards().getOutcome(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.AWARDS_TITLE,
                        content.getAwards().getTitle(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.AWARDS_DESCRIPTION,
                        content.getAwards().getDescription(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.AWARDS_YEAR,
                        content.getAwards().getYear(),
                        NumberRangeCoercer.createIntegerCoercer()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getRatingsAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.RATINGS_VALUE,
                        content.getRatings().getValue(),
                        NumberRangeCoercer.createFloatCoercer()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RATINGS_TYPE,
                        content.getRatings().getType(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RATINGS_SOURCE_KEY,
                        content.getRatings().getSource().getKey(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.RATINGS_SOURCE_COUNTRY,
                        content.getRatings().getSource().getCountry(),
                        StringCoercer.create()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?, ?>> getReviewsAttributes() {
        return ImmutableList.of(
                new SearchAttribute(
                        SherlockParameter.REVIEWS_REVIEW,
                        content.getReviews().getReview()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_LANGUAGE,
                        content.getReviews().getLanguage(),
                        StringCoercer.create()
                ),
                new SearchAttribute(
                        SherlockParameter.REVIEWS_AUTHOR,
                        content.getReviews().getAuthor()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_AUTHOR_EXACT,
                        content.getReviews().getAuthorExact(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_AUTHOR_INITIALS,
                        content.getReviews().getAuthorInitials(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_RATING,
                        content.getReviews().getRating(),
                        StringCoercer.create()
                ),
                new RangeAttribute<>(
                        SherlockParameter.REVIEWS_DATE,
                        content.getReviews().getDate(),
                        InstantRangeCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_REVIEW_TYPE,
                        content.getReviews().getReviewType(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_SOURCE_KEY,
                        content.getReviews().getSource().getKey(),
                        StringCoercer.create()
                ),
                new KeywordAttribute<>(
                        SherlockParameter.REVIEWS_SOURCE_COUNTRY,
                        content.getReviews().getSource().getCountry(),
                        StringCoercer.create()
                )
        );
    }
}
