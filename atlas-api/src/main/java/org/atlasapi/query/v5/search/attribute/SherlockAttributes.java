package org.atlasapi.query.v5.search.attribute;

import java.time.Instant;
import java.util.List;

import javax.annotation.Nonnull;

import org.atlasapi.content.ContentType;
import org.atlasapi.content.Specialization;
import org.atlasapi.query.common.coercers.BooleanCoercer;
import org.atlasapi.query.common.coercers.EnumCoercer;
import org.atlasapi.query.v5.search.coercer.DateRangeCoercer;
import org.atlasapi.query.v5.search.coercer.InstantRangeCoercer;
import org.atlasapi.query.v5.search.coercer.NumberRangeCoercer;
import org.atlasapi.source.Sources;

import com.metabroadcast.common.ids.NumberToShortStringCodec;
import com.metabroadcast.common.media.MimeType;
import com.metabroadcast.sherlock.client.search.parameter.NamedParameter;
import com.metabroadcast.sherlock.client.search.parameter.RangeParameter;
import com.metabroadcast.sherlock.common.mapping.ContentMapping;
import com.metabroadcast.sherlock.common.mapping.IndexMapping;
import com.metabroadcast.sherlock.common.type.InstantMapping;

import com.google.common.collect.ImmutableList;

public class SherlockAttributes {

    private final NumberToShortStringCodec idCodec;
    private final ContentMapping content = IndexMapping.getContentMapping();

    public SherlockAttributes(NumberToShortStringCodec idCodec) {
        this.idCodec = idCodec;
    }

    public List<SherlockAttribute<?, ?, ?>> getAttributes() {
        return ImmutableList.<SherlockAttribute<?, ?, ?>>builder()
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

    private List<SherlockAttribute<?, ?, ?>> getContentAttributes() {
        return ImmutableList.of(
                new SearchAttribute(
                        SherlockParameter.TITLE,
                        content.getTitle()
                ),
                new KeywordAttribute(
                        SherlockParameter.TITLE_EXACT,
                        content.getTitleExact()
                ),
                new SearchAttribute(
                        SherlockParameter.DESCRIPTION,
                        content.getDescription()
                ),
                new KeywordAttribute(
                        SherlockParameter.MEDIA_TYPE,
                        content.getMediaType()
                ),
                new EnumAttribute<>(
                        SherlockParameter.SPECIALIZATION,
                        content.getSpecialization(),
                        EnumCoercer.create(Specialization.FROM_KEY())
                ),
                new KeywordAttribute(
                        SherlockParameter.GENRES,
                        content.getGenres()
                ),
                new EnumAttribute<>(
                        SherlockParameter.TYPE,
                        content.getType(),
                        EnumCoercer.create(ContentType.fromKey())
                ),
                new KeywordAttribute(
                        SherlockParameter.EPISODE_NUMBER,
                        content.getEpisodeNumber()
                ),
                new KeywordAttribute(
                        SherlockParameter.SERIES_NUMBER,
                        content.getSeriesNumber()
                ),
                new KeywordAttribute(
                        SherlockParameter.TOTAL_EPISODES,
                        content.getTotalEpisodes()
                ),
                new KeywordAttribute(
                        SherlockParameter.COUNTRIES_OF_ORIGIN,
                        content.getCountriesOfOrigin()
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
                        SherlockParameter.PUBLISHER,
                        content.getSource().getKey(),
                        EnumCoercer.create(Sources.fromKey())
                ),
                new KeywordAttribute(
                        SherlockParameter.PUBLISHER_COUNTRY,
                        content.getSource().getCountry()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getAliasAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.ALIASES_VALUE,
                        content.getAliases().getValue()
                ),
                new KeywordAttribute(
                        SherlockParameter.ALIASES_NAMESPACE,
                        content.getAliases().getNamespace()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getRestrictionsAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.RESTRICTIONS_AUTHORITY,
                        content.getRestrictions().getAuthority()
                ),
                new KeywordAttribute(
                        SherlockParameter.RESTRICTIONS_RATING,
                        content.getRestrictions().getRating()
                ),
                new KeywordAttribute(
                        SherlockParameter.RESTRICTIONS_MINIMUM_AGE,
                        content.getRestrictions().getMinimumAge()
                ),
                new SearchAttribute(
                        SherlockParameter.RESTRICTIONS_MESSAGE,
                        content.getRestrictions().getMessage()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getCertificatesAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.CERTIFICATES_CLASSIFICATION,
                        content.getCertificates().getClassification()
                ),
                new KeywordAttribute(
                        SherlockParameter.CERTIFICATES_CODE,
                        content.getCertificates().getCode()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getLanguagesAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.LANGUAGES_CODE,
                        content.getLanguages().getCode()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getReleaseDatesAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.RELEASE_DATE,
                        content.getReleaseDates().getReleaseDate(),
                        DateRangeCoercer.create()
                ),
                new KeywordAttribute(
                        SherlockParameter.RELEASE_DATES_COUNTRY,
                        content.getReleaseDates().getCountry()
                ),
                new KeywordAttribute(
                        SherlockParameter.RELEASE_DATES_TYPE,
                        content.getReleaseDates().getType()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getScheduleAttributes() {
        return ImmutableList.of(
                new SherlockAttribute<Boolean, Instant, InstantMapping>(
                        SherlockParameter.SCHEDULE_UPCOMING,
                        content.getBroadcasts().getTransmissionStartTime(),
                        BooleanCoercer.create()
                ) {
                    @Override
                    protected NamedParameter<Instant> createParameter(
                            InstantMapping mapping, @Nonnull Boolean value
                    ) {
                        if (value) {
                            return RangeParameter.from(mapping, Instant.now());
                        } else {
                            return RangeParameter.to(mapping, Instant.now());
                        }
                    }
                },
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
                        idCodec
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getPeopleAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_URI,
                        content.getPeople().getUri()
                ),
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_CURIE,
                        content.getPeople().getCurie()
                ),
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_TYPE,
                        content.getPeople().getType()
                ),
                new SearchAttribute(
                        SherlockParameter.PEOPLE_NAME,
                        content.getPeople().getName()
                ),
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_NAME_EXACT,
                        content.getPeople().getNameExact()
                ),
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_ROLE,
                        content.getPeople().getRole()
                ),
                new KeywordAttribute(
                        SherlockParameter.PEOPLE_CHARACTER,
                        content.getPeople().getCharacter()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getLocationsAttributes() {
        return ImmutableList.of(
                new BooleanAttribute(
                        SherlockParameter.LOCATIONS_AVAILABLE,
                        content.getLocations().getAvailable()
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
                new KeywordAttribute(
                        SherlockParameter.LOCATIONS_AVAILABILITY_COUNTRIES,
                        content.getLocations().getAvailabilityCountries()
                ),
                new KeywordAttribute(
                        SherlockParameter.LOCATIONS_REVENUE_CONTRACT,
                        content.getLocations().getRevenueContract()
                ),
                new KeywordAttribute(
                        SherlockParameter.LOCATIONS_SUBSCRIPTION_PACKAGES,
                        content.getLocations().getSubscriptionPackages()
                ),
                new KeywordAttribute(
                        SherlockParameter.LOCATIONS_SOURCE,
                        content.getLocations().getSource()
                ),
                new KeywordAttribute(
                        SherlockParameter.LOCATIONS_DISTRIBUTOR,
                        content.getLocations().getDistributor()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getVideoAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.VIDEO_DURATION,
                        content.getLocations().getDuration(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new KeywordAttribute(
                        SherlockParameter.VIDEO_ASPECT_RATIO,
                        content.getLocations().getVideoAspectRatio()
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
                new KeywordAttribute(
                        SherlockParameter.VIDEO_QUALITY,
                        content.getLocations().getQuality()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getAudioAttributes() {
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

    private List<SherlockAttribute<?, ?, ?>> getDataAttributes() {
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

    private List<SherlockAttribute<?, ?, ?>> getPricingAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.PRICING_CURRENCY,
                        content.getLocations().getCurrency()
                ),
                new RangeAttribute<>(
                        SherlockParameter.PRICING_AMOUNT,
                        content.getLocations().getAmount(),
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
                ),
                new RangeAttribute<>(
                        SherlockParameter.PRICING_TIMED_AMOUNT,
                        content.getLocations().getPricing().getPrice(),
                        NumberRangeCoercer.createIntegerCoercer()
                ),
                new KeywordAttribute(
                        SherlockParameter.PRICING_TIMED_CURRENCY,
                        content.getLocations().getPricing().getCurrency()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getContainerAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.CONTAINER_ID,
                        content.getContainer().getId()
                ),
                new SearchAttribute(
                        SherlockParameter.CONTAINER_TITLE,
                        content.getContainer().getTitle()
                ),
                new KeywordAttribute(
                        SherlockParameter.CONTAINER_TITLE_EXACT,
                        content.getContainer().getTitleExact()
                ),
                new SearchAttribute(
                        SherlockParameter.CONTAINER_DESCRIPTION,
                        content.getContainer().getDescription()
                ),
                new KeywordAttribute(
                        SherlockParameter.CONTAINER_TYPE,
                        content.getContainer().getType()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getSeriesAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.SERIES_ID,
                        content.getSeries().getId()
                ),
                new SearchAttribute(
                        SherlockParameter.SERIES_TITLE,
                        content.getSeries().getTitle()
                ),
                new KeywordAttribute(
                        SherlockParameter.SERIES_TITLE_EXACT,
                        content.getSeries().getTitleExact()
                ),
                new SearchAttribute(
                        SherlockParameter.SERIES_DESCRIPTION,
                        content.getSeries().getDescription()
                ),
                new KeywordAttribute(
                        SherlockParameter.SERIES_SERIES_NUMBER,
                        content.getSeries().getSeriesNumber()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getChildrenAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.CHILDREN_ID,
                        content.getChildren().getId()
                ),
                new SearchAttribute(
                        SherlockParameter.CHILDREN_TITLE,
                        content.getChildren().getTitle()
                ),
                new KeywordAttribute(
                        SherlockParameter.CHILDREN_TITLE_EXACT,
                        content.getChildren().getTitleExact()
                ),
                new SearchAttribute(
                        SherlockParameter.CHILDREN_DESCRIPTION,
                        content.getChildren().getDescription()
                ),
                new KeywordAttribute(
                        SherlockParameter.CHILDREN_EPISODE_NUMBER,
                        content.getChildren().getEpisodeNumber()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getAwardsAttributes() {
        return ImmutableList.of(
                new KeywordAttribute(
                        SherlockParameter.AWARDS_OUTCOME,
                        content.getAwards().getOutcome()
                ),
                new SearchAttribute(
                        SherlockParameter.AWARDS_TITLE,
                        content.getAwards().getTitle()
                ),
                new KeywordAttribute(
                        SherlockParameter.AWARDS_TITLE_EXACT,
                        content.getAwards().getExactTitle()
                ),
                new SearchAttribute(
                        SherlockParameter.AWARDS_DESCRIPTION,
                        content.getAwards().getDescription()
                ),
                new RangeAttribute<>(
                        SherlockParameter.AWARDS_YEAR,
                        content.getAwards().getYear(),
                        NumberRangeCoercer.createIntegerCoercer()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getRatingsAttributes() {
        return ImmutableList.of(
                new RangeAttribute<>(
                        SherlockParameter.RATINGS_VALUE,
                        content.getRatings().getValue(),
                        NumberRangeCoercer.createFloatCoercer()
                ),
                new KeywordAttribute(
                        SherlockParameter.RATINGS_TYPE,
                        content.getRatings().getType()
                ),
                new KeywordAttribute(
                        SherlockParameter.RATINGS_SOURCE_KEY,
                        content.getRatings().getSource().getKey()
                ),
                new KeywordAttribute(
                        SherlockParameter.RATINGS_SOURCE_COUNTRY,
                        content.getRatings().getSource().getCountry()
                )
        );
    }

    private List<SherlockAttribute<?, ?, ?>> getReviewsAttributes() {
        return ImmutableList.of(
                new SearchAttribute(
                        SherlockParameter.REVIEWS_REVIEW,
                        content.getReviews().getReview()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_LANGUAGE,
                        content.getReviews().getLanguage()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_AUTHOR,
                        content.getReviews().getAuthor()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_AUTHOR_INITIALS,
                        content.getReviews().getAuthorInitials()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_RATING,
                        content.getReviews().getRating()
                ),
                new RangeAttribute<>(
                        SherlockParameter.REVIEWS_DATE,
                        content.getReviews().getDate(),
                        InstantRangeCoercer.create()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_REVIEW_TYPE,
                        content.getReviews().getReviewType()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_SOURCE_KEY,
                        content.getReviews().getSource().getKey()
                ),
                new KeywordAttribute(
                        SherlockParameter.REVIEWS_SOURCE_COUNTRY,
                        content.getReviews().getSource().getCountry()
                )
        );
    }
}
