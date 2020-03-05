package org.atlasapi.system.legacy;

import java.util.Collection;
import java.util.Date;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.LocalizedTitle;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Synopses;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.ReviewType;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Described;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Topic;
import org.atlasapi.media.entity.Version;
import org.atlasapi.source.Sources;

import com.google.common.base.Strings;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public abstract class DescribedLegacyResourceTransformer<F extends Described, T extends org.atlasapi.content.Described>
        extends BaseLegacyResourceTransformer<F, T> {

    protected final Logger log = LoggerFactory.getLogger(getClass());

    @Override
    public final T apply(F input) {
        T described = createDescribed(input);

        setIdentifiedFields(described, input);

        described.addAliases(moreAliases(input));

        described.setActivelyPublished(input.isActivelyPublished());
        described.setDescription(input.getDescription());
        described.setFirstSeen(input.getFirstSeen());
        described.setGenres(input.getGenres());
        described.setImage(input.getImage());
        described.setImages(transformImages(input.getImages()));
        described.setLastFetched(input.getLastFetched());
        described.setTitles(transformLocalizedTitles(input.getLocalizedTitles()));
        described.setLongDescription(input.getLongDescription());
        described.setMediaType(transformEnum(input.getMediaType(), MediaType.class));
        described.setMediumDescription(input.getMediumDescription());
        described.setPresentationChannel(input.getPresentationChannel());
        described.setPublisher(input.getPublisher());
        described.setRelatedLinks(transformRelatedLinks(input.getRelatedLinks()));
        described.setScheduleOnly(input.isScheduleOnly());
        described.setShortDescription(input.getShortDescription());
        described.setSynopses(getSynopses(input));
        described.setSpecialization(transformEnum(input.getSpecialization(), Specialization.class));
        if (input.getThisOrChildLastUpdated() != null) {
            described.setThisOrChildLastUpdated(input.getThisOrChildLastUpdated());
        } else {
            described.setThisOrChildLastUpdated(DateTime.now());
        }
        described.setThumbnail(input.getThumbnail());
        described.setTitle(input.getTitle());
//        Locale locale = new Locale();
        described.setPriority(transformPriority(input.getPriority()));
        described.setAwards(transformAwards(input.getAwards()));

        described.setReviews(transformReviews(input.getReviews()));
        described.setRatings(transformRatings(input.getRatings()));

        return described;
    }

    private org.atlasapi.content.Priority transformPriority(
            org.atlasapi.media.entity.Priority legacy) {
        if (legacy == null) {
            return null;
        }
        return new org.atlasapi.content.Priority(
                legacy.getScore(),
                new PriorityScoreReasons(
                        legacy.getReasons().getPositive(),
                        legacy.getReasons().getNegative()
                )
        );
    }

    protected <I extends org.atlasapi.entity.Identified> void setIdentifiedFields(I i,
            Identified input) {
        i.setAliases(transformAliases(input));
        i.setCanonicalUri(input.getCanonicalUri());
        i.setCurie(input.getCurie());
        i.setEquivalenceUpdate(input.getEquivalenceUpdate());

        // We want to carry across IDs for most items (and if it's missing in let i.setId NPE)
        // Clips do not carry IDs to skip
        if ((input instanceof Content || input instanceof Topic || input.getId() != null)
            && ! ( (input instanceof Clip) || (input instanceof Version)) ) {
            i.setId(input.getId());
        }
        if (input.getLastUpdated() != null) {
            i.setLastUpdated(input.getLastUpdated());
        } else {
            i.setLastUpdated(DateTime.now());
        }

        i.setCustomFields(input.getCustomFields());
    }

    protected abstract T createDescribed(F input);

    private Synopses getSynopses(org.atlasapi.media.entity.Described input) {
        Synopses synopses = Synopses.withShortDescription(input.getShortDescription());
        synopses.setMediumDescription(input.getMediumDescription());
        synopses.setLongDescription(input.getLongDescription());
        return synopses;
    }

    protected abstract Iterable<Alias> moreAliases(F input);

    protected Set<Award> transformAwards(Set<org.atlasapi.media.entity.Award> awards) {
        if(awards == null) {
            return ImmutableSet.of();
        }
        return awards.stream()
                .map(this::transformAward)
                .collect(Collectors.toSet());
    }

    protected Award transformAward(org.atlasapi.media.entity.Award awardLegacy) {
        Award award = new Award();
        award.setOutcome(awardLegacy.getOutcome());
        award.setTitle(awardLegacy.getTitle());
        award.setDescription(awardLegacy.getDescription());
        award.setYear(awardLegacy.getYear());
        return award;
    }

    protected Set<org.atlasapi.content.LocalizedTitle> transformLocalizedTitles(
            Collection<org.atlasapi.media.entity.LocalizedTitle> legacyLocalizedTitles)
    {
        if (legacyLocalizedTitles == null) {
            return ImmutableSet.of();
        }
        return legacyLocalizedTitles.stream()
                .map(this::transformLocalizedTitle)
                .collect(Collectors.toSet());
    }

    private LocalizedTitle transformLocalizedTitle(
            org.atlasapi.media.entity.LocalizedTitle legacyLocalizedTitle
    ) {
        LocalizedTitle localizedTitle = new LocalizedTitle();
        localizedTitle.setTitle(legacyLocalizedTitle.getTitle());
        localizedTitle.setLocale(legacyLocalizedTitle.getLocale());
        return localizedTitle;
    }

    protected Iterable<Review> transformReviews(Collection<org.atlasapi.media.entity.Review> legacyReviews) {
        return legacyReviews.stream()
                .map(this::transformReview)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
    }

    protected Optional<Review> transformReview(
            org.atlasapi.media.entity.Review legacyReview
    ) {
        // we don't want to fail the ingest of the whole content item because of
        // a broken legacy review
        try {
            if (Strings.isNullOrEmpty(legacyReview.getReview())) {
                return Optional.empty();
            }

            Review.Builder reviewBuilder = Review.builder(legacyReview.getReview());

            Date date = legacyReview.getDate();
            if (date != null) {
                reviewBuilder.withDate(new DateTime(date.getTime()));
            }
            return Optional.of(reviewBuilder.withLocale(legacyReview.getLocale())
                    .withAuthor(legacyReview.getAuthor())
                    .withAuthorInitials(legacyReview.getAuthorInitials())
                    .withRating(legacyReview.getRating())
                    .withReviewType(ReviewType.fromKey(legacyReview.getReviewTypeKey()))
                    .withSource(Optional.ofNullable(Sources.fromPossibleKey(legacyReview.getPublisherKey()).orNull()))
                    .build()
            );
        } catch (NullPointerException e) {
            return Optional.empty();
        }
    }

    protected Iterable<Rating> transformRatings(Iterable<org.atlasapi.media.entity.Rating> legacyRatings) {
        return Iterables.transform(
                legacyRatings,
                legacyRating -> new Rating(
                        legacyRating.getType(),
                        legacyRating.getValue(),
                        legacyRating.getPublisher(),
                        legacyRating.getNumberOfVotes()
                )
        );
    }
}
