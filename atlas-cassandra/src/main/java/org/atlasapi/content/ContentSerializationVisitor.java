package org.atlasapi.content;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import com.google.common.base.MoreObjects;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.AwardSerializer;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.Rating;
import org.atlasapi.entity.RatingSerializer;
import org.atlasapi.entity.Review;
import org.atlasapi.entity.ReviewSerializer;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Content.Builder;

import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import javax.annotation.Nullable;
import javax.validation.constraints.Null;

public final class ContentSerializationVisitor implements ContentVisitor<Builder> {

    private static final Logger log = LoggerFactory.getLogger(ContentSerializationVisitor.class);

    private final BroadcastSerializer broadcastSerializer = BroadcastSerializer.create();
    private final ImageSerializer imageSerializer = new ImageSerializer();
    private final EncodingSerializer encodingSerializer = new EncodingSerializer();
    private final SegmentEventSerializer segmentEventSerializer = new SegmentEventSerializer();
    private final RestrictionSerializer restrictionSerializer = new RestrictionSerializer();
    private final TagSerializer tagSerializer = new TagSerializer();
    private final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();
    private final KeyPhraseSerializer keyPhraseSerializer = new KeyPhraseSerializer();
    private final CrewMemberSerializer crewMemberSerializer = new CrewMemberSerializer();
    private final ContainerSummarySerializer containerSummarySerializer = new ContainerSummarySerializer();
    private final ReleaseDateSerializer releaseDateSerializer = new ReleaseDateSerializer();
    private final CertificateSerializer certificateSerializer = new CertificateSerializer();
    private final ItemAndBroadcastRefSerializer itemAndBroadcastRefSerializer = new ItemAndBroadcastRefSerializer();
    private final ItemAndLocationSummarySerializer itemAndLocationSummarySerializer = new ItemAndLocationSummarySerializer();
    private final ItemSummarySerializer itemSummarySerializer = new ItemSummarySerializer();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();
    private final EventRefSerializer eventRefSerializer = new EventRefSerializer();
    private final AwardSerializer awardSerializer = new AwardSerializer();
    private final ReviewSerializer reviewSerializer = ReviewSerializer.create();
    private final RatingSerializer ratingSerializer = new RatingSerializer();

    public ContentSerializationVisitor() {}

    private Builder visitIdentified(Identified ided) {
        Builder builder = ContentProtos.Content.newBuilder();
        if (ided.getId() != null) {
            builder.setId(ided.getId().longValue())
                    .setType(ided.getClass().getSimpleName().toLowerCase());
        }
        if (ided.getLastUpdated() != null) {
            builder.setLastUpdated(dateTimeSerializer.serialize(ided.getLastUpdated()));
        }
        if (ided.getCanonicalUri() != null) {
            builder.setUri(ided.getCanonicalUri());
        }
        for (Alias alias : ided.getAliases()) {
            builder.addAliases(CommonProtos.Alias.newBuilder()
                    .setNamespace(alias.getNamespace())
                    .setValue(alias.getValue()));
        }
        for (EquivalenceRef equivRef : ided.getEquivalentTo()) {
            builder.addEquivs(CommonProtos.Reference.newBuilder()
                    .setId(equivRef.getId().longValue())
                    .setSource(equivRef.getSource().key())
            );
        }
        return builder;
    }

    private Builder visitDescribed(Described content) {
        Builder builder = visitIdentified(content);
        if (content.getThisOrChildLastUpdated() != null) {
            builder.setChildLastUpdated(
                    dateTimeSerializer.serialize(content.getThisOrChildLastUpdated())
            );
        }
        if (content.getSource() != null) {
            builder.setSource(content.getSource().key());
        }
        if (content.getFirstSeen() != null) {
            builder.setFirstSeen(dateTimeSerializer.serialize(content.getFirstSeen()));
        }
        if (content.getMediaType() != null && !MediaType.VIDEO.equals(content.getMediaType())) {
            builder.setMediaType(content.getMediaType().toKey());
        }
        if (content.getTitle() != null) {
            builder.addTitlesBuilder().setValue(content.getTitle()).build();
        }
        if (content.getDescription() != null) {
            builder.setDescription(content.getDescription());
        }
        ContentProtos.Synopsis.Builder synopsis = ContentProtos.Synopsis.newBuilder();
        boolean includeSynopsis = false;
        if (content.getShortDescription() != null) {
            synopsis.setShort(content.getShortDescription());
            includeSynopsis = true;
        }
        if (content.getMediumDescription() != null) {
            synopsis.setMedium(content.getMediumDescription());
            includeSynopsis = true;
        }
        if (content.getLongDescription() != null) {
            synopsis.setLong(content.getLongDescription());
            includeSynopsis = true;
        }
        if (includeSynopsis) {
            builder.addSynopses(synopsis);
        }
        builder.addAllGenres(content.getGenres());
        if (content.getImage() != null) {
            builder.setImage(content.getImage());
        }
        if (content.getThumbnail() != null) {
            builder.setThumb(content.getThumbnail());
        }
        for (Image image : content.getImages()) {
            builder.addImages(imageSerializer.serialize(image));
        }
        if (content.getPresentationChannel() != null) {
            builder.setPresentationChannel(content.getPresentationChannel());
        }
        if (content.isScheduleOnly()) {
            builder.setScheduleOnly(content.isScheduleOnly());
        }
        if (content.getSpecialization() != null) {
            builder.setSpecialization(content.getSpecialization().toString());
        }
        for (RelatedLink relatedLink : content.getRelatedLinks()) {
            builder.addRelatedLink(relatedLinkSerializer.serialize(relatedLink));
        }
        Priority priority = content.getPriority();
        if (priority != null) {
            ContentProtos.Priority.Builder priorityBuilder = ContentProtos.Priority.newBuilder();
            if (priority.getPriority() != null) {
                priorityBuilder.setScore(priority.getPriority());
            }
            PriorityScoreReasons scoreReasons = priority.getReasons();
            if (scoreReasons != null) {
                if (scoreReasons.getPositive() != null) {
                    priorityBuilder.addAllPositiveReasons(scoreReasons.getPositive());
                }
                if (scoreReasons.getNegative() != null) {
                    priorityBuilder.addAllNegativeReasons(scoreReasons.getNegative());
                }
            }
            builder.setPriorities(priorityBuilder.build());
        }

        for(Award award : content.getAwards()) {
            builder.addAwards(awardSerializer.serialize(award));
        }

        for (Rating rating: content.getRatings()) {
            builder.addRatings(ratingSerializer.serialize(rating));
        }

        for (Review review: content.getReviews()) {
            builder.addReviews(reviewSerializer.serialize(review));
        }

        builder.setActivelyPublished(content.isActivelyPublished());
        return builder;
    }

    private Builder visitContent(Content content) {
        Builder builder = visitDescribed(content);
        for (Certificate certificate : content.getCertificates()) {
            builder.addCertificates(certificateSerializer.serialize(certificate)
            );
        }
        for (CrewMember crew : content.people()) {
            builder.addCrewMembers(crewMemberSerializer.serialize(crew));
        }
        for (Clip clip : content.getClips()) {
            builder.addClips(clip.accept(this));
        }
        for (ContentGroupRef groupRef : content.getContentGroupRefs()) {
            builder.addContentGroupsBuilder()
                    .setId(groupRef.getId().longValue())
                    .build();
        }

        builder.addAllLanguage(content.getLanguages());

        for (KeyPhrase keyPhrase : content.getKeyPhrases()) {
            builder.addKeyPhrases(keyPhraseSerializer.serialize(keyPhrase));
        }

        for (Tag tag : content.getTags()) {
            builder.addTopicRefs(tagSerializer.serialize(tag));
        }

        for (EventRef eventRef : content.getEventRefs()) {
            builder.addEventRefs(eventRefSerializer.serialize(eventRef));
        }

        if (content.getYear() != null) {
            builder.setYear(content.getYear());
        }
        builder.addAllEncodings(serializeEncoding(content.getManifestedAs()));
        if (content.isGenericDescription() != null) {
            builder.setGenericDescription(content.isGenericDescription());
        }
        return builder;
    }

    private Builder visitItem(Item item) {
        Builder builder = visitContent(item);
        if (item.getContainerRef() != null) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(item.getSource());
            builder.setContainerRef(refSerializer.serialize(item.getContainerRef()));
        }
        if (item.getContainerSummary() != null) {
            builder.setContainerSummary(containerSummarySerializer.serialize(item.getContainerSummary()));
        }
        if (item.getBlackAndWhite() != null) {
            builder.setBlackAndWhite(item.getBlackAndWhite());
        }
        if (!item.getCountriesOfOrigin().isEmpty()) {
            builder.addAllCountries(Countries.toCodes(item.getCountriesOfOrigin()));
        }
        if (item.getIsLongForm()) {
            builder.setLongform(item.getIsLongForm());
        }
        builder.addAllBroadcasts(serializeBroadcasts(item.getBroadcasts()));
        builder.addAllSegmentEvents(serializeSegmentEvents(item.getSegmentEvents()));
        builder.addAllRestrictions(serializeRestrictions(item.getRestrictions()));
        return builder;
    }

    private Iterable<ContentProtos.SegmentEvent> serializeSegmentEvents(
            List<SegmentEvent> segmentEvents) {
        return Iterables.transform(
                segmentEvents,
                segmentEvent -> segmentEventSerializer.serialize(segmentEvent).build()
        );
    }

    private Iterable<ContentProtos.Broadcast> serializeBroadcasts(Set<Broadcast> broadcasts) {
        return Iterables.transform(
                broadcasts,
                broadcast -> broadcastSerializer.serialize(broadcast).build()
        );
    }

    private Iterable<ContentProtos.Encoding> serializeEncoding(Set<Encoding> encodings) {
        return Iterables.transform(
                encodings,
                encoding -> encodingSerializer.serialize(encoding).build()
        );
    }

    private Iterable<ContentProtos.Restriction> serializeRestrictions(
            Set<Restriction> restrictions) {
        return Iterables.transform(
                restrictions,
                restriction -> restrictionSerializer.serialize(restriction).build()
        );
    }

    private Builder visitContainer(Container container) {
        Builder builder = visitContent(container);
        ContentRefSerializer refSerializer = new ContentRefSerializer(container.getSource());
        for (ItemRef child : container.getItemRefs()) {
            builder.addChildren(refSerializer.serialize(child));
        }
        for (Map.Entry<ItemRef, Iterable<BroadcastRef>> upcomingContent : container.getUpcomingContent()
                .entrySet()) {
            builder.addUpcomingContent(
                    itemAndBroadcastRefSerializer.serialize(
                            upcomingContent.getKey(),
                            upcomingContent.getValue()
                    )
            );
        }
        for (Map.Entry<ItemRef, Iterable<LocationSummary>> availableContent : container.getAvailableContent()
                .entrySet()) {
            builder.addAvailableContent(
                    itemAndLocationSummarySerializer.serialize(
                            availableContent.getKey(),
                            availableContent.getValue()
                    )
            );
        }
        
        if (container.getItemSummaries() != null) {
            builder.addAllItemSummaries(
                    container.getItemSummaries().stream()
                            .map(this::serializeItemSummary)
                            .filter(Objects::nonNull)
                            .collect(Collectors.toList())
            );
        }

        if (container.getCertificates() != null) {
            builder.addAllCertificates(container.getCertificates().stream()
                    .map(certificateSerializer::serialize)
                    .collect(MoreCollectors.toImmutableSet()));
        }

        if (container.getYear() != null) {
            builder.addReleaseYears(container.getYear());
        }

        return builder;
    }

    @Nullable
    private ContentProtos.ItemSummary serializeItemSummary(ItemSummary itemSummary) {
        try {
            return itemSummarySerializer.serialize(itemSummary).build();
        } catch (Exception e) {
            log.error("Failed to serialize ItemSummary: {}", itemSummary.getItemRef().getId(), e.getMessage());
            return null;
        }
    }

    @Override
    public Builder visit(Brand brand) {
        Builder builder = visitContainer(brand);
        ContentRefSerializer refSerializer = new ContentRefSerializer(brand.getSource());
        for (SeriesRef seriesRef : brand.getSeriesRefs()) {
            builder.addSecondaries(refSerializer.serialize(seriesRef));
        }
        return builder;
    }

    @Override
    public Builder visit(Series series) {
        Builder builder = visitContainer(series);
        if (series.getBrandRef() != null) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(series.getSource());
            builder.setContainerRef(refSerializer.serialize(series.getBrandRef()));
        }
        if (series.getSeriesNumber() != null) {
            builder.setSeriesNumber(series.getSeriesNumber());
        }
        if (series.getTotalEpisodes() != null) {
            builder.setTotalEpisodes(series.getTotalEpisodes());
        }
        return builder;
    }

    @Override
    public Builder visit(Episode episode) {
        Builder builder = visitItem(episode);

        if (episode.getSeriesRef() != null) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(episode.getSource());
            builder.setSeriesRef(refSerializer.serialize(episode.getSeriesRef()));
        }
        if (episode.getSeriesNumber() != null) {
            builder.setSeriesNumber(episode.getSeriesNumber());
        }
        if (episode.getEpisodeNumber() != null) {
            builder.setEpisodeNumber(episode.getEpisodeNumber());
        }
        if (episode.getPartNumber() != null) {
            builder.setPartNumber(episode.getPartNumber());
        }
        if (episode.getSpecial() != null) {
            builder.setSpecial(episode.getSpecial());
        }

        return builder;
    }

    @Override
    public Builder visit(Film film) {
        Builder builder = visitItem(film);
        for (ReleaseDate releaseDate : film.getReleaseDates()) {
            builder.addReleaseDates(releaseDateSerializer.serialize(releaseDate));
        }
        if (film.getWebsiteUrl() != null) {
            builder.setWebsiteUrl(film.getWebsiteUrl());
        }
        for (Subtitles subtitles : film.getSubtitles()) {
            ContentProtos.Subtitle.Builder sub = ContentProtos.Subtitle.newBuilder();
            sub.setLanguage(subtitles.code());
            builder.addSubtitles(sub);
        }
        return builder;
    }

    @Override
    public Builder visit(Song song) {
        Builder builder = visitItem(song);
        if (song.getIsrc() != null) {
            builder.setIsrc(song.getIsrc());
        }
        if (song.getDuration() != null) {
            builder.setDuration(song.getDuration().getMillis());
        }
        return builder;
    }

    @Override
    public Builder visit(Item item) {
        return visitItem(item);
    }

    @Override
    public Builder visit(Clip clip) {
        return visitItem(clip);
    }

}
