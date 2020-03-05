package org.atlasapi.content.v2.serialization.setters;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.MediaType;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.v2.model.Described;
import org.atlasapi.content.v2.model.udt.Award;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.Interval;
import org.atlasapi.content.v2.model.udt.LocalizedTitle;
import org.atlasapi.content.v2.serialization.AwardSerialization;
import org.atlasapi.content.v2.serialization.ImageSerialization;
import org.atlasapi.content.v2.serialization.LocalizedTitleSerialization;
import org.atlasapi.content.v2.serialization.PrioritySerialization;
import org.atlasapi.content.v2.serialization.RelatedLinkSerialization;
import org.atlasapi.content.v2.serialization.SynopsesSerialization;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import org.joda.time.DateTime;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class DescribedSetter {

    private final IdentifiedSetter identifiedSetter = new IdentifiedSetter();
    private final ImageSerialization image = new ImageSerialization();
    private final PrioritySerialization priority = new PrioritySerialization();
    private final SynopsesSerialization synopses = new SynopsesSerialization();
    private final RelatedLinkSerialization relatedLink = new RelatedLinkSerialization();
    private final LocalizedTitleSerialization localizedTitle = new LocalizedTitleSerialization();
    private final AwardSerialization award = new AwardSerialization();

    public void serialize(Described internal, org.atlasapi.content.Described content) {
        identifiedSetter.serialize(internal, content);

        internal.setTitle(content.getTitle());
        internal.setLocalizedTitles(content.getTitles().stream()
                .map(localizedTitle::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setShortDescription(content.getShortDescription());
        internal.setMediumDescription(content.getMediumDescription());
        internal.setLongDescription(content.getLongDescription());

        internal.setSynopses(synopses.serialize(content.getSynopses()));
        internal.setDescription(content.getDescription());

        MediaType mediaType = content.getMediaType();
        if (mediaType != null) {
            internal.setMediaType(mediaType.name());
        }
        Specialization specialization = content.getSpecialization();
        if (specialization != null) {
            internal.setSpecialization(specialization.name());
        }
        internal.setGenres(content.getGenres());
        Publisher source = content.getSource();
        if (source != null) {
            internal.setPublisher(source.key());
        }
        internal.setImage(content.getImage());

        internal.setImages(content.getImages().stream()
                .filter(Objects::nonNull)
                .collect(MoreCollectors.toImmutableMap(
                        image::serialize,
                        img -> {
                            Interval interval = new Interval();

                            DateTime start = img.getAvailabilityStart();
                            if (start != null) {
                                interval.setStart(toInstant(start));
                            }

                            DateTime end = img.getAvailabilityEnd();
                            if (end != null) {
                                interval.setEnd(toInstant(end));
                            }

                            return interval;
                        }
                )));
        internal.setThumbnail(content.getThumbnail());
        internal.setFirstSeen(toInstant(content.getFirstSeen()));
        internal.setLastFetched(toInstant(content.getLastFetched()));
        internal.setThisOrChildLastUpdated(toInstant(content.getThisOrChildLastUpdated()));
        internal.setScheduleOnly(content.isScheduleOnly());
        internal.setActivelyPublished(content.isActivelyPublished());
        internal.setPresentationChannel(content.getPresentationChannel());
        internal.setPriority(priority.serialize(content.getPriority()));
        internal.setRelatedLinks(content.getRelatedLinks().stream()
                .map(relatedLink::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setAwards(content.getAwards()
                .stream()
                .map(award::serialize)
                .collect(Collectors.toSet()));
    }

    public void deserialize(org.atlasapi.content.Described content, Described internal) {
        identifiedSetter.deserialize(content, internal);

        content.setTitle(internal.getTitle());

        Set<LocalizedTitle> localizedTitles = internal.getLocalizedTitles();
        if (localizedTitles != null) {
            content.setTitles(localizedTitles.stream()
            .map(localizedTitle::deserialize)
            .collect(Collectors.toSet()));
        }

        content.setShortDescription(internal.getShortDescription());
        content.setMediumDescription(internal.getMediumDescription());
        content.setLongDescription(internal.getLongDescription());

        content.setSynopses(synopses.deserialize(internal.getSynopses()));
        content.setDescription(internal.getDescription());

        String mediaType = internal.getMediaType();
        if (mediaType != null) {
            content.setMediaType(MediaType.valueOf(mediaType));
        }

        String specialization = internal.getSpecialization();
        if (specialization != null) {
            content.setSpecialization(Specialization.valueOf(specialization));
        }

        Set<String> genres = internal.getGenres();
        if (genres != null) {
            content.setGenres(genres);
        }

        String publisher = internal.getPublisher();
        if (publisher != null) {
            content.setPublisher(Publisher.fromKey(publisher).requireValue());
        }

        content.setImage(internal.getImage());

        Map<Image, Interval> images = internal.getImages();

        if (images != null) {
            content.setImages(images.entrySet().stream().map(internalImg -> {
                org.atlasapi.content.Image img = image.deserialize(internalImg.getKey());

                Interval availability = internalImg.getValue();
                img.setAvailabilityStart(toDateTime(availability.getStart()));
                img.setAvailabilityEnd(toDateTime(availability.getEnd()));

                return img;
            }).collect(MoreCollectors.toImmutableSet()));
        }

        content.setThumbnail(internal.getThumbnail());

        content.setFirstSeen(toDateTime(internal.getFirstSeen()));
        content.setLastFetched(toDateTime(internal.getLastFetched()));
        content.setThisOrChildLastUpdated(toDateTime(internal.getThisOrChildLastUpdated()));

        Boolean scheduleOnly = internal.getScheduleOnly();
        if (scheduleOnly != null) {
            content.setScheduleOnly(scheduleOnly);
        }

        Boolean activelyPublished = internal.getActivelyPublished();
        if (activelyPublished != null) {
            content.setActivelyPublished(activelyPublished);
        }

        content.setPresentationChannel(internal.getPresentationChannel());

        content.setPriority(priority.deserialize(internal.getPriority()));

        Set<org.atlasapi.content.v2.model.udt.RelatedLink> relatedLinks = internal.getRelatedLinks();
        if (relatedLinks != null) {
            content.setRelatedLinks(relatedLinks.stream()
                    .map(relatedLink::deserialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        Set<Award> awards = internal.getAwards();
        if (awards != null) {
            content.setAwards(awards.stream().map(award::deserialize).collect(Collectors.toSet()));
        }
    }
}