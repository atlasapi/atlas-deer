package org.atlasapi.content;

import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Award;
import org.atlasapi.entity.AwardSerializer;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.RatingSerializer;
import org.atlasapi.entity.ReviewSerializer;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Subtitle;
import org.atlasapi.serialization.protobuf.ContentProtos.Synopsis;
import org.atlasapi.source.Sources;

import com.metabroadcast.common.intl.Countries;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Ordering;
import org.joda.time.Duration;

import static org.atlasapi.annotation.Annotation.AGGREGATED_BROADCASTS;
import static org.atlasapi.annotation.Annotation.AVAILABLE_CONTENT;
import static org.atlasapi.annotation.Annotation.AVAILABLE_CONTENT_DETAIL;
import static org.atlasapi.annotation.Annotation.AVAILABLE_LOCATIONS;
import static org.atlasapi.annotation.Annotation.BROADCASTS;
import static org.atlasapi.annotation.Annotation.CURRENT_AND_FUTURE_BROADCASTS;
import static org.atlasapi.annotation.Annotation.EXTENDED_DESCRIPTION;
import static org.atlasapi.annotation.Annotation.FIRST_BROADCASTS;
import static org.atlasapi.annotation.Annotation.LOCATIONS;
import static org.atlasapi.annotation.Annotation.NEXT_BROADCASTS;
import static org.atlasapi.annotation.Annotation.SUB_ITEMS;
import static org.atlasapi.annotation.Annotation.SUB_ITEM_SUMMARIES;
import static org.atlasapi.annotation.Annotation.UPCOMING_BROADCASTS;

final class ContentDeserializationVisitor implements ContentVisitor<Content> {

    private static final ImageSerializer imageSerializer = new ImageSerializer();
    private static final BroadcastSerializer broadcastSerializer = BroadcastSerializer.create();
    private static final EncodingSerializer encodingSerializer = new EncodingSerializer();
    private static final SegmentEventSerializer segmentEventSerializer = new SegmentEventSerializer();
    private static final RestrictionSerializer restrictionSerializer = new RestrictionSerializer();
    private static final TagSerializer tagSerializer = new TagSerializer();
    private static final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();
    private static final KeyPhraseSerializer keyPhraseSerializer = new KeyPhraseSerializer();
    private static final CrewMemberSerializer crewMemberSerializer = new CrewMemberSerializer();
    private static final ContainerSummarySerializer containerSummarySerializer = new ContainerSummarySerializer();
    private static final ReleaseDateSerializer releaseDateSerializer = new ReleaseDateSerializer();
    private static final CertificateSerializer certificateSerializer = new CertificateSerializer();
    private final ItemAndBroadcastRefSerializer itemAndBroadcastRefSerializer = new ItemAndBroadcastRefSerializer();
    private final ItemAndLocationSummarySerializer itemAndLocationSummarySerializer = new ItemAndLocationSummarySerializer();
    private final ItemSummarySerializer itemSummarySerializer = new ItemSummarySerializer();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();
    private final EventRefSerializer eventRefSerializer = new EventRefSerializer();
    private final AwardSerializer awardSerializer = new AwardSerializer();
    private final ReviewSerializer reviewSerializer = ReviewSerializer.create();
    private final RatingSerializer ratingSerializer = new RatingSerializer();

    private static final Set<Annotation> BROADCAST_ANNOTATIONS = ImmutableSet.of(
            BROADCASTS,
            UPCOMING_BROADCASTS,
            CURRENT_AND_FUTURE_BROADCASTS,
            FIRST_BROADCASTS,
            NEXT_BROADCASTS,
            AGGREGATED_BROADCASTS
    );
    private static final Annotation SUB_ITEMS_ANNOTATIONS = SUB_ITEMS;
    private static final Annotation SUB_ITEM_SUMMARIES_ANNOTATION = SUB_ITEM_SUMMARIES;
    private static final Set<Annotation> LOCATIONS_ANNOTATIONS = ImmutableSet.of(LOCATIONS, AVAILABLE_LOCATIONS);
    private static final Annotation UPCOMING_CONTENT_DETAIL = Annotation.UPCOMING_CONTENT_DETAIL;
    private static final Set<Annotation> AVAILABLE_CONTENT_ANNOTATIONS =
            ImmutableSet.of(AVAILABLE_CONTENT, AVAILABLE_CONTENT_DETAIL);

    private Set<Annotation> annotations;
    private ContentProtos.Content msg;

    public ContentDeserializationVisitor(ContentProtos.Content msg, Set<Annotation> activeAnnotations) {
        this.msg = msg;
        this.annotations = activeAnnotations;
    }

    public ContentDeserializationVisitor(ContentProtos.Content msg) {
        this.msg = msg;
        this.annotations = Annotation.all();
    }

    private <I extends Identified> I visitIdentified(I identified) {
        if (msg.hasId()) {
            identified.setId(Id.valueOf(msg.getId()));
        }
        if (msg.hasUri()) {
            identified.setCanonicalUri(msg.getUri());
        }
        if (msg.hasLastUpdated()) {
            identified.setLastUpdated(dateTimeSerializer.deserialize(msg.getLastUpdated()));
        }

        Builder<Alias> aliases = ImmutableSet.builder();
        for (CommonProtos.Alias alias : msg.getAliasesList()) {
            aliases.add(new Alias(alias.getNamespace(), alias.getValue()));
        }
        identified.setAliases(aliases.build());

        ImmutableSet.Builder<EquivalenceRef> equivRefs = ImmutableSet.builder();
        for (Reference equivRef : msg.getEquivsList()) {
            equivRefs.add(new EquivalenceRef(
                    Id.valueOf(equivRef.getId()),
                    Sources.fromPossibleKey(equivRef.getSource()).get()
            ));
        }
        identified.setEquivalentTo(equivRefs.build());
        return identified;
    }

    private <D extends Described> D visitDescribed(D described) {
        described = visitIdentified(described);

        Optional<Publisher> possibleSource = Optional.ofNullable(Sources.fromPossibleKey(msg.getSource()).orNull());
        described.setPublisher(possibleSource.get());  // NPE here is _desired behaviour_
        if (msg.hasFirstSeen()) {
            described.setFirstSeen(dateTimeSerializer.deserialize(msg.getFirstSeen()));
        }
        if (msg.hasChildLastUpdated()) {
            described.setThisOrChildLastUpdated(dateTimeSerializer.deserialize(msg.getChildLastUpdated()));
        }
        if (msg.hasMediaType()) {
            described.setMediaType(MediaType.fromKey(msg.getMediaType()).get());
        }
        if (msg.getTitlesCount() > 0) {
            described.setTitle(msg.getTitles(0).getValue());
        }
        if (msg.hasDescription()) {
            described.setDescription(msg.getDescription());
        }
        if (msg.hasImage()) {
            described.setImage(msg.getImage());
        }
        if (msg.hasThumb()) {
            described.setThumbnail(msg.getThumb());
        }
        if (msg.getSynopsesCount() > 0) {
            Synopsis synopsis = msg.getSynopses(0);
            if (synopsis.hasShort()) {
                described.setShortDescription(synopsis.getShort());
            }
            if (synopsis.hasMedium()) {
                described.setMediumDescription(synopsis.getMedium());
            }
            if (synopsis.hasLong()) {
                described.setLongDescription(synopsis.getLong());
            }
        }

        ImmutableSet.Builder<Image> images = ImmutableSet.builder();
        for (CommonProtos.Image image : msg.getImagesList()) {
            images.add(imageSerializer.deserialize(image));
        }
        described.setImages(images.build());
        described.setGenres(msg.getGenresList());
        described.setPresentationChannel(msg.getPresentationChannel());
        if (msg.hasScheduleOnly()) {
            described.setScheduleOnly(msg.getScheduleOnly());
        }
        if (msg.hasSpecialization()) {
            described.setSpecialization(Specialization.valueOf(msg.getSpecialization()
                    .toUpperCase()));
        }
        if (msg.hasPriority()) {
            Priority priority = new Priority(msg.getPriority(), new PriorityScoreReasons(
                    ImmutableList.of("Legacy priority"),
                    ImmutableList.of("Legacy priority")
            ));
            described.setPriority(priority);
        }
        if (msg.hasPriorities()) {
            ContentProtos.Priority priorities = msg.getPriorities();
            Priority priority = new Priority(priorities.getScore(), new PriorityScoreReasons(
                    priorities.getPositiveReasonsList(),
                    priorities.getNegativeReasonsList()
            ));
            described.setPriority(priority);
        }
        if (msg.hasActivelyPublished()) {
            described.setActivelyPublished(msg.getActivelyPublished());
        }
        ImmutableSet.Builder<Award> awardBuilder = ImmutableSet.builder();
        for (CommonProtos.Award award : msg.getAwardsList()) {
            awardBuilder.add(awardSerializer.deserialize(award));
        }
        described.setAwards(awardBuilder.build());

        // deserialization discards entities that failed to parse
        described.setReviews(msg.getReviewsList().stream()
                .map(reviewSerializer::deserialize)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));

        // deserialization discards entities that failed to parse
        described.setRatings(msg.getRatingsList().stream()
                .map(ratingSerializer::deserialize)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList()));

        return described;
    }

    private <C extends Content> C visitContent(C content) {
        content = visitDescribed(content);
        ImmutableSet.Builder<Certificate> certificates = ImmutableSet.builder();
        for (CommonProtos.Certificate cert : msg.getCertificatesList()) {
            certificates.add(certificateSerializer.deserialize(cert));
        }
        content.setCertificates(certificates.build());

        ImmutableSet.Builder<CrewMember> crew = ImmutableSet.builder();
        for (ContentProtos.CrewMember crewMember : msg.getCrewMembersList()) {
            crew.add(crewMemberSerializer.deserialize(crewMember));
        }
        content.setPeople(crew.build().asList());

        ImmutableSet.Builder<Clip> clips = ImmutableSet.builder();
        for (ContentProtos.Content clipPb : msg.getClipsList()) {
            clips.add((Clip) new Clip().accept(new ContentDeserializationVisitor(clipPb)));
        }
        content.setClips(clips.build());

        ImmutableSet.Builder<ContentGroupRef> groupRefs = ImmutableSet.builder();
        for (Reference groupRef : msg.getContentGroupsList()) {
            groupRefs.add(new ContentGroupRef(
                    Id.valueOf(groupRef.getId()), ""
            ));
        }
        content.setContentGroupRefs(groupRefs.build());

        ImmutableSet.Builder<KeyPhrase> phrases = ImmutableSet.builder();
        for (ContentProtos.KeyPhrase phrase : msg.getKeyPhrasesList()) {
            phrases.add(keyPhraseSerializer.deserialize(phrase));
        }
        content.setKeyPhrases(phrases.build());

        content.setLanguages(msg.getLanguageList());

        ImmutableSet.Builder<RelatedLink> links = ImmutableSet.builder();
        for (int i = 0; i < msg.getRelatedLinkCount(); i++) {
            links.add(relatedLinkSerializer.deserialize(msg.getRelatedLink(i)));
        }
        content.setRelatedLinks(links.build());

        ImmutableSet.Builder<Tag> topicRefs = ImmutableSet.builder();
        for (int i = 0; i < msg.getTopicRefsCount(); i++) {
            topicRefs.add(tagSerializer.deserialize(msg.getTopicRefs(i)));
        }
        content.setTags(topicRefs.build());

        ImmutableSet.Builder<EventRef> eventRefs = ImmutableSet.builder();
        for (ContentProtos.EventRef eventRef : msg.getEventRefsList()) {
            eventRefs.add(eventRefSerializer.deserialize(eventRef));
        }
        content.setEventRefs(eventRefs.build());

        if (msg.hasYear()) {
            content.setYear(msg.getYear());
        }

        boolean hasLocationAnnotation = annotations.stream().anyMatch(LOCATIONS_ANNOTATIONS::contains);

        if (hasLocationAnnotation) {
            content.setManifestedAs(getEncodings());
        } else {
            content.setManifestedAs(null);
        }
        if (msg.hasGenericDescription()) {
            content.setGenericDescription(msg.getGenericDescription());
        }
        return content;
    }

    private <C extends Container> C visitContainer(C container) {
        container = visitContent(container);
        ContentRefSerializer refSerializer = new ContentRefSerializer(container.getSource());
        if (annotations.contains(SUB_ITEMS_ANNOTATIONS)) {
            ImmutableSet.Builder<ItemRef> childRefs = ImmutableSet.builder();
            for (int i = 0; i < msg.getChildrenCount(); i++) {
                childRefs.add((ItemRef) refSerializer.deserialize(msg.getChildren(i)));
            }
            container.setItemRefs(Ordering.natural().immutableSortedCopy(childRefs.build()));
        } else {
            container.setItemRefs(null);
        }

        if (annotations.contains(UPCOMING_CONTENT_DETAIL)) {
            ImmutableList.Builder<ContentProtos.ItemAndBroadcastRef> itemAndBroadcastRefBuilder = ImmutableList.<ContentProtos.ItemAndBroadcastRef>builder();
            for (int i = 0; i < msg.getUpcomingContentCount(); i++) {
                itemAndBroadcastRefBuilder.add(msg.getUpcomingContent(i));
            }
            container.setUpcomingContent(
                    itemAndBroadcastRefSerializer.deserialize(
                            itemAndBroadcastRefBuilder.build()
                    )
            );
        } else {
            container.setUpcomingContent(null);
        }

        boolean hasAvailableContentAnnotation =
                annotations.stream().anyMatch(AVAILABLE_CONTENT_ANNOTATIONS::contains);
        if (hasAvailableContentAnnotation) {
            ImmutableList.Builder<ContentProtos.ItemAndLocationSummary> itemAndLocationSummaries = ImmutableList
                    .builder();
            for (int i = 0; i < msg.getAvailableContentCount(); i++) {
                itemAndLocationSummaries.add(msg.getAvailableContent(i));
            }
            container.setAvailableContent(
                    itemAndLocationSummarySerializer.deserialize(
                            itemAndLocationSummaries.build()
                    )
            );
        } else {
            container.setAvailableContent(null);
        }

        if (annotations.contains(SUB_ITEM_SUMMARIES_ANNOTATION) || annotations.contains(EXTENDED_DESCRIPTION)) {
            ImmutableList.Builder<ContentProtos.ItemSummary> itemSummaries = ImmutableList.builder();

            for (int i = 0; i < msg.getItemSummariesCount(); i++) {
                itemSummaries.add(msg.getItemSummaries(i));
            }
            container.setItemSummaries(
                    itemSummarySerializer.deserialize(itemSummaries.build())
            );
        } else {
            container.setItemSummaries(null);
        }

        return container;
    }

    @Override
    public Brand visit(Brand brand) {
        brand = visitContainer(brand);
        ImmutableSet.Builder<SeriesRef> seriesRefs = ImmutableSet.builder();
        ContentRefSerializer refSerializer = new ContentRefSerializer(brand.getSource());
        for (int i = 0; i < msg.getSecondariesCount(); i++) {
            seriesRefs.add((SeriesRef) refSerializer.deserialize(msg.getSecondaries(i)));
        }
        brand.setSeriesRefs(SeriesRef.dedupeAndSort(seriesRefs.build()));
        return brand;
    }

    @Override
    public Series visit(Series series) {
        series = visitContainer(series);
        if (msg.hasContainerRef()) {
            series.setBrandRef(new BrandRef(
                    Id.valueOf(msg.getContainerRef().getId()),
                    Sources.fromPossibleKey(msg.getContainerRef().getSource())
                            .or(series.getSource())
            ));
        }
        series.withSeriesNumber(msg.hasSeriesNumber() ? msg.getSeriesNumber() : null);
        series.setTotalEpisodes(msg.hasTotalEpisodes() ? msg.getTotalEpisodes() : null);
        return series;
    }

    @Override
    public Episode visit(Episode episode) {
        episode = visitItem(episode);
        if (msg.hasSeriesRef()) {
            episode.setSeriesRef(new SeriesRef(
                    Id.valueOf(msg.getSeriesRef().getId()),
                    Sources.fromPossibleKey(msg.getContainerRef().getSource())
                            .or(episode.getSource())
            ));
        }

        episode.setSeriesNumber(msg.hasSeriesNumber() ? msg.getSeriesNumber() : null);
        episode.setEpisodeNumber(msg.hasEpisodeNumber() ? msg.getEpisodeNumber() : null);
        episode.setPartNumber(msg.hasPartNumber() ? msg.getPartNumber() : null);
        episode.setSpecial(msg.hasSpecial() ? msg.getSpecial() : null);

        return episode;
    }

    @Override
    public Film visit(Film film) {
        film = visitItem(film);
        ImmutableSet.Builder<ReleaseDate> releaseDates = ImmutableSet.builder();
        for (int i = 0; i < msg.getReleaseDatesCount(); i++) {
            releaseDates.add(releaseDateSerializer.deserialize(msg.getReleaseDates(i)));
        }
        film.setReleaseDates(releaseDates.build());
        film.setWebsiteUrl(msg.hasWebsiteUrl() ? msg.getWebsiteUrl() : null);
        ImmutableSet.Builder<Subtitles> subtitles = ImmutableSet.builder();
        for (Subtitle sub : msg.getSubtitlesList()) {
            subtitles.add(new Subtitles(sub.getLanguage()));
        }
        film.setSubtitles(subtitles.build());
        return film;
    }

    @Override
    public Song visit(Song song) {
        song = visitItem(song);
        song.setIsrc(msg.hasIsrc() ? msg.getIsrc() : null);
        song.setDuration(msg.hasDuration() ? Duration.millis(msg.getDuration()) : null);
        return song;
    }

    @Override
    public Item visit(Item item) {
        return visitItem(item);
    }

    private <I extends Item> I visitItem(I item) {
        item = visitContent(item);
        if (msg.hasContainerRef()) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(item.getSource());
            item.setContainerRef((ContainerRef) refSerializer.deserialize(msg.getContainerRef()));
        }
        if (msg.hasContainerSummary()) {
            item.setContainerSummary(containerSummarySerializer.deserialize(msg.getContainerSummary()));
        }
        if (msg.hasBlackAndWhite()) {
            item.setBlackAndWhite(msg.getBlackAndWhite());
        }
        item.setCountriesOfOrigin(Countries.fromCodes(msg.getCountriesList()));
        if (msg.hasLongform()) {
            item.setIsLongForm(msg.getLongform());
        }

        boolean hasBroadcastAnnotation = annotations
                .stream()
                .anyMatch(BROADCAST_ANNOTATIONS::contains);
        if (hasBroadcastAnnotation) {
            item.setBroadcasts(getBroadcasts());
        } else {
            item.setBroadcasts(null);
        }
        item.setSegmentEvents(getSegmentEvents());
        item.setRestrictions(getRestrictions());
        return item;
    }

    private ImmutableSet<Broadcast> getBroadcasts() {
        ImmutableSet.Builder<Broadcast> broadcasts = ImmutableSet.builder();
        for (int i = 0; i < msg.getBroadcastsCount(); i++) {
            ContentProtos.Broadcast broadcast = msg.getBroadcasts(i);
            broadcasts.add(broadcastSerializer.deserialize(broadcast));
        }
        return broadcasts.build();
    }

    private ImmutableSet<Encoding> getEncodings() {
        ImmutableSet.Builder<Encoding> encodings = ImmutableSet.builder();
        for (int i = 0; i < msg.getEncodingsCount(); i++) {
            ContentProtos.Encoding encoding = msg.getEncodings(i);
            encodings.add(encodingSerializer.deserialize(encoding));
        }
        return encodings.build();
    }

    private ImmutableSet<SegmentEvent> getSegmentEvents() {
        ImmutableSet.Builder<SegmentEvent> segmentEvents = ImmutableSet.builder();
        for (int i = 0; i < msg.getSegmentEventsCount(); i++) {
            ContentProtos.SegmentEvent segmentEvent = msg.getSegmentEvents(i);
            segmentEvents.add(segmentEventSerializer.deserialize(segmentEvent));
        }
        return segmentEvents.build();
    }

    private ImmutableSet<Restriction> getRestrictions() {
        ImmutableSet.Builder<Restriction> restrictions = ImmutableSet.builder();
        for (int i = 0; i < msg.getRestrictionsCount(); i++) {
            ContentProtos.Restriction segmentEvent = msg.getRestrictions(i);
            restrictions.add(restrictionSerializer.deserialize(segmentEvent));
        }
        return restrictions.build();
    }

    @Override
    public Content visit(Clip clip) {
        return visitItem(clip);
    }

}
