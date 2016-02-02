package org.atlasapi.content;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.CommonProtos.Reference;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Subtitle;
import org.atlasapi.serialization.protobuf.ContentProtos.Synopsis;
import org.atlasapi.source.Sources;
import org.joda.time.Duration;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSet.Builder;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.intl.Countries;

final class ContentDeserializationVisitor implements ContentVisitor<Content> {

    private static final ImageSerializer imageSerializer = new ImageSerializer();
    private static final BroadcastSerializer broadcastSerializer = new BroadcastSerializer();
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

    private ContentProtos.Content msg;

    public ContentDeserializationVisitor(ContentProtos.Content msg) {
        this.msg = msg;
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
            equivRefs.add(new EquivalenceRef(Id.valueOf(equivRef.getId()),
                Sources.fromPossibleKey(equivRef.getSource()).get()
            ));
        }
        identified.setEquivalentTo(equivRefs.build());
        return identified;
    }

    private <D extends Described> D visitDescribed(D described) {
        described = visitIdentified(described);
        described.setPublisher(Sources.fromPossibleKey(msg.getSource()).get());
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
            described.setSpecialization(Specialization.valueOf(msg.getSpecialization().toUpperCase()));
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
        if(msg.hasActivelyPublished()) {
            described.setActivelyPublished(msg.getActivelyPublished());
        }
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
                Id.valueOf(groupRef.getId()),""
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
        for(ContentProtos.EventRef eventRef : msg.getEventRefsList()) {
            eventRefs.add(eventRefSerializer.deserialize(eventRef));
        }
        content.setEventRefs(eventRefs.build());

        if (msg.hasYear()) {
            content.setYear(msg.getYear());
        }
        content.setManifestedAs(getEncodings());
        if (msg.hasGenericDescription()) {
            content.setGenericDescription(msg.getGenericDescription());
        }
        return content;
    }

    private <C extends Container> C visitContainer(C container) {
        container = visitContent(container);
        ContentRefSerializer refSerializer = new ContentRefSerializer(container.getSource());
        ImmutableSet.Builder<ItemRef> childRefs = ImmutableSet.builder();
        for (int i = 0; i < msg.getChildrenCount(); i++) {
            childRefs.add((ItemRef)refSerializer.deserialize(msg.getChildren(i)));
        }
        container.setItemRefs(Ordering.natural().immutableSortedCopy(childRefs.build()));
        ImmutableList.Builder<ContentProtos.ItemAndBroadcastRef> itemAndBroadcastRefBuilder = ImmutableList.<ContentProtos.ItemAndBroadcastRef>builder();
        for (int i = 0; i < msg.getUpcomingContentCount(); i++) {
            itemAndBroadcastRefBuilder.add(msg.getUpcomingContent(i));
        }
        container.setUpcomingContent(
                itemAndBroadcastRefSerializer.deserialize(
                        itemAndBroadcastRefBuilder.build()
                )
        );

        ImmutableList.Builder<ContentProtos.ItemAndLocationSummary> itemAndBroadcastSummaries = ImmutableList.builder();
        for (int i = 0; i < msg.getAvailableContentCount(); i++) {
            itemAndBroadcastSummaries.add(msg.getAvailableContent(i));
        }
        container.setAvailableContent(
                itemAndLocationSummarySerializer.deserialize(
                        itemAndBroadcastSummaries.build()
                )
        );
        ImmutableList.Builder<ContentProtos.ItemSummary> itemSummaries = ImmutableList.builder();

        for (int i = 0; i < msg.getItemSummariesCount(); i++) {
            itemSummaries.add(msg.getItemSummaries(i));
        }
        container.setItemSummaries(
                itemSummarySerializer.deserialize(itemSummaries.build())
        );

        return container;
    }

    @Override
    public Brand visit(Brand brand) {
        brand = visitContainer(brand);
        ImmutableSet.Builder<SeriesRef> seriesRefs = ImmutableSet.builder();
        ContentRefSerializer refSerializer = new ContentRefSerializer(brand.getSource());
        for (int i = 0; i < msg.getSecondariesCount(); i++) {
            seriesRefs.add((SeriesRef)refSerializer.deserialize(msg.getSecondaries(i)));
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
                Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(series.getSource())
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
                Sources.fromPossibleKey(msg.getContainerRef().getSource()).or(episode.getSource())
            ));
        }
        episode.setSeriesNumber(msg.hasSeriesNumber() ? msg.getSeriesNumber() : null);
        episode.setEpisodeNumber(msg.hasEpisodeNumber() ? msg.getEpisodeNumber() : null);
        episode.setPartNumber(msg.hasPartNumber() ? msg.getPartNumber() : null);
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
        item.setBroadcasts(getBroadcasts());
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