package org.atlasapi.system.legacy;

import java.util.List;
import java.util.Optional;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.content.BlackoutRestriction;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.ClipRef;
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.EpisodeRef;
import org.atlasapi.content.FilmRef;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.Location;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Pricing;
import org.atlasapi.content.Provider;
import org.atlasapi.content.Quality;
import org.atlasapi.content.ReleaseDate.ReleaseType;
import org.atlasapi.content.Restriction;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.SongRef;
import org.atlasapi.content.Tag;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.channel.ChannelResolver;
import org.atlasapi.media.entity.Brand;
import org.atlasapi.media.entity.ChildRef;
import org.atlasapi.media.entity.Clip;
import org.atlasapi.media.entity.Container;
import org.atlasapi.media.entity.Content;
import org.atlasapi.media.entity.Episode;
import org.atlasapi.media.entity.Film;
import org.atlasapi.media.entity.Item;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ReleaseDate;
import org.atlasapi.media.entity.Series;
import org.atlasapi.media.entity.Song;
import org.atlasapi.media.entity.Subtitles;
import org.atlasapi.media.entity.TopicRef;
import org.atlasapi.media.entity.Version;
import org.atlasapi.segment.Segment;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;
import org.atlasapi.system.legacy.exception.LegacyChannelNotFoundException;

import com.metabroadcast.common.base.Maybe;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.base.Predicates;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;
import org.joda.time.DateTime;
import org.joda.time.Duration;

import static com.google.common.base.Preconditions.checkNotNull;

public class LegacyContentTransformer
        extends DescribedLegacyResourceTransformer<Content, org.atlasapi.content.Content> {

    private final ChannelResolver channelResolver;
    private final LegacySegmentMigrator legacySegmentMigrator;
    private final LegacyCrewMemberTransformer legacyCrewMemberTransformer;
    private final GenreToTagMapper legacyGenreToTagTransformer;
    private final LegacyContentTopicMerger legacyTagTopicMerger;

    public LegacyContentTransformer(ChannelResolver channelResolver,
            LegacySegmentMigrator segmentMigrator,
            GenreToTagMapper genreToTagTransformer) {
        this.channelResolver = checkNotNull(channelResolver);
        this.legacySegmentMigrator = checkNotNull(segmentMigrator);
        this.legacyCrewMemberTransformer = new LegacyCrewMemberTransformer();
        this.legacyTagTopicMerger = new LegacyContentTopicMerger();
        this.legacyGenreToTagTransformer = checkNotNull(genreToTagTransformer);
    }

    @Override
    protected org.atlasapi.content.Content createDescribed(Content input) {
        org.atlasapi.content.Content c = null;
        if (input instanceof Episode) {
            c = createEpisode((Episode) input);
        } else if (input instanceof Film) {
            c = createFilm((Film) input);
        } else if (input instanceof Song) {
            c = createSong((Song) input);
        } else if (input instanceof Clip) {
            c = createClip((Clip) input);
        } else if (input instanceof Item) {
            c = createItem((Item) input);
        } else if (input instanceof Brand) {
            c = createBrand((Brand) input);
        } else if (input instanceof Series) {
            c = createSeries((Series) input);
        } else if (input instanceof Container) {
            c = createBrand((Container) input);
        }
        return setContentFields(c, input);
    }

    private org.atlasapi.content.Content createBrand(Container input) {
        org.atlasapi.content.Brand b = new org.atlasapi.content.Brand();
        return setContainerFields(b, input);
    }

    private org.atlasapi.content.Content createSeries(Series input) {
        org.atlasapi.content.Series series = new org.atlasapi.content.Series();
        series.withSeriesNumber(input.getSeriesNumber());
        series.setTotalEpisodes(input.getTotalEpisodes());
        if (input.getParent() != null && input.getParent().getId() != null) {
            series.setBrandRef(transformToBrandRef(input.getParent(), input.getPublisher()));
        }
        return setContainerFields(series, input);
    }

    private org.atlasapi.content.Content createBrand(final Brand brand) {
        org.atlasapi.content.Brand b = new org.atlasapi.content.Brand();
        b.setSeriesRefs(Iterables.transform(
                brand.getSeriesRefs(),
                new Function<org.atlasapi.media.entity.SeriesRef, SeriesRef>() {

                    @Override
                    public SeriesRef apply(org.atlasapi.media.entity.SeriesRef input) {
                        return new SeriesRef(Id.valueOf(input.getId()),
                                brand.getPublisher(),
                                input.getTitle(),
                                input.getSeriesNumber(),
                                input.getUpdated(),
                                null,
                                null
                        );
                    }
                }
        ));
        return setContainerFields(b, brand);
    }

    private <C extends org.atlasapi.content.Container> C setContainerFields(C c,
            final Container container) {
        c.setItemRefs(Iterables.filter(Iterables.transform(
                container.getChildRefs(),
                input -> {
                    if (input.getId() == null) {
                        log.warn("no id in ref for {} in {}", input, container);
                        return null;
                    }
                    return legacyRefToRef(input, container.getPublisher());
                }
        ), Predicates.notNull()));
        return c;
    }

    @Nullable
    public static ItemRef legacyRefToRef(ChildRef input, Publisher source) {
        if (input.getId() == null) {
            return null;
        }

        Id id = Id.valueOf(input.getId());
        DateTime updated = Objects.firstNonNull(
                input.getUpdated(),
                new DateTime(DateTimeZones.UTC)
        );

        org.atlasapi.media.entity.EntityType type = transformEnum(
                input.getType(), org.atlasapi.media.entity.EntityType.class
        );

        switch (type) {
        case ITEM:
            return new ItemRef(id, source, input.getSortKey(), updated);
        case CLIP:
            return new ClipRef(id, source, input.getSortKey(), updated);
        case EPISODE:
            return new EpisodeRef(id, source, input.getSortKey(), updated);
        case FILM:
            return new FilmRef(id, source, input.getSortKey(), updated);
        case SONG:
            return new SongRef(id, source, input.getSortKey(), updated);
        case CONTAINER:
        case BRAND:
        case SERIES:
        case PERSON:
        case CONTENT_GROUP:
        default:
            return null;
        }
    }

    private org.atlasapi.content.Content createClip(Clip input) {
        org.atlasapi.content.Clip c = new org.atlasapi.content.Clip();
        return setItemFields(c, input);
    }

    private org.atlasapi.content.Content createItem(Item input) {
        return setItemFields(new org.atlasapi.content.Item(), input);
    }

    private <I extends org.atlasapi.content.Item> I setItemFields(I i, Item input) {
        if (input.getContainer() != null) {
            i.setContainerRef(new BrandRef(
                    Id.valueOf(input.getContainer().getId()),
                    input.getPublisher()
            ));
        }
        transformVersions(i, input.getVersions());
        i.setIsLongForm(input.getIsLongForm());
        i.setBlackAndWhite(input.getBlackAndWhite());
        i.withSortKey(input.sortKey());
        if(input.getDuration() != null) {
            i.setDuration(input.getDuration());
        }
        return i;
    }

    private <I extends org.atlasapi.content.Item> void transformVersions(I i,
            Set<org.atlasapi.media.entity.Version> versions) {
        i.setRestrictions(getRestrictions(versions));
        i.setBroadcasts(getBroadcasts(versions));
        i.setSegmentEvents(getSegmentEvents(versions));
    }

    private <I extends org.atlasapi.content.Content> void transformEncodings(I i,
            Set<org.atlasapi.media.entity.Version> versions) {
        i.setManifestedAs(getEncodings(versions));
    }

    private Set<Restriction> getRestrictions(Set<Version> versions) {
        return Sets.newHashSet(Iterables.transform(versions, new Function<Version, Restriction>() {

            @Override
            public Restriction apply(Version version) {
                return transformRestriction(version.getRestriction());
            }
        }));
    }

    private Set<Encoding> getEncodings(Set<Version> versions) {
        Set<Encoding> encodings = Sets.newHashSet();
        for (Version version : versions) {
            for (org.atlasapi.media.entity.Encoding encoding : version.getManifestedAs()) {
                encodings.add(transformEncoding(encoding, version));
            }
        }
        return encodings;
    }

    private Set<Broadcast> getBroadcasts(Set<Version> versions) {
        Set<Broadcast> broadcasts = Sets.newHashSet();
        for (Version version : versions) {
            for (org.atlasapi.media.entity.Broadcast broadcast : broadcastsWithIds(version)) {
                try {
                    broadcasts.add(transformBroadcast(broadcast, version));
                } catch (LegacyChannelNotFoundException e) {
                    log.warn(
                            "{}, Broadcast with source id: {}, version id: {}",
                            e.getMessage(),
                            broadcast.getSourceId(),
                            version.getId()
                    );
                }
            }
        }
        return broadcasts;
    }

    private Iterable<SegmentEvent> getSegmentEvents(Set<Version> versions) {
        Set<SegmentEvent> segEvents = Sets.newHashSet();
        for (Version version : versions) {
            for (org.atlasapi.media.segment.SegmentEvent segementEvent : version.getSegmentEvents()) {
                try {
                    segEvents.add(transformSegmentEvent(segementEvent, version));
                } catch (UnresolvedLegacySegmentException e) {
                    log.warn("Failed to transform legacy segment - {}", e.toString());
                }
            }
        }
        return segEvents;
    }

    private Set<org.atlasapi.media.entity.Broadcast> broadcastsWithIds(
            org.atlasapi.media.entity.Version input) {
        return Sets.filter(
                input.getBroadcasts(),
                new Predicate<org.atlasapi.media.entity.Broadcast>() {

                    @Override
                    public boolean apply(org.atlasapi.media.entity.Broadcast input) {
                        if (input.getSourceId() == null) {
                            log.warn("Broadcast with null sourceId will likely be ignored; tx start {} channel {}",
                                    input.getTransmissionTime(),
                                    input.getBroadcastOn());
                        }
                        return input.getSourceId() != null;
                    }
                }
        );
    }

    private SegmentEvent transformSegmentEvent(org.atlasapi.media.segment.SegmentEvent input,
            Version version)
            throws UnresolvedLegacySegmentException {

        SegmentEvent se = new SegmentEvent();
        setIdentifiedFields(se, input);
        se.setOffset(input.getOffset());
        se.setIsChapter(input.getIsChapter());
        org.atlasapi.media.entity.Description d = input.getDescription();
        se.setDescription(new Description(
                d.getTitle(),
                d.getSynopsis(),
                d.getImage(),
                d.getThumbnail()
        ));
        org.atlasapi.media.segment.SegmentRef sr = input.getSegment();
        Id segmentId = Id.valueOf(sr.identifier());
        WriteResult<Segment, Segment> legacyMigrationResult = legacySegmentMigrator.migrateLegacySegment(
                segmentId);
        se.setSegment(new SegmentRef(segmentId, legacyMigrationResult.getResource().getSource()));
        se.setVersionId(version.getCanonicalUri());
        return se;
    }

    private Encoding transformEncoding(org.atlasapi.media.entity.Encoding input, Version version) {
        Encoding e = new Encoding();
        // setIdentifiedFields(e, input); // , are not accessed in Deer model, see EncodingSerializer
        e.setAvailableAt(transformLocations(input));
        e.setContainsAdvertising(input.getContainsAdvertising());
        e.setAdvertisingDuration(input.getAdvertisingDuration());
        e.setBitRate(input.getBitRate());
        e.setAudioBitRate(input.getAudioBitRate());
        e.setAudioChannels(input.getAudioChannels());
        e.setAudioCoding(input.getAudioCoding());
        e.setVideoAspectRatio(input.getVideoAspectRatio());
        e.setVideoBitRate(input.getVideoBitRate());
        e.setVideoCoding(input.getVideoCoding());
        e.setVideoFrameRate(input.getVideoFrameRate());
        e.setVideoHorizontalSize(input.getVideoHorizontalSize());
        e.setVideoProgressiveScan(input.getVideoProgressiveScan());
        e.setVideoVerticalSize(input.getVideoVerticalSize());
        e.setDataSize(input.getDataSize());
        e.setDataContainerFormat(input.getDataContainerFormat());
        e.setSource(input.getSource());
        e.setDistributor(input.getDistributor());
        e.setHasDOG(input.getHasDOG());
        e.set3d(version.is3d());
        e.setVersionId(version.getCanonicalUri());
        if (version.getDuration() != null) {
            e.setDuration(Duration.standardSeconds(version.getDuration()));
        }
        if (input.getQuality() != null) {
            e.setQuality(Quality.valueOf(input.getQuality().name()));
        } else {
            if (Boolean.TRUE.equals(input.getHighDefinition())) {
                e.setQuality(Quality.HD);
            }
        }
        e.setQualityDetail(input.getQualityDetail());
        return e;
    }

    private ImmutableSet<Location> transformLocations(org.atlasapi.media.entity.Encoding input) {
        return ImmutableSet.copyOf(Iterables.transform(
                input.getAvailableAt(),
                new Function<org.atlasapi.media.entity.Location, Location>() {

                    @Override
                    public Location apply(org.atlasapi.media.entity.Location input) {
                        return transformLocation(input);
                    }
                }
        ));
    }

    private Location transformLocation(org.atlasapi.media.entity.Location input) {
        Location l = new Location();
        setIdentifiedFields(l, input);
        l.setAvailable(input.getAvailable());
        l.setTransportIsLive(input.getTransportIsLive());
        l.setTransportSubType(transformEnum(input.getTransportSubType(), TransportSubType.class));
        l.setTransportType(transformEnum(input.getTransportType(), TransportType.class));
        l.setUri(input.getUri());
        l.setEmbedCode(input.getEmbedCode());
        l.setEmbedId(input.getEmbedId());
        if(input.getProvider() != null) {
            l.setProvider(transformProvider(input.getProvider()));
        }
        l.setPolicy(transformPolicy(input.getPolicy()));
        return l;
    }

    private Provider transformProvider(org.atlasapi.media.entity.Provider inputProvider) {
        Provider provider = new Provider();
        provider.setName(inputProvider.getName());
        provider.setIconUrl(inputProvider.getIconUrl());
        return provider;
    }

    private Policy transformPolicy(org.atlasapi.media.entity.Policy input) {
        // policies are optional
        if (null == input) {
            return null;
        }

        Policy p = new Policy();
        // setIdentifiedFields(p, input); // Policy identified parts are not accessed in Deer model, see LocationSerializer.
        p.setAvailabilityStart(input.getAvailabilityStart());
        p.setAvailabilityEnd(input.getAvailabilityEnd());
        p.setDrmPlayableFrom(input.getDrmPlayableFrom());
        p.setAvailableCountries(input.getAvailableCountries());
        p.setAvailabilityLength(input.getAvailabilityLength());
        p.setRevenueContract(transformEnum(
                input.getRevenueContract(),
                Policy.RevenueContract.class
        ));
        p.setPrice(input.getPrice());
        p.setPlatform(transformEnum(input.getPlatform(), Policy.Platform.class));
        p.setSubscriptionPackages(input.getSubscriptionPackages());
        if (input.getService() != null) {
            p.setServiceId(Id.valueOf(input.getService()));
        }
        if (input.getPlayer() != null) {
            p.setPlayerId(Id.valueOf(input.getPlayer()));
        }
        p.setNetwork(transformEnum(input.getNetwork(), Policy.Network.class));
        p.setActualAvailabilityStart(input.getActualAvailabilityStart());
        p.setPricing(
                input.getPricing()
                        .stream()
                        .map(legacy -> new Pricing(
                                legacy.getStartTime(),
                                legacy.getEndTime(),
                                legacy.getPrice()
                        ))
                        .collect(MoreCollectors.toImmutableList())
        );
        return p;
    }

    private Broadcast transformBroadcast(org.atlasapi.media.entity.Broadcast input,
            Version version) {
        Broadcast b = new Broadcast(channelId(input.getBroadcastOn()),
                input.getTransmissionTime(), input.getTransmissionEndTime()
        );
        setIdentifiedFields(b, input);
        b.setScheduleDate(input.getScheduleDate());
        b.setIsActivelyPublished(input.isActivelyPublished());
        b.withId(input.getSourceId());
        b.setRepeat(input.getRepeat());
        b.setSubtitled(input.getSubtitled());
        b.setSigned(input.getSigned());
        b.setAudioDescribed(input.getAudioDescribed());
        b.setHighDefinition(input.getHighDefinition());
        b.setWidescreen(input.getWidescreen());
        b.setSurround(input.getSurround());
        b.setLive(input.getLive());
        b.setNewSeries(input.getNewSeries());
        b.setNewEpisode(input.getNewEpisode());
        b.setNewOneOff(input.getNewOneOff());
        b.setPremiere(input.getPremiere());
        b.setContinuation(input.getContinuation());
        b.set3d(version.is3d());
        b.setVersionId(version.getCanonicalUri());
        b.setBlackoutRestriction(transformBlackoutRestriction(input.getBlackoutRestriction()));
        b.setRevisedRepeat(input.getRevisedRepeat());
        return b;
    }

    private BlackoutRestriction transformBlackoutRestriction(
            org.atlasapi.media.entity.BlackoutRestriction legacy) {
        if (legacy == null) {
            return null;
        }
        return new BlackoutRestriction(legacy.getAll());
    }

    private Id channelId(String broadcastOn) {
        Maybe<Channel> possibleChannel = channelResolver.fromUri(broadcastOn);
        if (possibleChannel.isNothing()) {
            throw new LegacyChannelNotFoundException("no channel found for uri " + broadcastOn);
        }
        return Id.valueOf(possibleChannel.requireValue().getId());
    }

    private Restriction transformRestriction(org.atlasapi.media.entity.Restriction input) {
        Restriction r = new Restriction();
        //setIdentifiedFields(r, input);
        r.setRestricted(input.isRestricted());
        r.setMinimumAge(input.getMinimumAge());
        r.setMessage(input.getMessage());
        r.setAuthority(input.getAuthority());
        r.setRating(input.getRating());
        return r;
    }

    private org.atlasapi.content.Content createSong(Song input) {
        org.atlasapi.content.Song s = new org.atlasapi.content.Song();
        s.setIsrc(input.getIsrc());
        return setItemFields(s, input);
    }

    private org.atlasapi.content.Content createFilm(Film input) {
        org.atlasapi.content.Film f = new org.atlasapi.content.Film();
        f.setWebsiteUrl(input.getWebsiteUrl());
        f.setSubtitles(Iterables.transform(
                input.getSubtitles(),
                new Function<Subtitles, org.atlasapi.content.Subtitles>() {

                    @Override
                    public org.atlasapi.content.Subtitles apply(Subtitles input) {
                        return new org.atlasapi.content.Subtitles(input.code());
                    }
                }
        ));
        f.setReleaseDates(Iterables.transform(
                input.getReleaseDates(),
                new Function<ReleaseDate, org.atlasapi.content.ReleaseDate>() {

                    @Override
                    public org.atlasapi.content.ReleaseDate apply(ReleaseDate input) {
                        ReleaseType type = transformEnum(input.type(), ReleaseType.class);
                        return new org.atlasapi.content.ReleaseDate(
                                input.date(),
                                input.country(),
                                type
                        );
                    }
                }
        ));
        return setItemFields(f, input);
    }

    private org.atlasapi.content.Content createEpisode(Episode input) {
        org.atlasapi.content.Episode e = new org.atlasapi.content.Episode();
        e.setEpisodeNumber(input.getEpisodeNumber());
        e.setPartNumber(e.getPartNumber());
        e.setSeriesNumber(input.getSeriesNumber());
        if (input.getSeriesRef() != null) {
            e.setSeriesRef(transformToSeriesRef(input.getSeriesRef(), input.getPublisher()));
        }
        e.setSpecial(input.getSpecial());
        return setItemFields(e, input);
    }

    private SeriesRef transformToSeriesRef(org.atlasapi.media.entity.ParentRef seriesRef,
            Publisher publisher) {
        return new SeriesRef(
                Id.valueOf(seriesRef.getId()),
                publisher,
                null,
                null,
                null,
                null,
                null
        );
    }

    private BrandRef transformToBrandRef(org.atlasapi.media.entity.ParentRef ref,
            Publisher publisher) {
        return new BrandRef(Id.valueOf(ref.getId()), publisher);
    }

    private <C extends org.atlasapi.content.Content> C setContentFields(C c, Content input) {
        c.setYear(input.getYear());
        c.setLanguages(input.getLanguages());
        c.setCountriesOfOrigin(input.getCountriesOfOrigin());

        c.setTags(
                translateTopicRefs(
                        legacyTagTopicMerger.mergeTags(
                                input.getTopicRefs(),
                                legacyGenreToTagTransformer.mapGenresToTopicRefs(input.getGenres())
                        )));

        c.setGenericDescription(input.getGenericDescription());
        c.setEventRefs(translateEventRefs(input.events()));
        c.setCertificates(input.getCertificates().stream()
                .map(certificate -> new Certificate(
                        certificate.classification(),
                        certificate.country()
                ))
                .collect(Collectors.toList()));

        c.setClips(transformClipsOfContent(input));

        c.setPeople(
                input.people().stream()
                        .map(legacyCrewMemberTransformer::apply)
                        .filter(java.util.Objects::nonNull)
                        .collect(Collectors.toList())
        );

        transformEncodings(c, input.getVersions());
        return c;
    }

    private Iterable<org.atlasapi.content.Clip> transformClipsOfContent(Content inputContent) {
        List<Clip> input = inputContent.getClips();
        String id = java.util.Objects.toString(inputContent.getCanonicalUri(), "unknown");

        log.debug("Legacy '" + id + "': Clips in count=" + input.size());

        List<org.atlasapi.content.Clip> result = input.stream()
                .map(this::transformClip)
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());

        log.debug("Legacy '" + id + "': Clips out count=" + result.size());

        return result;
    }

    private Optional<org.atlasapi.content.Clip> transformClip(Clip inputClip) {
        try {
            return Optional.of((org.atlasapi.content.Clip) apply(inputClip));
        } catch(NullPointerException e) {
            log.warn("Unable to process Clip of: '{}'", inputClip.getClipOf());
            return Optional.empty();
        }
    }



    private Iterable<Tag> translateTopicRefs(Iterable<TopicRef> topicRefs) {
        return StreamSupport.stream(topicRefs.spliterator(), false)
                .map(tr -> Tag.valueOf(
                        Id.valueOf(tr.getTopic()),
                        tr.getWeighting(),
                        tr.isSupervised(),
                        Tag.Relationship.valueOf(tr.getRelationship().name())
                        )
                )
                .collect(MoreCollectors.toImmutableList());
    }

    private Iterable<EventRef> translateEventRefs(
            List<org.atlasapi.media.entity.EventRef> eventRefs) {
        return eventRefs.stream().map(eventRef ->
                new EventRef(Id.valueOf(eventRef.id()), eventRef.getPublisher()))
                .collect(MoreCollectors.toImmutableList());
    }

    @Override
    protected Iterable<Alias> moreAliases(Content input) {
        return ImmutableList.of();
    }

}
