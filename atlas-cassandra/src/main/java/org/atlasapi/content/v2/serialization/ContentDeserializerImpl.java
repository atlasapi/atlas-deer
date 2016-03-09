package org.atlasapi.content.v2.serialization;

import java.io.IOException;
import java.util.Currency;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Description;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Pricing;
import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.Quality;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Song;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.content.v2.model.udt.Clip;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.Encoding;
import org.atlasapi.content.v2.model.udt.ItemSummary;
import org.atlasapi.content.v2.model.udt.Location;
import org.atlasapi.content.v2.model.udt.Priority;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.Restriction;
import org.atlasapi.content.v2.model.udt.SegmentEvent;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.content.v2.model.udt.Tag;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentRef;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Countries;
import com.metabroadcast.common.media.MimeType;

import com.codepoetics.protonpack.maps.MapStream;
import com.fasterxml.jackson.databind.JavaType;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.datatype.joda.JodaModule;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentDeserializerImpl implements ContentDeserializer {

    private static final ObjectMapper mapper;
    private static final JavaType clipType;
    private static final JavaType encodingType;

    static {
        mapper = new ObjectMapper();
        mapper.registerModule(new JodaModule());
        clipType = mapper.getTypeFactory().constructCollectionType(List.class, Clip.class);
        encodingType = mapper.getTypeFactory().constructCollectionType(List.class, Encoding.class);
    }

    @Override
    public Content deserialize(Iterable<org.atlasapi.content.v2.model.Content> rows) {
        org.atlasapi.content.v2.model.Content main = null, clips = null, encodings = null;

        for (org.atlasapi.content.v2.model.Content row : rows) {
            switch (row.getDiscriminator()) {
                case org.atlasapi.content.v2.model.Content.ROW_MAIN:
                    main = row;
                    break;
                case org.atlasapi.content.v2.model.Content.ROW_CLIPS:
                    clips = row;
                    break;
                case org.atlasapi.content.v2.model.Content.ROW_ENCODINGS:
                    encodings = row;
                    break;
                default:
                    throw new IllegalArgumentException(String.format(
                            "Illegal row discriminator: %s",
                            row.getDiscriminator()
                    ));
            }
        }

        Content content = makeEmptyContent(checkNotNull(main));
        setContentFields(content, main);

        switch (main.getType()) {
            case "item":
                setItemFields(content, main);
                break;
            case "song":
                setItemFields(content, main);
                setSongFields(content, main);
                break;
            case "episode":
                setItemFields(content, main);
                setEpisodeFields(content, main);
                break;
            case "film":
                setItemFields(content, main);
                setFilmFields(content, main);
                break;
            case "brand":
                setContainerFields(content, main);
                setBrandFields(content, main);
                break;
            case "series":
                setContainerFields(content, main);
                setSeriesFields(content, main);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format("Illegal object type: %s", main.getType())
                );
        }

        try {
            if (clips != null) {
                List<Clip> clipList = mapper.readValue(clips.getJsonBlob(), clipType);
                content.setClips(clipList.stream()
                        .map(this::makeClip)
                        .collect(Collectors.toList()));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        try {
            if (encodings != null) {
                List<Encoding> encodingList = mapper.readValue(encodings.getJsonBlob(), encodingType);
                content.setManifestedAs(encodingList.stream()
                        .map(this::makeEncoding)
                        .collect(Collectors.toSet()));
            }
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }

        return content;
    }

    private void setSeriesFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Series series = (Series) content;

        series = series.withSeriesNumber(internal.getSeriesNumber());
        series.setTotalEpisodes(internal.getTotalEpisodes());
        series.setBrandRef(makeBrandRef(internal.getBrandRef()));
    }

    private BrandRef makeBrandRef(Ref brandRef) {
        return new BrandRef(
                Id.valueOf(brandRef.getId()),
                Publisher.fromKey(brandRef.getSource()).requireValue()
        );
    }

    private void setBrandFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Brand brand = (Brand) content;

        brand.setSeriesRefs(internal.getSeriesRefs()
                .stream()
                .map(this::makeSeriesRef)
                .collect(Collectors.toList()));
    }

    private void setContainerFields(Content content,
            org.atlasapi.content.v2.model.Content internal) {
        Container container = (Container) content;

        container.setItemRefs(internal.getItemRefs().stream()
                .map(this::makeItemRef)
                .collect(Collectors.toList()));

        Map<ItemRef, Iterable<BroadcastRef>> upcomingContent =
                MapStream.of(internal.getUpcomingContent())
                .mapEntries(
                        this::makeItemRef,
                        broadcastRefs -> (Iterable<BroadcastRef>) broadcastRefs.stream()
                                .map(this::makeBroadcastRef)
                                .collect(Collectors.toList())
                ).collect();

        container.setUpcomingContent(upcomingContent);

        Map<ItemRef, Iterable<LocationSummary>> availableContent = MapStream.of(
                internal.getAvailableContent()
        ).mapEntries(
                this::makeItemRef,
                locationSummaries -> (Iterable<org.atlasapi.content.LocationSummary>)
                        locationSummaries.stream()
                                .map(this::makeLocationSummary)
                                .collect(Collectors.toList())
                ).collect();
        container.setAvailableContent(availableContent);

        container.setItemSummaries(internal.getItemSummaries()
                .stream()
                .map(this::makeItemSummary)
                .collect(Collectors.toList()));
    }

    private org.atlasapi.content.ItemSummary makeItemSummary(ItemSummary is) {
        return new org.atlasapi.content.ItemSummary(
                makeItemRef(is.getRef()),
                is.getTitle(),
                is.getDescription(),
                is.getImage(),
                is.getReleaseYear(),
                is.getCertificate()
                        .stream()
                        .map(this::makeCertificate)
                        .collect(Collectors.toList())
        );
    }

    private LocationSummary makeLocationSummary(
            org.atlasapi.content.v2.model.udt.LocationSummary ls) {
        return new LocationSummary(
                ls.getAvailable(),
                ls.getUri(),
                ls.getStart().toDateTime(DateTimeZone.UTC),
                ls.getEnd().toDateTime(DateTimeZone.UTC));
    }

    private BroadcastRef makeBroadcastRef(org.atlasapi.content.v2.model.udt.BroadcastRef br) {
        return new BroadcastRef(
                br.getSourceId(),
                Id.valueOf(br.getChannelId()),
                new Interval(br.getStart(), br.getEnd())
        );
    }

    private ItemRef makeItemRef(org.atlasapi.content.v2.model.udt.ItemRef itemRef) {
        return new ItemRef(
                Id.valueOf(itemRef.getRef().getId()),
                Publisher.fromKey(itemRef.getRef().getSource()).requireValue(),
                itemRef.getSortKey(),
                itemRef.getUpdated().toDateTime(DateTimeZone.UTC)
        );
    }

    private void setFilmFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Film film = (Film) content;

        film.setWebsiteUrl(internal.getWebsiteUrl());
        film.setSubtitles(internal.getSubtitles()
                .stream()
                .map(Subtitles::new)
                .collect(Collectors.toSet()));
        film.setReleaseDates(internal.getReleaseDates()
                .stream()
                .map(rd -> new ReleaseDate(
                        rd.getReleaseDate(),
                        Countries.fromCode(rd.getCountry()),
                        ReleaseDate.ReleaseType.valueOf(rd.getType())))
                .collect(Collectors.toSet()));
    }

    private void setEpisodeFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Episode episode = (Episode) content;

        episode.setSeriesNumber(internal.getSeriesNumber());
        episode.setEpisodeNumber(internal.getEpisodeNumber());
        episode.setPartNumber(internal.getPartNumber());
        episode.setSpecial(internal.getSpecial());

        List<org.atlasapi.content.v2.model.udt.SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null && !seriesRefs.isEmpty()) {
            episode.setSeriesRef(makeSeriesRef(Iterables.getOnlyElement(seriesRefs)));
        }
    }

    private SeriesRef makeSeriesRef(org.atlasapi.content.v2.model.udt.SeriesRef ref) {
        return new SeriesRef(
                Id.valueOf(ref.getRef().getId()),
                Publisher.fromKey(ref.getRef().getSource()).requireValue(),
                ref.getTitle(),
                ref.getSeriesNumber(),
                ref.getUpdated().toDateTime(DateTimeZone.UTC),
                ref.getReleaseYear(),
                ref.getCertificates()
                        .stream()
                        .map(this::makeCertificate)
                        .collect(Collectors.toList())
        );
    }

    private Certificate makeCertificate(org.atlasapi.content.v2.model.udt.Certificate cert) {
        return new Certificate(cert.getClassification(), Countries.fromCode(cert.getCountryCode()));
    }

    private void setSongFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Song song = (Song) content;

        song.setIsrc(internal.getIsrc());
        Long duration = internal.getDuration();
        if (duration != null) {
            song.setDuration(new Duration(duration));
        }
    }

    private void setItemFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Item item = (Item) content;

        ContainerRef icr = internal.getContainerRef();
        item.setContainerRef(makeContainerRef(icr));

        Boolean isLongForm = internal.getIsLongForm();
        if (isLongForm != null) {
            item.setIsLongForm(isLongForm);
        }

        item.setBlackAndWhite(internal.getBlackAndWhite());

        Set<String> countriesOfOrigin = internal.getCountriesOfOrigin();
        if (countriesOfOrigin != null) {
            item.setCountriesOfOrigin(countriesOfOrigin.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        item = item.withSortKey(internal.getSortKey());

        org.atlasapi.content.v2.model.udt.ContainerSummary internalContainerSummary =
                internal.getContainerSummary();
        if (internalContainerSummary != null) {
            item.setContainerSummary(new ContainerSummary(
                    internalContainerSummary.getType(),
                    internalContainerSummary.getTitle(),
                    internalContainerSummary.getDescription(),
                    internalContainerSummary.getSeriesNumber()
            ));
        }

        Set<org.atlasapi.content.v2.model.udt.Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            item.setBroadcasts(broadcasts.stream()
                    .map(this::makeBroadcast)
                    .collect(Collectors.toSet()));
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            item.setSegmentEvents(segmentEvents.stream()
                    .map(this::makeSegmentEvent)
                    .collect(Collectors.toList()));
        }

        Set<Restriction> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            item.setRestrictions(restrictions.stream()
                    .map(this::makeRestriction)
                    .collect(Collectors.toSet()));
        }
    }

    private org.atlasapi.content.Restriction makeRestriction(Restriction internal) {

        org.atlasapi.content.Restriction restriction = new org.atlasapi.content.Restriction();

        Long id = internal.getId();
        if (id != null) {
            restriction.setId(id);
        }
        restriction.setCanonicalUri(internal.getCanonicalUri());
        restriction.setCurie(internal.getCurie());
        restriction.setAliasUrls(internal.getAliasUrls());
        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            restriction.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            restriction.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        restriction.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        restriction.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        restriction.setRestricted(internal.getRestricted());
        restriction.setMinimumAge(internal.getMinimumAge());
        restriction.setMessage(internal.getMessage());
        restriction.setAuthority(internal.getAuthority());
        restriction.setRating(internal.getRating());

        return restriction;
    }

    private org.atlasapi.segment.SegmentEvent makeSegmentEvent(SegmentEvent internal) {
        org.atlasapi.segment.SegmentEvent segment = new org.atlasapi.segment.SegmentEvent();

        segment.setId(internal.getId());
        segment.setCanonicalUri(internal.getCanonicalUri());
        segment.setCurie(internal.getCurie());
        segment.setAliasUrls(internal.getAliasUrls());
        segment.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        segment.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
        segment.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        segment.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        segment.setPosition(internal.getPosition());
        Long offset = internal.getOffset();
        if (offset != null) {
            segment.setOffset(new Duration(offset));
        }
        segment.setIsChapter(internal.getIsChapter());
        org.atlasapi.content.v2.model.udt.Description description = internal.getDescription();
        if (description != null) {
            segment.setDescription(new Description(
                    description.getTitle(),
                    description.getSynopsis(),
                    description.getImage(),
                    description.getThumbnail()
            ));
        }
        Ref segmentRef = internal.getSegmentRef();
        if (segmentRef != null) {
            segment.setSegment(new SegmentRef(
                    Id.valueOf(segmentRef.getId()),
                    Publisher.fromKey(segmentRef.getSource()).requireValue()
            ));
        }
        segment.setVersionId(internal.getVersionId());

        String publisher = internal.getPublisher();
        if (publisher != null) {
            segment.setPublisher(Publisher.fromKey(publisher).requireValue());
        }

        return segment;
    }

    private Broadcast makeBroadcast(org.atlasapi.content.v2.model.udt.Broadcast internal) {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(internal.getChannelId()),
                convertDateTime(internal.getTransmissionStart()),
                convertDateTime(internal.getTransmissionEnd()),
                internal.getActivelyPublished()
        );

        Long id = internal.getId();
        if (id != null) {
            broadcast.setId(id);
        }
        broadcast.setCanonicalUri(internal.getCanonicalUri());
        broadcast.setCurie(internal.getCurie());
        broadcast.setAliasUrls(internal.getAliasUrls());
        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            broadcast.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            broadcast.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        broadcast.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        broadcast.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        broadcast.setScheduleDate(convertDate(internal.getScheduleDate()));
        broadcast.setIsActivelyPublished(internal.getActivelyPublished());
        broadcast = broadcast.withId(internal.getSourceId());
        broadcast.setVersionId(internal.getVersionId());
        broadcast.setRepeat(internal.getRepeat());
        broadcast.setSubtitled(internal.getSubtitled());
        broadcast.setSigned(internal.getSigned());
        broadcast.setAudioDescribed(internal.getAudioDescribed());
        broadcast.setHighDefinition(internal.getHighDefinition());
        broadcast.setWidescreen(internal.getWidescreen());
        broadcast.setSurround(internal.getSurround());
        broadcast.setLive(internal.getLive());
        broadcast.setNewSeries(internal.getNewSeries());
        broadcast.setNewEpisode(internal.getNewEpisode());
        broadcast.setPremiere(internal.getPremiere());
        broadcast.set3d(internal.getIs3d());
        broadcast.setBlackoutRestriction(
                new org.atlasapi.content.BlackoutRestriction(internal.getBlackoutRestriction()));

        return broadcast;
    }

    private DateTime convertDateTime(Instant dateTime) {
        if (dateTime == null) {
            return null;
        }
        return dateTime.toDateTime(DateTimeZone.UTC);
    }

    private org.joda.time.LocalDate convertDate(org.joda.time.LocalDate date) {
        return date;
    }

    private void setContentFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        setIdentifiedFields(content, internal);
        setDescribedFields(content, internal);

        Set<org.atlasapi.content.v2.model.udt.KeyPhrase> keyPhrases = internal.getKeyPhrases();
        if (keyPhrases != null) {
            content.setKeyPhrases(keyPhrases.stream()
                    .map(kp -> new KeyPhrase(kp.getPhrase(), kp.getWeighting()))
                    .collect(Collectors.toList()));
        }

        List<Tag> tags = internal.getTags();
        if (tags != null) {
            content.setTags(tags.stream()
                    .map(this::makeTag)
                    .collect(Collectors.toList()));
        }

        List<org.atlasapi.content.v2.model.udt.ContentGroupRef> contentGroupRefs =
                internal.getContentGroupRefs();
        if (contentGroupRefs != null) {
            content.setContentGroupRefs(contentGroupRefs.stream()
                    .map(ref -> new ContentGroupRef(Id.valueOf(ref.getId()), ref.getUri()))
                    .collect(Collectors.toList()));
        }

        List<CrewMember> people = internal.getPeople();
        if (people != null) {
            content.setPeople(people.stream()
                    .map(this::makeCrewMember)
                    .collect(Collectors.toList()));
        }

        Set<String> languages = internal.getLanguages();
        if (languages != null) {
            content.setLanguages(languages);
        }

        Set<org.atlasapi.content.v2.model.udt.Certificate> certificates = internal.getCertificates();
        if (certificates != null) {
            content.setCertificates(certificates.stream()
                    .map(cert -> new Certificate(
                            cert.getClassification(),
                            Countries.fromCode(cert.getCountryCode())))
                    .collect(Collectors.toList()));
        }

        content.setYear(internal.getYear());

        content.setGenericDescription(internal.getGenericDescription());

        Set<Ref> eventRefs = internal.getEventRefs();
        if (eventRefs != null) {
            content.setEventRefs(eventRefs.stream()
                    .map(ref -> new EventRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()))
                    .collect(Collectors.toList()));
        }
    }

    private org.atlasapi.content.Encoding makeEncoding(Encoding internal) {
        org.atlasapi.content.Encoding encoding = new org.atlasapi.content.Encoding();

        Long id = internal.getId();
        if (id != null) {
            encoding.setId(id);
        }

        encoding.setCanonicalUri(internal.getCanonicalUri());
        encoding.setCurie(internal.getCurie());
        encoding.setAliasUrls(internal.getAliasUrls());
        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            encoding.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            encoding.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        encoding.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        encoding.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        Set<Location> availableAt = internal.getAvailableAt();
        if (availableAt != null) {
            encoding.setAvailableAt(availableAt.stream()
                    .map(this::makeLocation)
                    .collect(Collectors.toSet()));
        }

        encoding.setContainsAdvertising(internal.getContainsAdvertising());
        encoding.setAdvertisingDuration(internal.getAdvertisingDuration());
        encoding.setDuration(new Duration(internal.getDuration()));
        encoding.setBitRate(internal.getBitRate());
        encoding.setAudioBitRate(internal.getAudioBitRate());
        encoding.setAudioChannels(internal.getAudioChannels());

        String audioCoding = internal.getAudioCoding();
        if (audioCoding != null) {
            encoding.setAudioCoding(MimeType.valueOf(audioCoding));
        }

        encoding.setVideoAspectRatio(internal.getVideoAspectRatio());

        encoding.setVideoBitRate(internal.getVideoBitRate());

        String videoCoding = internal.getVideoCoding();
        if (videoCoding != null) {
            encoding.setVideoCoding(MimeType.valueOf(videoCoding));
        }

        encoding.setVideoFrameRate(internal.getVideoFrameRate());
        encoding.setVideoHorizontalSize(internal.getVideoHorizontalSize());
        encoding.setVideoVerticalSize(internal.getVideoVerticalSize());
        encoding.setVideoProgressiveScan(internal.getVideoProgressiveScan());

        encoding.setDataSize(internal.getDataSize());

        String dataContainerFormat = internal.getDataContainerFormat();
        if (dataContainerFormat != null) {
            encoding.setDataContainerFormat(MimeType.valueOf(dataContainerFormat));
        }

        encoding.setSource(internal.getSource());
        encoding.setDistributor(internal.getDistributor());
        encoding.setHasDOG(internal.getHasDog());
        encoding.set3d(internal.getIs3d());

        String quality = internal.getQuality();
        if (quality != null) {
            encoding.setQuality(Quality.valueOf(quality));
        }

        encoding.setQualityDetail(internal.getQualityDetail());

        encoding.setVersionId(internal.getVersionId());

        return encoding;
    }

    private org.atlasapi.content.Location makeLocation(Location internal) {
        org.atlasapi.content.Location location = new org.atlasapi.content.Location();

        Long id = internal.getId();
        if (id != null) {
            location.setId(id);
        }

        location.setCanonicalUri(internal.getCanonicalUri());
        location.setCurie(internal.getCurie());
        location.setAliasUrls(internal.getAliasUrls());
        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            location.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            location.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        location.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        location.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        if (internal.getAvailable() != null) {
            location.setAvailable(internal.getAvailable());
        }

        location.setTransportIsLive(internal.getTransportIsLive());

        if (internal.getTransportSubType() != null) {
            location.setTransportSubType(TransportSubType.valueOf(internal.getTransportSubType()));
        }
        if (internal.getTransportType() != null) {
            location.setTransportType(TransportType.valueOf(internal.getTransportType()));
        }

        location.setUri(internal.getUri());
        location.setEmbedCode(internal.getEmbedCode());
        location.setEmbedId(internal.getEmbedId());
        location.setPolicy(makePolicy(internal.getPolicy()));

        return location;
    }

    private Policy makePolicy(org.atlasapi.content.v2.model.udt.Policy internal) {
        if (internal == null) {
            return null;
        }
        Policy policy = new Policy();

        Long id = internal.getId();
        if (id != null) {
            policy.setId(id);
        }
        policy.setCanonicalUri(internal.getCanonicalUri());
        policy.setCurie(internal.getCurie());
        policy.setAliasUrls(internal.getAliasUrls());
        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            policy.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            policy.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        policy.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        policy.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        policy.setAvailabilityStart(convertDateTime(internal.getAvailabilityStart()));
        policy.setAvailabilityEnd(convertDateTime(internal.getAvailabilityEnd()));
        policy.setDrmPlayableFrom(convertDateTime(internal.getDrmPlayableFrom()));

        Set<String> availableCountries = internal.getAvailableCountries();
        if (availableCountries != null) {
            policy.setAvailableCountries(availableCountries.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        policy.setAvailabilityLength(internal.getAvailabilityLength());
        String revenueContract = internal.getRevenueContract();
        if (revenueContract != null) {
            policy.setRevenueContract(Policy.RevenueContract.fromKey(revenueContract));
        }
        policy.setSubscriptionPackages(internal.getSubscriptionPackages());
        org.atlasapi.content.v2.model.udt.Price internalPrice = internal.getPrice();
        if (internalPrice != null) {
            policy.setPrice(new Price(
                    Currency.getInstance(internalPrice.getCurrency()),
                    internalPrice.getPrice().doubleValue()));
        }
        List<org.atlasapi.content.v2.model.udt.Pricing> pricing = internal.getPricing();
        if (pricing != null) {
            policy.setPricing(pricing.stream()
                    .map(pr -> new Pricing(
                            convertDateTime(pr.getStart()),
                            convertDateTime(pr.getEnd()),
                            new Price(
                                    Currency.getInstance(pr.getPrice().getCurrency()),
                                    pr.getPrice().getPrice().doubleValue()
                            )
                    ))
                    .collect(Collectors.toList()));
        }

        Long serviceId = internal.getServiceId();
        if (serviceId != null) {
            policy.setServiceId(Id.valueOf(serviceId));
        }
        Long playerId = internal.getPlayerId();
        if (playerId != null) {
            policy.setPlayerId(Id.valueOf(playerId));
        }
        if (internal.getPlatform() != null) {
            policy.setPlatform(Policy.Platform.valueOf(internal.getPlatform()));
        }
        if (internal.getNetwork() != null) {
            policy.setNetwork(Policy.Network.valueOf(internal.getNetwork()));
        }
        policy.setActualAvailabilityStart(convertDateTime(internal.getActualAvailabilityStart()));

        return policy;
    }

    private org.atlasapi.content.CrewMember makeCrewMember(CrewMember internal) {
        org.atlasapi.content.CrewMember crewMember = new org.atlasapi.content.CrewMember();

        crewMember.setId(internal.getId());
        crewMember.setCanonicalUri(internal.getCanonicalUri());
        crewMember.setCurie(internal.getCurie());
        crewMember.setAliasUrls(internal.getAliasUrls());
        crewMember.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        crewMember.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
        crewMember.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        crewMember.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        crewMember = crewMember
                .withRole(org.atlasapi.content.CrewMember.Role.valueOf(internal.getRole()))
                .withName(internal.getName())
                .withPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());

        return crewMember;
    }

    private org.atlasapi.content.Tag makeTag(Tag internal) {
        org.atlasapi.content.Tag tag = new org.atlasapi.content.Tag(
                internal.getTopic(),
                internal.getWeighting(),
                internal.getSupervised(),
                org.atlasapi.content.Tag.Relationship.valueOf(internal.getRelationship())
        );
        if (internal.getOffset() != null) {
            tag.setOffset(internal.getOffset());
        }
        if (internal.getPublisher() != null) {
            tag.setPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());
        }

        return tag;
    }

    private org.atlasapi.content.Clip makeClip(Clip internal) {
        // TODO: refactor to reuse content setters
        org.atlasapi.content.Clip content = new org.atlasapi.content.Clip();

        content.setId(internal.getId());
        content.setCanonicalUri(internal.getCanonicalUri());
        content.setCurie(internal.getCurie());
        content.setAliasUrls(internal.getAliasUrls());
        content.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        content.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
        content.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        content.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        content.setTitle(internal.getTitle());
        content.setShortDescription(internal.getShortDescription());
        content.setMediumDescription(internal.getMediumDescription());
        content.setLongDescription(internal.getLongDescription());
        content.setSynopses(makeSynopses(internal.getSynopses()));
        content.setDescription(internal.getDescription());
        content.setMediaType(MediaType.valueOf(internal.getMediaType()));
        content.setSpecialization(Specialization.valueOf(internal.getSpecialization()));
        content.setGenres(internal.getGenres());
        content.setPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());
        content.setImage(internal.getImage());
        content.setImages(internal.getImages().stream()
                .map(this::makeImage)
                .collect(Collectors.toSet()));

        content.setThumbnail(internal.getThumbnail());

        content.setFirstSeen(convertDateTime(internal.getFirstSeen()));
        content.setLastFetched(convertDateTime(internal.getLastFetched()));
        content.setThisOrChildLastUpdated(convertDateTime(internal.getThisOrChildLastUpdated()));
        if (internal.getScheduleOnly() != null) {
            content.setScheduleOnly(internal.getScheduleOnly());
        }
        if (internal.getActivelyPublished() != null) {
            content.setActivelyPublished(internal.getActivelyPublished());
        }

        content.setPresentationChannel(internal.getPresentationChannel());

        Priority internalPriority = internal.getPriority();
        if (internalPriority != null) {
            content.setPriority(new org.atlasapi.content.Priority(
                    internalPriority.getPriority(),
                    new PriorityScoreReasons(
                            internalPriority.getPositive(), internalPriority.getNegative())
            ));
        }

        content.setRelatedLinks(internal.getRelatedLinks().stream()
                .map(rl -> RelatedLink.unknownTypeLink(rl.getUrl())
                        .withTitle(rl.getTitle())
                        .withDescription(rl.getDescription())
                        .withImage(rl.getImage())
                        .withShortName(rl.getShortName())
                        .withSourceId(rl.getSourceId())
                        .withThumbnail(rl.getThumbnail())
                        .build())
                .collect(Collectors.toSet()));

        Set<org.atlasapi.content.v2.model.udt.KeyPhrase> keyPhrases = internal.getKeyPhrases();
        if (keyPhrases != null) {
            content.setKeyPhrases(keyPhrases.stream()
                    .map(kp -> new KeyPhrase(kp.getPhrase(), kp.getWeighting()))
                    .collect(Collectors.toList()));
        }

        List<Tag> tags = internal.getTags();
        if (tags != null) {
            content.setTags(tags.stream()
                    .map(this::makeTag)
                    .collect(Collectors.toList()));
        }

        List<org.atlasapi.content.v2.model.udt.ContentGroupRef> contentGroupRefs = internal.getContentGroupRefs();
        if (contentGroupRefs != null) {
            content.setContentGroupRefs(contentGroupRefs.stream()
                    .map(ref -> new ContentGroupRef(Id.valueOf(ref.getId()), ref.getUri()))
                    .collect(Collectors.toList()));
        }

        List<CrewMember> people = internal.getPeople();
        if (people != null) {
            content.setPeople(people.stream()
                    .map(this::makeCrewMember)
                    .collect(Collectors.toList()));
        }

        content.setLanguages(internal.getLanguages());

        Set<org.atlasapi.content.v2.model.udt.Certificate> certificates = internal.getCertificates();
        if (certificates != null) {
            content.setCertificates(certificates.stream()
                    .map(cert -> new Certificate(
                            cert.getClassification(),
                            Countries.fromCode(cert.getCountryCode())))
                    .collect(Collectors.toList()));
        }

        content.setYear(internal.getYear());

        Set<Encoding> manifestedAs = internal.getManifestedAs();
        if (manifestedAs != null) {
            content.setManifestedAs(manifestedAs.stream()
                    .map(this::makeEncoding)
                    .collect(Collectors.toSet()));
        }

        content.setGenericDescription(internal.getGenericDescription());

        Set<Ref> eventRefs = internal.getEventRefs();
        if (eventRefs != null) {
            content.setEventRefs(eventRefs.stream()
                    .map(ref -> new EventRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()))
                    .collect(Collectors.toList()));
        }

        if (internal.getIsLongForm() != null) {
            content.setIsLongForm(internal.getIsLongForm());
        }
        content.setBlackAndWhite(internal.getBlackAndWhite());
        Set<String> countriesOfOrigin = internal.getCountriesOfOrigin();
        if (countriesOfOrigin != null) {
            content.setCountriesOfOrigin(countriesOfOrigin.stream()
                    .map(Countries::fromCode)
                    .collect(Collectors.toSet()));
        }

        content = (org.atlasapi.content.Clip) content.withSortKey(internal.getSortKey());

        org.atlasapi.content.v2.model.udt.ContainerSummary internalContainerSummary =
                internal.getContainerSummary();
        if (internalContainerSummary != null) {
            content.setContainerSummary(new ContainerSummary(
                    internalContainerSummary.getType(),
                    internalContainerSummary.getTitle(),
                    internalContainerSummary.getDescription(),
                    internalContainerSummary.getSeriesNumber()
            ));
        }

        ContainerRef icr = internal.getContainerRef();
        content.setContainerRef(makeContainerRef(icr));

        Set<org.atlasapi.content.v2.model.udt.Broadcast> broadcasts = internal.getBroadcasts();
        if (broadcasts != null) {
            content.setBroadcasts(broadcasts.stream()
                    .map(this::makeBroadcast)
                    .collect(Collectors.toSet()));
        }

        List<SegmentEvent> segmentEvents = internal.getSegmentEvents();
        if (segmentEvents != null) {
            content.setSegmentEvents(segmentEvents.stream()
                    .map(this::makeSegmentEvent)
                    .collect(Collectors.toList()));
        }

        Set<Restriction> restrictions = internal.getRestrictions();
        if (restrictions != null) {
            content.setRestrictions(restrictions.stream()
                    .map(this::makeRestriction)
                    .collect(Collectors.toSet()));
        }

        content.setClipOf(internal.getClipOf());

        return content;
    }

    private org.atlasapi.content.ContainerRef makeContainerRef(ContainerRef icr) {
        if (icr == null) {
            return null;
        }

        ContentType type = ContentType.valueOf(icr.getType());
        switch (type) {
            case SERIES:
                return new SeriesRef(
                        Id.valueOf(icr.getId()),
                        Publisher.fromKey(icr.getSource()).requireValue(),
                        icr.getTitle(),
                        icr.getSeriesNumber(),
                        convertDateTime(icr.getUpdated()),
                        icr.getReleaseYear(),
                        icr.getCertificates().stream()
                                .map(cert -> new Certificate(
                                        cert.getClassification(),
                                        Countries.fromCode(cert.getCountryCode())))
                                .collect(Collectors.toList())
                );
            case BRAND:
                return new BrandRef(
                        Id.valueOf(icr.getId()),
                        Publisher.fromKey(icr.getSource()).requireValue()
                );
            default:
                throw new IllegalArgumentException(String.format(
                        "Illegal container ref type: %s",
                        icr.getType()
                ));
        }
    }

    private Image makeImage(org.atlasapi.content.v2.model.udt.Image img) {
        if (img == null) {
            return null;
        }

        Image newImg = new Image(img.getUri());

        String type = img.getType();
        if (type != null) {
            newImg.setType(Image.Type.valueOf(type));
        }

        String color = img.getColor();
        if (color != null) {
            newImg.setColor(Image.Color.valueOf(color));
        }

        String theme = img.getTheme();
        if (theme != null) {
            newImg.setTheme(Image.Theme.valueOf(theme));
        }

        newImg.setHeight(img.getHeight());
        newImg.setWidth(img.getWidth());

        String aspectRatio = img.getAspectRatio();
        if (aspectRatio != null) {
            newImg.setAspectRatio(Image.AspectRatio.valueOf(aspectRatio));
        }

        String mimeType = img.getMimeType();
        if (mimeType != null) {
            newImg.setMimeType(MimeType.valueOf(mimeType));
        }

        Instant availabilityStart = img.getAvailabilityStart();
        if (availabilityStart != null) {
            newImg.setAvailabilityStart(convertDateTime(availabilityStart));
        }

        Instant availabilityEnd = img.getAvailabilityEnd();
        if (availabilityEnd != null) {
            newImg.setAvailabilityEnd(convertDateTime(availabilityEnd));
        }

        newImg.setHasTitleArt(img.getHasTitleArt());

        String source = img.getSource();
        if (source != null) {
            newImg.setSource(Publisher.fromKey(source).requireValue());
        }

        return newImg;
    }

    private void setDescribedFields(
            Content content, org.atlasapi.content.v2.model.Content internal) {

        content.setTitle(internal.getTitle());

        content.setShortDescription(internal.getShortDescription());
        content.setMediumDescription(internal.getMediumDescription());
        content.setLongDescription(internal.getLongDescription());

        content.setSynopses(makeSynopses(internal.getSynopses()));
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
        Set<org.atlasapi.content.v2.model.udt.Image> images = internal.getImages();
        if (images != null) {
            content.setImages(images.stream()
                    .map(this::makeImage)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        content.setThumbnail(internal.getThumbnail());

        content.setFirstSeen(convertDateTime(internal.getFirstSeen()));
        content.setLastFetched(convertDateTime(internal.getLastFetched()));
        content.setThisOrChildLastUpdated(convertDateTime(internal.getThisOrChildLastUpdated()));

        Boolean scheduleOnly = internal.getScheduleOnly();
        if (scheduleOnly != null) {
            content.setScheduleOnly(scheduleOnly);
        }

        Boolean activelyPublished = internal.getActivelyPublished();
        if (activelyPublished != null) {
            content.setActivelyPublished(activelyPublished);
        }

        content.setPresentationChannel(internal.getPresentationChannel());

        Priority internalPriority = internal.getPriority();
        if (internalPriority != null) {
            content.setPriority(new org.atlasapi.content.Priority(
                    internalPriority.getPriority(),
                    new PriorityScoreReasons(
                            internalPriority.getPositive(), internalPriority.getNegative())
            ));
        }

        Set<org.atlasapi.content.v2.model.udt.RelatedLink> relatedLinks = internal.getRelatedLinks();
        if (relatedLinks != null) {
            content.setRelatedLinks(relatedLinks.stream()
                    .map(rl -> RelatedLink.unknownTypeLink(rl.getUrl())
                            .withTitle(rl.getTitle())
                            .withDescription(rl.getDescription())
                            .withImage(rl.getImage())
                            .withShortName(rl.getShortName())
                            .withSourceId(rl.getSourceId())
                            .withThumbnail(rl.getThumbnail())
                            .build())
                    .collect(Collectors.toSet()));
        }
    }

    private org.atlasapi.content.Synopses makeSynopses(Synopses internal) {
        if (internal == null) {
            return null;
        }

        org.atlasapi.content.Synopses synopses = new org.atlasapi.content.Synopses();

        synopses.setShortDescription(internal.getShortDescr());
        synopses.setMediumDescription(internal.getMediumDescr());
        synopses.setLongDescription(internal.getLongDescr());

        return synopses;
    }

    private void setIdentifiedFields(
            Content content, org.atlasapi.content.v2.model.Content internal) {

        content.setId(internal.getId());
        content.setCanonicalUri(internal.getCanonicalUri());
        content.setCurie(internal.getCurie());

        Set<String> aliasUrls = internal.getAliasUrls();
        if (aliasUrls != null) {
            content.setAliasUrls(aliasUrls);
        }

        Set<org.atlasapi.content.v2.model.udt.Alias> aliases = internal.getAliases();
        if (aliases != null) {
            content.setAliases(aliases.stream()
                    .map(a -> new Alias(a.getNamespace(), a.getValue()))
                    .collect(Collectors.toSet()));
        }
        Set<Ref> equivalentTo = internal.getEquivalentTo();
        if (equivalentTo != null) {
            content.setEquivalentTo(equivalentTo.stream()
                    .map(ref -> new EquivalenceRef(
                            Id.valueOf(ref.getId()),
                            Publisher.fromKey(ref.getSource()).requireValue()
                    )).collect(Collectors.toSet()));
        }
        content.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        content.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));
    }

    private Content makeEmptyContent(org.atlasapi.content.v2.model.Content internal) {
        switch (internal.getType()) {
            case "item":
                return new Item();
            case "song":
                return new Song();
            case "episode":
                return new Episode();
            case "film":
                return new Film();
            case "brand":
                return new Brand();
            case "series":
                return new Series();
            default:
                throw new IllegalArgumentException(
                        String.format("Illegal object type: %s", internal.getType())
                );
        }
    }
}
