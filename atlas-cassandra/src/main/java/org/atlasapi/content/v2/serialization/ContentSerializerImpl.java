package org.atlasapi.content.v2.serialization;

import java.math.BigDecimal;
import java.util.Currency;
import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.Clip;
import org.atlasapi.content.Container;
import org.atlasapi.content.ContainerRef;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.CrewMember;
import org.atlasapi.content.Description;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.content.ItemSummary;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.Location;
import org.atlasapi.content.LocationSummary;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Pricing;
import org.atlasapi.content.Priority;
import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.Quality;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.ReleaseDate;
import org.atlasapi.content.Restriction;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Song;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.Subtitles;
import org.atlasapi.content.Tag;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Alias;
import org.atlasapi.content.v2.model.udt.ContainerSummary;
import org.atlasapi.content.v2.model.udt.Image;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.Synopses;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.event.EventRef;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.segment.SegmentRef;

import com.metabroadcast.common.currency.Price;
import com.metabroadcast.common.intl.Country;
import com.metabroadcast.common.media.MimeType;

import com.codepoetics.protonpack.maps.MapStream;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Lists;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;
import org.joda.time.Interval;

public class ContentSerializerImpl implements ContentSerializer {

    @Override
    public Content serialize(org.atlasapi.content.Content content) {
        Content internal = new Content();
        setType(internal, content);

        setIdentified(internal, content);
        setDescribed(internal, content);
        setContent(internal, content);

        setItem(internal, content);
        setSong(internal, content);
        setEpisode(internal, content);
        setFilm(internal, content);

        setContainer(internal, content);
        setBrand(internal, content);
        setSeries(internal, content);

        return internal;
    }

    private void setType(Content internal, org.atlasapi.content.Content content) {
        internal.setType(content.getClass().getSimpleName().toLowerCase());
    }

    private void setSeries(Content internal, org.atlasapi.content.Content content) {
        if (!Series.class.isInstance(content)) {
            return;
        }

        Series series = (Series) content;

        internal.setSeriesNumber(series.getSeriesNumber());
        internal.setTotalEpisodes(series.getTotalEpisodes());
        internal.setBrandRef(makeBrandRef(series.getBrandRef()));
    }

    private Ref makeBrandRef(BrandRef brandRef) {
        if (brandRef == null) {
            return null;
        }
        Ref ref = new Ref();

        ref.setId(brandRef.getId().longValue());
        ref.setSource(brandRef.getSource().key());

        return ref;
    }

    private void setBrand(Content internal, org.atlasapi.content.Content content) {
        if (!Brand.class.isInstance(content)) {
            return;
        }

        Brand brand = (Brand) content;
        internal.setSeriesRefs(brand.getSeriesRefs()
                .stream()
                .map(this::makeSeriesRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    private void setFilm(Content internal, org.atlasapi.content.Content content) {
        if (!Film.class.isInstance(content)) {
            return;
        }

        Film film = (Film) content;

        internal.setWebsiteUrl(film.getWebsiteUrl());
        internal.setSubtitles(film.getSubtitles()
                .stream()
                .map(Subtitles::code)
                .collect(Collectors.toSet()));
        internal.setReleaseDates(film.getReleaseDates()
                .stream()
                .map(this::makeReleaseDate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    private org.atlasapi.content.v2.model.udt.ReleaseDate makeReleaseDate(ReleaseDate releaseDate) {
        if (releaseDate == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.ReleaseDate internal =
                new org.atlasapi.content.v2.model.udt.ReleaseDate();

        Country country = releaseDate.country();
        if (country != null) {
            internal.setCountry(country.code());
        }

        ReleaseDate.ReleaseType type = releaseDate.type();
        if (type != null) {
            internal.setType(type.name());
        }

        internal.setReleaseDate(releaseDate.date());

        return internal;
    }

    private void setContainer(Content internal, org.atlasapi.content.Content content) {
        if (!Container.class.isInstance(content)) {
            return;
        }

        Container container = (Container) content;
        internal.setItemRefs(container.getItemRefs()
                .stream()
                .map(this::makeItemRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setUpcomingContent(MapStream.of(container.getUpcomingContent())
                .mapEntries(
                        this::makeItemRef,
                        broadcastRefs -> StreamSupport.stream(broadcastRefs.spliterator(), false)
                                .map(this::makeBroadcastRef)
                                .collect(Collectors.toList())
                ).collect());

        internal.setAvailableContent(MapStream.of(container.getAvailableContent())
                .mapEntries(
                        this::makeItemRef,
                        locationSummaries -> StreamSupport.stream(
                                locationSummaries.spliterator(),
                                false
                        ).map(this::makeLocationSummary).collect(Collectors.toList())
                ).collect());

        internal.setItemSummaries(container.getItemSummaries()
                .stream()
                .map(this::makeItemSummary)
                .collect(Collectors.toList()));
    }

    private org.atlasapi.content.v2.model.udt.ItemSummary makeItemSummary(ItemSummary itemSummary) {
        org.atlasapi.content.v2.model.udt.ItemSummary internal =
                new org.atlasapi.content.v2.model.udt.ItemSummary();

        internal.setRef(makeItemRef(itemSummary.getItemRef()));
        internal.setTitle(itemSummary.getTitle());

        Optional<String> description = itemSummary.getDescription();
        if (description.isPresent()) {
            internal.setDescription(description.get());
        }

        Optional<String> image = itemSummary.getImage();
        if (image.isPresent()) {
            internal.setImage(image.get());
        }

        Optional<Integer> releaseYear = itemSummary.getReleaseYear();
        if (releaseYear.isPresent()) {
            internal.setReleaseYear(releaseYear.get());
        }

        Optional<ImmutableSet<Certificate>> certificates = itemSummary.getCertificates();
        if (certificates.isPresent()) {
            internal.setCertificate(certificates.get()
                    .stream()
                    .map(this::makeCertificate)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.LocationSummary makeLocationSummary(
            LocationSummary locationSummary) {
        if (locationSummary == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.LocationSummary internal =
                new org.atlasapi.content.v2.model.udt.LocationSummary();

        internal.setAvailable(locationSummary.getAvailable());
        internal.setUri(locationSummary.getUri());
        Optional<DateTime> availabilityStart = locationSummary.getAvailabilityStart();
        if (availabilityStart.isPresent()) {
            internal.setStart(availabilityStart.get().toInstant());
        }
        Optional<DateTime> availabilityEnd = locationSummary.getAvailabilityEnd();
        if (availabilityEnd.isPresent()) {
            internal.setEnd(availabilityEnd.get().toInstant());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.BroadcastRef makeBroadcastRef(
            BroadcastRef broadcastRef) {
        if (broadcastRef == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.BroadcastRef internal =
                new org.atlasapi.content.v2.model.udt.BroadcastRef();

        internal.setSourceId(broadcastRef.getSourceId());
        Id channelId = broadcastRef.getChannelId();
        if (channelId != null) {
            internal.setChannelId(channelId.longValue());
        }

        Interval transmissionInterval = broadcastRef.getTransmissionInterval();
        if (transmissionInterval != null) {
            internal.setStart(transmissionInterval.getStart().toInstant());
            internal.setEnd(transmissionInterval.getEnd().toInstant());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.ItemRef makeItemRef(ItemRef itemRef) {
        if (itemRef == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.ItemRef internal =
                new org.atlasapi.content.v2.model.udt.ItemRef();

        Ref ref = new Ref();
        Id id = itemRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }

        Publisher source = itemRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        internal.setRef(ref);
        internal.setSortKey(itemRef.getSortKey());
        DateTime updated = itemRef.getUpdated();
        if (updated != null) {
            internal.setUpdated(updated.toInstant());
        }

        return internal;
    }

    private void setEpisode(Content internal, org.atlasapi.content.Content content) {
        if (!Episode.class.isInstance(content)) {
            return;
        }

        Episode episode = (Episode) content;
        internal.setSeriesNumber(episode.getSeriesNumber());
        internal.setEpisodeNumber(episode.getEpisodeNumber());
        internal.setPartNumber(episode.getPartNumber());
        internal.setSpecial(episode.getSpecial());
        org.atlasapi.content.v2.model.udt.SeriesRef seriesRef = makeSeriesRef(episode.getSeriesRef());
        if (seriesRef != null) {
            internal.setSeriesRefs(Lists.newArrayList(seriesRef));
        }
    }

    private org.atlasapi.content.v2.model.udt.SeriesRef makeSeriesRef(SeriesRef seriesRef) {
        if (seriesRef == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.SeriesRef internal =
                new org.atlasapi.content.v2.model.udt.SeriesRef();

        Ref ref = new Ref();
        Id id = seriesRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }

        Publisher source = seriesRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        internal.setRef(ref);

        internal.setTitle(seriesRef.getTitle());
        DateTime updated = seriesRef.getUpdated();
        if (updated != null) {
            internal.setUpdated(updated.toInstant());
        }
        internal.setSeriesNumber(seriesRef.getSeriesNumber());
        internal.setReleaseYear(seriesRef.getReleaseYear());
        internal.setCertificates(seriesRef.getCertificates()
                .stream()
                .map(this::makeCertificate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        return internal;
    }

    private void setSong(Content internal, org.atlasapi.content.Content content) {
        if (!Song.class.isInstance(content)) {
            return;
        }

        Song song = (Song) content;
        internal.setIsrc(song.getIsrc());
        Duration duration = song.getDuration();
        if (duration != null) {
            internal.setDuration(duration.getMillis());
        }
    }

    private void setItem(Content internal, org.atlasapi.content.Content content) {
        if (!Item.class.isInstance(content)) {
            return;
        }

        Item item = (Item) content;

        internal.setContainerRef(makeContainerRef(item.getContainerRef()));
        internal.setIsLongForm(item.getIsLongForm());
        internal.setBlackAndWhite(item.getBlackAndWhite());
        internal.setCountriesOfOrigin(
                item.getCountriesOfOrigin().stream()
                        .map(Country::code)
                        .collect(Collectors.toSet())
        );
        internal.setSortKey(item.sortKey());

        org.atlasapi.content.ContainerSummary itemContainerSummary = item.getContainerSummary();
        if (itemContainerSummary != null) {
            ContainerSummary containerSummary = new ContainerSummary();
            containerSummary.setType(itemContainerSummary.getType());
            containerSummary.setTitle(itemContainerSummary.getTitle());
            containerSummary.setSeriesNumber(itemContainerSummary.getSeriesNumber());
            containerSummary.setDescription(itemContainerSummary.getDescription());
            internal.setContainerSummary(containerSummary);
        }

        internal.setBroadcasts(item.getBroadcasts().stream()
                .map(this::makeBroadcast)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setSegmentEvents(item.getSegmentEvents()
                .stream()
                .map(this::makeSegmentEvent)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setRestrictions(item.getRestrictions()
                .stream()
                .map(this::makeRestriction)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    private org.atlasapi.content.v2.model.udt.Restriction makeRestriction(Restriction restriction) {
        if (restriction == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.Restriction internal =
                new org.atlasapi.content.v2.model.udt.Restriction();

        Id id = restriction.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        internal.setCanonicalUri(restriction.getCanonicalUri());
        internal.setCurie(restriction.getCurie());
        internal.setAliasUrls(restriction.getAliasUrls());
        internal.setAliases(restriction.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(restriction.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = restriction.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = restriction.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }
        internal.setRestricted(restriction.isRestricted());
        internal.setMinimumAge(restriction.getMinimumAge());
        internal.setMessage(restriction.getMessage());
        internal.setAuthority(restriction.getAuthority());
        internal.setRating(restriction.getRating());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.SegmentEvent makeSegmentEvent(
            SegmentEvent segmentEvent) {
        if (segmentEvent == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.SegmentEvent internal =
                new org.atlasapi.content.v2.model.udt.SegmentEvent();

        Id id = segmentEvent.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(segmentEvent.getCanonicalUri());
        internal.setCurie(segmentEvent.getCurie());
        internal.setAliasUrls(segmentEvent.getAliasUrls());
        internal.setAliases(segmentEvent.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(segmentEvent.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = segmentEvent.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = segmentEvent.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }

        internal.setPosition(segmentEvent.getPosition());
        Duration offset = segmentEvent.getOffset();
        if (offset != null) {
            internal.setOffset(offset.getMillis());
        }
        internal.setIsChapter(segmentEvent.getIsChapter());
        internal.setDescription(makeDescription(segmentEvent.getDescription()));
        internal.setSegmentRef(makeSegmentRef(segmentEvent.getSegmentRef()));
        internal.setVersionId(segmentEvent.getVersionId());
        Publisher source = segmentEvent.getSource();
        if (source != null) {
            internal.setPublisher(source.key());
        }

        return internal;
    }

    private Ref makeSegmentRef(SegmentRef segmentRef) {
        if (segmentRef == null) {
            return null;
        }
        Ref internal = new Ref();

        Id id = segmentRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        Publisher source = segmentRef.getSource();
        if (source != null) {
            internal.setSource(source.key());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Description makeDescription(Description description) {
        if (description == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.Description internal =
                new org.atlasapi.content.v2.model.udt.Description();

        internal.setTitle(description.getTitle());
        internal.setImage(description.getImage());
        internal.setSynopsis(description.getSynopsis());
        internal.setThumbnail(description.getThumbnail());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Broadcast makeBroadcast(Broadcast broadcast) {
        if (broadcast == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Broadcast internal =
                new org.atlasapi.content.v2.model.udt.Broadcast();

        Id id = broadcast.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(broadcast.getCanonicalUri());
        internal.setCurie(broadcast.getCurie());
        internal.setAliasUrls(broadcast.getAliasUrls());
        internal.setAliases(broadcast.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(broadcast.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = broadcast.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = broadcast.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }

        Id channelId = broadcast.getChannelId();
        if (channelId != null) {
            internal.setChannelId(channelId.longValue());
        }

        DateTime transmissionTime = broadcast.getTransmissionTime();
        if (transmissionTime != null) {
            internal.setTransmissionStart(transmissionTime.toInstant());
        }

        DateTime transmissionEndTime = broadcast.getTransmissionEndTime();
        if (transmissionEndTime != null) {
            internal.setTransmissionEnd(transmissionEndTime.toInstant());
        }

        Duration broadcastDuration = broadcast.getBroadcastDuration();
        if (broadcastDuration != null) {
            internal.setBroadcastDuration(broadcastDuration.getMillis());
        }
        internal.setScheduleDate(broadcast.getScheduleDate());
        internal.setActivelyPublished(broadcast.isActivelyPublished());
        internal.setSourceId(broadcast.getSourceId());
        internal.setVersionId(broadcast.getVersionId());
        internal.setRepeat(broadcast.getRepeat());
        internal.setSubtitled(broadcast.getSubtitled());
        internal.setSigned(broadcast.getSigned());
        internal.setAudioDescribed(broadcast.getAudioDescribed());
        internal.setHighDefinition(broadcast.getHighDefinition());
        internal.setWidescreen(broadcast.getWidescreen());
        internal.setSurround(broadcast.getSurround());
        internal.setLive(broadcast.getLive());
        internal.setNewSeries(broadcast.getNewSeries());
        internal.setNewEpisode(broadcast.getNewEpisode());
        internal.setPremiere(broadcast.getPremiere());
        internal.setIs3d(broadcast.is3d());
        internal.setBlackoutRestriction(broadcast.getBlackoutRestriction().isPresent()
                && broadcast.getBlackoutRestriction().get().getAll());

        return internal;
    }

    private Ref makeEquivalentRef(EquivalenceRef equivalenceRef) {
        if (equivalenceRef == null) {
            return null;
        }
        Ref internal = new Ref();

        Id id = equivalenceRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        Publisher source = equivalenceRef.getSource();
        if (source != null) {
            internal.setSource(source.key());
        }

        return internal;
    }

    private Alias makeAlias(org.atlasapi.entity.Alias alias) {
        if (alias == null) {
            return null;
        }
        Alias internal = new Alias();

        internal.setValue(alias.getValue());
        internal.setNamespace(alias.getNamespace());

        return internal;
    }

    private void setContent(Content internal, org.atlasapi.content.Content content) {
        internal.setClips(content.getClips().stream()
                .map(this::makeClip)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setKeyPhrases(content.getKeyPhrases().stream()
                .map(this::makeKeyPhrase)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setTags(content.getTags().stream()
                .map(this::makeTag)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setContentGroupRefs(content.getContentGroupRefs().stream()
                .map(this::makeContentGroupRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setPeople(content.people().stream()
                .map(this::makeCrewMember)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setLanguages(content.getLanguages());

        internal.setCertificates(content.getCertificates().stream()
                .map(this::makeCertificate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setYear(content.getYear());
        internal.setManifestedAs(content.getManifestedAs().stream()
                .map(this::makeEncoding)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setGenericDescription(content.isGenericDescription());
        internal.setEventRefs(content.getEventRefs().stream()
                .map(this::makeEventRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    private org.atlasapi.content.v2.model.udt.CrewMember makeCrewMember(CrewMember crewMember) {
        if (crewMember == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.CrewMember internal =
                new org.atlasapi.content.v2.model.udt.CrewMember();

        Id id = crewMember.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(crewMember.getCanonicalUri());
        internal.setCurie(crewMember.getCurie());
        internal.setAliasUrls(crewMember.getAliasUrls());
        internal.setAliases(crewMember.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(crewMember.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = crewMember.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = crewMember.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }
        CrewMember.Role role = crewMember.role();
        if (role != null) {
            internal.setRole(role.key());
        }
        internal.setName(crewMember.name());
        Publisher publisher = crewMember.publisher();
        if (publisher != null) {
            internal.setPublisher(publisher.key());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Encoding makeEncoding(Encoding encoding) {
        if (encoding == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Encoding internal =
                new org.atlasapi.content.v2.model.udt.Encoding();

        Id id = encoding.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(encoding.getCanonicalUri());
        internal.setCurie(encoding.getCurie());
        internal.setAliasUrls(encoding.getAliasUrls());
        internal.setAliases(encoding.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(encoding.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = encoding.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = encoding.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }

        internal.setAvailableAt(encoding.getAvailableAt()
                .stream()
                .map(this::makeLocation)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setContainsAdvertising(encoding.getContainsAdvertising());
        internal.setAdvertisingDuration(encoding.getAdvertisingDuration());
        Duration duration = encoding.getDuration();
        if (duration != null) {
            internal.setDuration(duration.getMillis());
        }
        internal.setBitRate(encoding.getBitRate());

        internal.setAudioBitRate(encoding.getAudioBitRate());
        internal.setAudioChannels(encoding.getAudioChannels());
        MimeType audioCoding = encoding.getAudioCoding();
        if (audioCoding != null) {
            internal.setAudioCoding(audioCoding.name());
        }

        internal.setVideoAspectRatio(encoding.getVideoAspectRatio());
        internal.setVideoBitRate(encoding.getVideoBitRate());
        MimeType videoCoding = encoding.getVideoCoding();
        if (videoCoding != null) {
            internal.setVideoCoding(videoCoding.name());
        }
        internal.setVideoFrameRate(encoding.getVideoFrameRate());
        internal.setVideoHorizontalSize(encoding.getVideoHorizontalSize());
        internal.setVideoProgressiveScan(encoding.getVideoProgressiveScan());
        internal.setVideoVerticalSize(encoding.getVideoVerticalSize());
        internal.setDataSize(encoding.getDataSize());
        MimeType dataContainerFormat = encoding.getDataContainerFormat();
        if (dataContainerFormat != null) {
            internal.setDataContainerFormat(dataContainerFormat.name());
        }
        internal.setSource(encoding.getSource());
        internal.setDistributor(encoding.getDistributor());
        internal.setHasDog(encoding.getHasDOG());
        internal.setIs3d(encoding.is3d());
        Quality quality = encoding.getQuality();
        if (quality != null) {
            internal.setQuality(quality.name());
        }
        internal.setQualityDetail(encoding.getQualityDetail());
        internal.setVersionId(encoding.getVersionId());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Location makeLocation(Location location) {
        if (location == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Location internal =
                new org.atlasapi.content.v2.model.udt.Location();

        Id id = location.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }

        internal.setCanonicalUri(location.getCanonicalUri());
        internal.setCurie(location.getCurie());
        internal.setAliasUrls(location.getAliasUrls());
        internal.setAliases(location.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(location.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = location.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = location.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }

        internal.setAvailable(location.getAvailable());
        internal.setTransportIsLive(location.getTransportIsLive());
        TransportSubType transportSubType = location.getTransportSubType();
        if (transportSubType != null) {
            internal.setTransportSubType(transportSubType.name());
        }
        TransportType transportType = location.getTransportType();
        if (transportType != null) {
            internal.setTransportType(transportType.name());
        }
        internal.setUri(location.getUri());
        internal.setEmbedCode(location.getEmbedCode());
        internal.setEmbedId(location.getEmbedId());
        internal.setPolicy(makePolicy(location.getPolicy()));

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Policy makePolicy(Policy policy) {
        if (policy == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Policy internal =
                new org.atlasapi.content.v2.model.udt.Policy();

        Id id = policy.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(policy.getCanonicalUri());
        internal.setCurie(policy.getCurie());
        internal.setAliasUrls(policy.getAliasUrls());
        internal.setAliases(policy.getAliases()
                .stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(policy.getEquivalentTo()
                .stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        DateTime lastUpdated = policy.getLastUpdated();
        if (lastUpdated != null) {
            internal.setLastUpdated(lastUpdated.toInstant());
        }
        DateTime equivalenceUpdate = policy.getEquivalenceUpdate();
        if (equivalenceUpdate != null) {
            internal.setEquivalenceUpdate(equivalenceUpdate.toInstant());
        }

        DateTime availabilityStart = policy.getAvailabilityStart();
        if (availabilityStart != null) {
            internal.setAvailabilityStart(availabilityStart.toInstant());
        }
        DateTime availabilityEnd = policy.getAvailabilityEnd();
        if (availabilityEnd != null) {
            internal.setAvailabilityEnd(availabilityEnd.toInstant());
        }
        DateTime drmPlayableFrom = policy.getDrmPlayableFrom();
        if (drmPlayableFrom != null) {
            internal.setDrmPlayableFrom(drmPlayableFrom.toInstant());
        }
        internal.setAvailableCountries(policy.getAvailableCountries()
                .stream()
                .map(Country::code)
                .collect(Collectors.toSet()));
        internal.setAvailabilityLength(policy.getAvailabilityLength());
        Policy.RevenueContract revenueContract = policy.getRevenueContract();
        if (revenueContract != null) {
            internal.setRevenueContract(revenueContract.key());
        }
        internal.setSubscriptionPackages(policy.getSubscriptionPackages());
        internal.setPrice(makePrice(policy.getPrice()));
        internal.setPricing(policy.getPricing()
                .stream()
                .map(this::makePricing)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
        Id serviceRef = policy.getServiceRef();
        if (serviceRef != null) {
            internal.setServiceId(serviceRef.longValue());
        }
        Id playerRef = policy.getPlayerRef();
        if (playerRef != null) {
            internal.setPlayerId(playerRef.longValue());
        }
        Policy.Platform platform = policy.getPlatform();
        if (platform != null) {
            internal.setPlatform(platform.key());
        }
        Policy.Network network = policy.getNetwork();
        if (network != null) {
            internal.setNetwork(network.key());
        }
        DateTime actualAvailabilityStart = policy.getActualAvailabilityStart();
        if (actualAvailabilityStart != null) {
            internal.setActualAvailabilityStart(actualAvailabilityStart.toInstant());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Pricing makePricing(Pricing pricing) {
        if (pricing == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Pricing internal =
                new org.atlasapi.content.v2.model.udt.Pricing();

        DateTime startTime = pricing.getStartTime();
        if (startTime != null) {
            internal.setStart(startTime.toInstant());
        }
        DateTime endTime = pricing.getEndTime();
        if (endTime != null) {
            internal.setEnd(endTime.toInstant());
        }
        internal.setPrice(makePrice(pricing.getPrice()));

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Price makePrice(Price price) {
        if (price == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Price internal =
                new org.atlasapi.content.v2.model.udt.Price();

        internal.setPrice(BigDecimal.valueOf(price.getAmount()));
        Currency currency = price.getCurrency();
        if (currency != null) {
            internal.setCurrency(currency.getCurrencyCode());
        }

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Certificate makeCertificate(Certificate certificate) {
        if (certificate == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Certificate internal = new org.atlasapi.content.v2.model.udt.Certificate();

        internal.setClassification(certificate.classification());
        Country country = certificate.country();
        if (country != null) {
            internal.setCountryCode(country.code());
        }

        return internal;
    }

    private Ref makeEventRef(EventRef eventRef) {
        if (eventRef == null) {
            return null;
        }
        Ref ref = new Ref();

        Id id = eventRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }
        Publisher source = eventRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        return ref;
    }

    private org.atlasapi.content.v2.model.udt.Clip makeClip(Clip clip) {
        if (clip == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Clip internal =
                new org.atlasapi.content.v2.model.udt.Clip();

        Id id = clip.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(clip.getCanonicalUri());
        internal.setCurie(clip.getCurie());
        internal.setAliasUrls(clip.getAliasUrls());
        internal.setAliases(clip.getAliases().stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(clip.getEquivalentTo().stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setLastUpdated(convertDateTime(clip.getLastUpdated()));
        internal.setEquivalenceUpdate(convertDateTime(clip.getEquivalenceUpdate()));

        internal.setTitle(clip.getTitle());
        internal.setShortDescription(clip.getShortDescription());
        internal.setMediumDescription(clip.getMediumDescription());
        internal.setLongDescription(clip.getLongDescription());

        internal.setSynopses(makeSynopses(clip.getSynopses()));
        internal.setDescription(clip.getDescription());

        MediaType mediaType = clip.getMediaType();
        if (mediaType != null) {
            internal.setMediaType(mediaType.name());
        }
        Specialization specialization = clip.getSpecialization();
        if (specialization != null) {
            internal.setSpecialization(specialization.name());
        }
        internal.setGenres(clip.getGenres());
        Publisher source = clip.getSource();
        if (source != null) {
            internal.setPublisher(source.key());
        }
        internal.setImage(clip.getImage());

        internal.setImages(clip.getImages().stream()
                .map(this::makeImage)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setThumbnail(clip.getThumbnail());
        internal.setFirstSeen(convertDateTime(clip.getFirstSeen()));
        internal.setLastFetched(convertDateTime(clip.getLastFetched()));
        internal.setThisOrChildLastUpdated(convertDateTime(clip.getThisOrChildLastUpdated()));
        internal.setScheduleOnly(clip.isScheduleOnly());
        internal.setActivelyPublished(clip.isActivelyPublished());
        internal.setPresentationChannel(clip.getPresentationChannel());
        internal.setPriority(makePriority(clip.getPriority()));
        internal.setRelatedLinks(clip.getRelatedLinks().stream()
                .map(this::makeRelatedLink)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setKeyPhrases(clip.getKeyPhrases().stream()
                .map(this::makeKeyPhrase)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setTags(clip.getTags().stream()
                .map(this::makeTag)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setContentGroupRefs(clip.getContentGroupRefs().stream()
                .map(this::makeContentGroupRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setPeople(clip.people().stream()
                .map(this::makeCrewMember)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setLanguages(clip.getLanguages());

        internal.setCertificates(clip.getCertificates().stream()
                .map(this::makeCertificate)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setYear(clip.getYear());
        internal.setManifestedAs(clip.getManifestedAs().stream()
                .map(this::makeEncoding)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setGenericDescription(clip.isGenericDescription());
        internal.setEventRefs(clip.getEventRefs().stream()
                .map(this::makeEventRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setContainerRef(makeContainerRef(clip.getContainerRef()));

        internal.setIsLongForm(clip.getIsLongForm());
        internal.setBlackAndWhite(clip.getBlackAndWhite());
        internal.setCountriesOfOrigin(
                clip.getCountriesOfOrigin().stream()
                        .map(Country::code)
                        .collect(Collectors.toSet())
        );
        internal.setSortKey(clip.sortKey());

        org.atlasapi.content.ContainerSummary itemContainerSummary = clip.getContainerSummary();
        if (itemContainerSummary != null) {
            ContainerSummary containerSummary = new ContainerSummary();
            containerSummary.setType(itemContainerSummary.getType());
            containerSummary.setTitle(itemContainerSummary.getTitle());
            containerSummary.setSeriesNumber(itemContainerSummary.getSeriesNumber());
            containerSummary.setDescription(itemContainerSummary.getDescription());
            internal.setContainerSummary(containerSummary);
        }

        internal.setBroadcasts(clip.getBroadcasts().stream()
                .map(this::makeBroadcast)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setSegmentEvents(clip.getSegmentEvents()
                .stream()
                .map(this::makeSegmentEvent)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));

        internal.setRestrictions(clip.getRestrictions()
                .stream()
                .map(this::makeRestriction)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        internal.setClipOf(clip.getClipOf());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.ContainerRef makeContainerRef(
            ContainerRef containerRef) {

        if (containerRef == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.ContainerRef internalContainerRef =
                new org.atlasapi.content.v2.model.udt.ContainerRef();
        Id id = containerRef.getId();
        if (id != null) {
            internalContainerRef.setId(id.longValue());
        }
        Publisher source = containerRef.getSource();
        if (source != null) {
            internalContainerRef.setSource(source.key());
        }
        ContentType contentType = containerRef.getContentType();
        internalContainerRef.setType(contentType.name());
        switch (contentType) {
            case BRAND:
                break;
            case SERIES:
                SeriesRef seriesRef = (SeriesRef) containerRef;
                internalContainerRef.setTitle(seriesRef.getTitle());
                DateTime updated = seriesRef.getUpdated();
                if (updated != null) {
                    internalContainerRef.setUpdated(updated.toInstant());
                }
                internalContainerRef.setSeriesNumber(seriesRef.getSeriesNumber());
                internalContainerRef.setReleaseYear(seriesRef.getReleaseYear());
                internalContainerRef.setCertificates(seriesRef.getCertificates()
                        .stream()
                        .map(this::makeCertificate)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet()));
                break;
            default:
                throw new IllegalArgumentException(String.format(
                        "%s can't be a container",
                        containerRef
                ));
        }
        return internalContainerRef;
    }

    private org.atlasapi.content.v2.model.udt.KeyPhrase makeKeyPhrase(KeyPhrase keyPhrase) {
        if (keyPhrase == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.KeyPhrase internal = new org.atlasapi.content.v2.model.udt.KeyPhrase();

        internal.setPhrase(keyPhrase.getPhrase());
        internal.setWeighting(keyPhrase.getWeighting());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.Tag makeTag(Tag tag) {
        if (tag == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.Tag internal = new org.atlasapi.content.v2.model.udt.Tag();

        Id topic = tag.getTopic();
        if (topic != null) {
            internal.setTopic(topic.longValue());
        }

        Publisher publisher = tag.getPublisher();
        if (publisher != null) {
            internal.setPublisher(publisher.key());
        }

        internal.setSupervised(tag.isSupervised());
        internal.setWeighting(tag.getWeighting());

        Tag.Relationship relationship = tag.getRelationship();
        if (relationship != null) {
            internal.setRelationship(relationship.name());
        }

        internal.setOffset(tag.getOffset());

        return internal;
    }

    private org.atlasapi.content.v2.model.udt.ContentGroupRef makeContentGroupRef(ContentGroupRef contentGroupRef) {
        if (contentGroupRef == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.ContentGroupRef internal = new org.atlasapi.content.v2.model.udt.ContentGroupRef();

        Id id = contentGroupRef.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setUri(contentGroupRef.getUri());

        return internal;
    }

    private void setDescribed(Content internal, org.atlasapi.content.Content content) {
        internal.setTitle(content.getTitle());
        internal.setShortDescription(content.getShortDescription());
        internal.setMediumDescription(content.getMediumDescription());
        internal.setLongDescription(content.getLongDescription());

        internal.setSynopses(makeSynopses(content.getSynopses()));
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
                .map(this::makeImage)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setThumbnail(content.getThumbnail());
        internal.setFirstSeen(convertDateTime(content.getFirstSeen()));
        internal.setLastFetched(convertDateTime(content.getLastFetched()));
        internal.setThisOrChildLastUpdated(convertDateTime(content.getThisOrChildLastUpdated()));
        internal.setScheduleOnly(content.isScheduleOnly());
        internal.setActivelyPublished(content.isActivelyPublished());
        internal.setPresentationChannel(content.getPresentationChannel());
        internal.setPriority(makePriority(content.getPriority()));
        internal.setRelatedLinks(content.getRelatedLinks().stream()
                .map(this::makeRelatedLink)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
    }

    private Image makeImage(org.atlasapi.content.Image img) {
        if (img == null) {
            return null;
        }
        Image image = new Image();

        image.setUri(img.getCanonicalUri());

        org.atlasapi.content.Image.Type type = img.getType();
        if (type != null) {
            image.setType(type.name());
        }

        org.atlasapi.content.Image.Color color = img.getColor();
        if (color != null) {
            image.setColor(color.name());
        }

        org.atlasapi.content.Image.Theme theme = img.getTheme();
        if (theme != null) {
            image.setTheme(theme.name());
        }

        image.setHeight(img.getHeight());
        image.setWidth(img.getWidth());

        org.atlasapi.content.Image.AspectRatio aspectRatio = img.getAspectRatio();
        if (aspectRatio != null) {
            image.setAspectRatio(aspectRatio.name());
        }

        MimeType mimeType = img.getMimeType();
        if (mimeType != null) {
            image.setMimeType(mimeType.name());
        }

        image.setAvailabilityStart(convertDateTime(img.getAvailabilityStart()));
        image.setAvailabilityEnd(convertDateTime(img.getAvailabilityEnd()));
        image.setHasTitleArt(img.hasTitleArt());

        Publisher source = img.getSource();
        if (source != null) {
            image.setSource(source.key());
        }

        return image;
    }

    private org.atlasapi.content.v2.model.udt.RelatedLink makeRelatedLink(RelatedLink rl) {
        if (rl == null) {
            return null;
        }
        org.atlasapi.content.v2.model.udt.RelatedLink link = new org.atlasapi.content.v2.model.udt.RelatedLink();

        link.setUrl(rl.getUrl());
        RelatedLink.LinkType type = rl.getType();
        if (type != null) {
            link.setType(type.name());
        }
        link.setSourceId(rl.getSourceId());
        link.setShortName(rl.getShortName());
        link.setTitle(rl.getTitle());
        link.setDescription(rl.getDescription());
        link.setImage(rl.getImage());
        link.setThumbnail(rl.getThumbnail());

        return link;
    }

    private org.atlasapi.content.v2.model.udt.Priority makePriority(Priority priority) {
        if (priority == null) {
            return null;
        }

        org.atlasapi.content.v2.model.udt.Priority internal = new org.atlasapi.content.v2.model.udt.Priority();

        internal.setPriority(priority.getPriority());
        PriorityScoreReasons reasons = priority.getReasons();
        if (reasons != null) {
            internal.setPositive(reasons.getPositive());
            internal.setNegative(reasons.getNegative());
        }

        return internal;
    }

    private Synopses makeSynopses(org.atlasapi.content.Synopses cSynopses) {
        if (cSynopses == null) {
            return null;
        }

        Synopses synopses = new Synopses();

        synopses.setShortDescr(cSynopses.getShortDescription());
        synopses.setMediumDescr(cSynopses.getMediumDescription());
        synopses.setLongDescr(cSynopses.getLongDescription());

        return synopses;
    }

    private void setIdentified(Content internal, org.atlasapi.content.Content content) {
        Id id = content.getId();
        if (id != null) {
            internal.setId(id.longValue());
        }
        internal.setCanonicalUri(content.getCanonicalUri());
        internal.setCurie(content.getCurie());
        internal.setAliasUrls(content.getAliasUrls());
        internal.setAliases(content.getAliases().stream()
                .map(this::makeAlias)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setEquivalentTo(content.getEquivalentTo().stream()
                .map(this::makeEquivalentRef)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));
        internal.setLastUpdated(convertDateTime(content.getLastUpdated()));
        internal.setEquivalenceUpdate(convertDateTime(content.getEquivalenceUpdate()));
    }

    private org.joda.time.LocalDate convertDate(org.joda.time.LocalDate joda) {
        return joda;
    }

    private Instant convertDateTime(DateTime joda) {
        if (joda != null) {
            return joda.toInstant();
        } else {
            return null;
        }
    }
}
