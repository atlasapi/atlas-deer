package org.atlasapi.content.v2.serialization;

import java.util.Currency;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.BrandRef;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Certificate;
import org.atlasapi.content.ContainerSummary;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentGroupRef;
import org.atlasapi.content.Description;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Image;
import org.atlasapi.content.Item;
import org.atlasapi.content.KeyPhrase;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Pricing;
import org.atlasapi.content.PriorityScoreReasons;
import org.atlasapi.content.Quality;
import org.atlasapi.content.RelatedLink;
import org.atlasapi.content.Series;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.Song;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.TransportSubType;
import org.atlasapi.content.TransportType;
import org.atlasapi.content.v2.model.udt.Clip;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.content.v2.model.udt.CrewMember;
import org.atlasapi.content.v2.model.udt.Encoding;
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

import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Instant;

public class ContentDeserializerImpl implements ContentDeserializer {

    @Override
    public Content deserialize(org.atlasapi.content.v2.model.Content internal) {
        Content content = makeEmptyContent(internal);
        setContentFields(content, internal);
        switch (internal.getType()) {
            case "item":
                setItemFields(content, internal);
                break;
            case "song":
                setItemFields(content, internal);
                setSongFields(content, internal);
                break;
            case "episode":
                setItemFields(content, internal);
                setEpisodeFields(content, internal);
                break;
            case "film":
                setItemFields(content, internal);
                setFilmFields(content, internal);
                break;
            case "brand":
                setContainerFields(content, internal);
                setBrandFields(content, internal);
                break;
            case "series":
                setContainerFields(content, internal);
                setSeriesFields(content, internal);
                break;
            default:
                throw new IllegalArgumentException(
                        String.format( "Illegal object type: %s", internal.getType())
                );
        }
        return content;
    }

    private void setSeriesFields(Content content, org.atlasapi.content.v2.model.Content internal) {
    }

    private void setBrandFields(Content content, org.atlasapi.content.v2.model.Content internal) {
    }

    private void setContainerFields(Content content,
            org.atlasapi.content.v2.model.Content internal) {
    }

    private void setFilmFields(Content content, org.atlasapi.content.v2.model.Content internal) {

    }

    private void setEpisodeFields(Content content, org.atlasapi.content.v2.model.Content internal) {

    }

    private void setSongFields(Content content, org.atlasapi.content.v2.model.Content internal) {

    }

    private void setItemFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        Item item = (Item) content;

        ContainerRef icr = internal.getContainerRef();
        item.setContainerRef(makeContainerRef(icr));

        if (internal.getIsLongForm() != null) {
            item.setIsLongForm(internal.getIsLongForm());
        }
        item.setBlackAndWhite(internal.getBlackAndWhite());
        item.setCountriesOfOrigin(internal.getCountriesOfOrigin().stream()
                .map(Countries::fromCode)
                .collect(Collectors.toSet()));
        item = item.withSortKey(internal.getSortKey());

        org.atlasapi.content.v2.model.udt.ContainerSummary internalContainerSummary =
                internal.getContainerSummary();
        item.setContainerSummary(new ContainerSummary(
                internalContainerSummary.getType(),
                internalContainerSummary.getTitle(),
                internalContainerSummary.getDescription(),
                internalContainerSummary.getSeriesNumber()
        ));

        item.setBroadcasts(internal.getBroadcasts().stream()
                .map(this::makeBroadcast)
                .collect(Collectors.toSet()));

        item.setSegmentEvents(internal.getSegmentEvents().stream()
                .map(this::makeSegmentEvent)
                .collect(Collectors.toList()));

        item.setRestrictions(internal.getRestrictions().stream()
                .map(this::makeRestriction)
                .collect(Collectors.toSet()));
    }

    private org.atlasapi.content.Restriction makeRestriction(Restriction internal) {

        org.atlasapi.content.Restriction restriction = new org.atlasapi.content.Restriction();

        restriction.setId(internal.getId());
        restriction.setCanonicalUri(internal.getCanonicalUri());
        restriction.setCurie(internal.getCurie());
        restriction.setAliasUrls(internal.getAliasUrls());
        restriction.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        restriction.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
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
        segment.setOffset(new Duration(internal.getOffset()));
        segment.setIsChapter(internal.getIsChapter());
        org.atlasapi.content.v2.model.udt.Description description = internal.getDescription();
        segment.setDescription(new Description(
                description.getTitle(),
                description.getSynopsis(),
                description.getImage(),
                description.getThumbnail()
        ));
        Ref segmentRef = internal.getSegmentRef();
        segment.setSegment(new SegmentRef(
                Id.valueOf(segmentRef.getId()),
                Publisher.fromKey(segmentRef.getSource()).requireValue()
        ));
        segment.setVersionId(internal.getVersionId());

        segment.setPublisher(Publisher.fromKey(internal.getPublisher()).requireValue());

        return segment;
    }

    private Broadcast makeBroadcast(org.atlasapi.content.v2.model.udt.Broadcast internal) {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(internal.getChannelId()),
                convertDateTime(internal.getTransmissionStart()),
                convertDateTime(internal.getTransmissionEnd()),
                internal.getActivelyPublished()
        );

        broadcast.setId(internal.getId());
        broadcast.setCanonicalUri(internal.getCanonicalUri());
        broadcast.setCurie(internal.getCurie());
        broadcast.setAliasUrls(internal.getAliasUrls());
        broadcast.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        broadcast.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
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
        broadcast.set3d(internal.getIs3d());
        broadcast.setBlackoutRestriction(
                new org.atlasapi.content.BlackoutRestriction(internal.getBlackoutRestriction()));

        return broadcast;
    }

    private DateTime convertDateTime(Instant dateTime) {
        return dateTime.toDateTime();
    }

    private org.joda.time.LocalDate convertDate(org.joda.time.LocalDate date) {
        return date;
    }

    private void setContentFields(Content content, org.atlasapi.content.v2.model.Content internal) {
        setIdentifiedFields(content, internal);
        setDescribedFields(content, internal);

        content.setClips(internal.getClips().stream()
                .map(this::makeClip)
                .collect(Collectors.toList()));

        content.setKeyPhrases(internal.getKeyPhrases().stream()
                .map(kp -> new KeyPhrase(kp.getPhrase(), kp.getWeighting()))
                .collect(Collectors.toList()));

        content.setTags(internal.getTags().stream()
                .map(this::makeTag)
                .collect(Collectors.toList()));

        content.setContentGroupRefs(internal.getContentGroupRefs().stream()
                .map(ref -> new ContentGroupRef(Id.valueOf(ref.getId()), ref.getUri()))
                .collect(Collectors.toList()));

        content.setPeople(internal.getPeople().stream()
                .map(this::makeCrewMember)
                .collect(Collectors.toList()));

        content.setLanguages(internal.getLanguages());

        content.setCertificates(internal.getCertificates().stream()
                .map(cert -> new Certificate(
                        cert.getClassification(),
                        Countries.fromCode(cert.getCountryCode())))
                .collect(Collectors.toList()));

        content.setYear(internal.getYear());

        content.setManifestedAs(internal.getManifestedAs().stream()
                .map(this::makeEncoding)
                .collect(Collectors.toSet()));

        content.setGenericDescription(internal.getGenericDescription());

        content.setEventRefs(internal.getEventRefs().stream()
                .map(ref -> new EventRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()))
                .collect(Collectors.toList()));
    }

    private org.atlasapi.content.Encoding makeEncoding(Encoding internal) {
        org.atlasapi.content.Encoding encoding = new org.atlasapi.content.Encoding();

        // TODO: reuse setIdentified

        encoding.setId(internal.getId());
        encoding.setCanonicalUri(internal.getCanonicalUri());
        encoding.setCurie(internal.getCurie());
        encoding.setAliasUrls(internal.getAliasUrls());
        encoding.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        encoding.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
        encoding.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        encoding.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        encoding.setAvailableAt(internal.getAvailableAt().stream()
                .map(this::makeLocation)
                .collect(Collectors.toSet()));

        encoding.setContainsAdvertising(internal.getContainsAdvertising());
        encoding.setAdvertisingDuration(internal.getAdvertisingDuration());
        encoding.setDuration(new Duration(internal.getDuration()));
        encoding.setBitRate(internal.getBitRate());
        encoding.setAudioBitRate(internal.getAudioBitRate());
        encoding.setAudioChannels(internal.getAudioChannels());
        encoding.setAudioCoding(MimeType.valueOf(internal.getAudioCoding()));
        encoding.setVideoAspectRatio(internal.getVideoAspectRatio());

        encoding.setVideoBitRate(internal.getVideoBitRate());
        encoding.setVideoCoding(MimeType.valueOf(internal.getVideoCoding()));
        encoding.setVideoFrameRate(internal.getVideoFrameRate());
        encoding.setVideoHorizontalSize(internal.getVideoHorizontalSize());
        encoding.setVideoVerticalSize(internal.getVideoVerticalSize());
        encoding.setVideoProgressiveScan(internal.getVideoProgressiveScan());

        encoding.setDataSize(internal.getDataSize());
        encoding.setDataContainerFormat(MimeType.valueOf(internal.getDataContainerFormat()));
        encoding.setSource(internal.getSource());
        encoding.setDistributor(internal.getDistributor());
        encoding.setHasDOG(internal.getHasDog());
        encoding.set3d(internal.getIs3d());
        encoding.setQuality(Quality.valueOf(internal.getQuality()));
        encoding.setQualityDetail(internal.getQualityDetail());

        encoding.setVersionId(internal.getVersionId());

        return encoding;
    }

    private org.atlasapi.content.Location makeLocation(Location internal) {
        org.atlasapi.content.Location location = new org.atlasapi.content.Location();

        location.setId(internal.getId());
        location.setCanonicalUri(internal.getCanonicalUri());
        location.setCurie(internal.getCurie());
        location.setAliasUrls(internal.getAliasUrls());
        location.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        location.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
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
        Policy policy = new Policy();

        policy.setId(internal.getId());
        policy.setCanonicalUri(internal.getCanonicalUri());
        policy.setCurie(internal.getCurie());
        policy.setAliasUrls(internal.getAliasUrls());
        policy.setAliases(internal.getAliases().stream()
                .map(a -> new Alias(a.getNamespace(), a.getValue()))
                .collect(Collectors.toSet()));
        policy.setEquivalentTo(internal.getEquivalentTo().stream()
                .map(ref -> new EquivalenceRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()
                )).collect(Collectors.toSet()));
        policy.setLastUpdated(convertDateTime(internal.getLastUpdated()));
        policy.setEquivalenceUpdate(convertDateTime(internal.getEquivalenceUpdate()));

        policy.setAvailabilityStart(convertDateTime(internal.getAvailabilityStart()));
        policy.setAvailabilityEnd(convertDateTime(internal.getAvailabilityEnd()));
        policy.setDrmPlayableFrom(convertDateTime(internal.getDrmPlayableFrom()));

        policy.setAvailableCountries(internal.getAvailableCountries().stream()
                .map(Countries::fromCode)
                .collect(Collectors.toSet()));

        policy.setAvailabilityLength(internal.getAvailabilityLength());
        policy.setRevenueContract(Policy.RevenueContract.valueOf(internal.getRevenueContract()));
        policy.setSubscriptionPackages(internal.getSubscriptionPackages());
        org.atlasapi.content.v2.model.udt.Price internalPrice = internal.getPrice();
        policy.setPrice(new Price(
                Currency.getInstance(internalPrice.getCurrency()),
                internalPrice.getPrice().doubleValue()));
        policy.setPricing(internal.getPricing().stream()
                .map(pr -> new Pricing(
                        convertDateTime(pr.getStart()),
                        convertDateTime(pr.getEnd()),
                        new Price(
                                Currency.getInstance(pr.getPrice().getCurrency()),
                                pr.getPrice().getPrice().doubleValue()
                        )
                ))
                .collect(Collectors.toList()));

        policy.setServiceId(Id.valueOf(internal.getServiceId()));
        policy.setPlayerId(Id.valueOf(internal.getPlayerId()));
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

        // TODO: reuse setIdentified somehow

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

        content.setKeyPhrases(internal.getKeyPhrases().stream()
                .map(kp -> new KeyPhrase(kp.getPhrase(), kp.getWeighting()))
                .collect(Collectors.toList()));

        content.setTags(internal.getTags().stream()
                .map(this::makeTag)
                .collect(Collectors.toList()));

        content.setContentGroupRefs(internal.getContentGroupRefs().stream()
                .map(ref -> new ContentGroupRef(Id.valueOf(ref.getId()), ref.getUri()))
                .collect(Collectors.toList()));

        content.setPeople(internal.getPeople().stream()
                .map(this::makeCrewMember)
                .collect(Collectors.toList()));

        content.setLanguages(internal.getLanguages());

        content.setCertificates(internal.getCertificates().stream()
                .map(cert -> new Certificate(
                        cert.getClassification(),
                        Countries.fromCode(cert.getCountryCode())))
                .collect(Collectors.toList()));

        content.setYear(internal.getYear());

        content.setManifestedAs(internal.getManifestedAs().stream()
                .map(this::makeEncoding)
                .collect(Collectors.toSet()));

        content.setGenericDescription(internal.getGenericDescription());

        content.setEventRefs(internal.getEventRefs().stream()
                .map(ref -> new EventRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue()))
                .collect(Collectors.toList()));

        if (internal.getIsLongForm() != null) {
            content.setIsLongForm(internal.getIsLongForm());
        }
        content.setBlackAndWhite(internal.getBlackAndWhite());
        content.setCountriesOfOrigin(internal.getCountriesOfOrigin().stream()
                .map(Countries::fromCode)
                .collect(Collectors.toSet()));

        content = (org.atlasapi.content.Clip) content.withSortKey(internal.getSortKey());

        org.atlasapi.content.v2.model.udt.ContainerSummary internalContainerSummary =
                internal.getContainerSummary();
        content.setContainerSummary(new ContainerSummary(
                internalContainerSummary.getType(),
                internalContainerSummary.getTitle(),
                internalContainerSummary.getDescription(),
                internalContainerSummary.getSeriesNumber()
        ));

        ContainerRef icr = internal.getContainerRef();
        content.setContainerRef(makeContainerRef(icr));

        content.setBroadcasts(internal.getBroadcasts().stream()
                .map(this::makeBroadcast)
                .collect(Collectors.toSet()));

        content.setSegmentEvents(internal.getSegmentEvents().stream()
                .map(this::makeSegmentEvent)
                .collect(Collectors.toList()));

        content.setRestrictions(internal.getRestrictions().stream()
                .map(this::makeRestriction)
                .collect(Collectors.toSet()));

        content.setClipOf(internal.getClipOf());

        return content;
    }

    private org.atlasapi.content.ContainerRef makeContainerRef(ContainerRef icr) {
        switch (icr.getType()) {
            case "series":
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
            case "brand":
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
        Image newImg = new Image(img.getUri());
        if (img.getType() != null) {
            newImg.setType(Image.Type.valueOf(img.getType()));
        }
        if (img.getColor() != null) {
            newImg.setColor(Image.Color.valueOf(img.getColor()));
        }
        if (img.getTheme() != null) {
            newImg.setTheme(Image.Theme.valueOf(img.getTheme()));
        }

        newImg.setHeight(img.getHeight());
        newImg.setWidth(img.getWidth());

        if (img.getAspectRatio() != null) {
            newImg.setAspectRatio(Image.AspectRatio.valueOf(img.getAspectRatio()));
        }
        if (img.getMimeType() != null) {
            newImg.setMimeType(MimeType.valueOf(img.getMimeType()));
        }
        if (img.getAvailabilityStart() != null) {
            newImg.setAvailabilityStart(convertDateTime(img.getAvailabilityStart()));
        }
        if (img.getAvailabilityEnd() != null) {
            newImg.setAvailabilityEnd(convertDateTime(img.getAvailabilityEnd()));
        }
        newImg.setHasTitleArt(img.getHasTitleArt());
        if (img.getSource() != null) {
            newImg.setSource(Publisher.fromKey(img.getSource()).requireValue());
        }

        return newImg;
    }

    private void setDescribedFields(Content content,
            org.atlasapi.content.v2.model.Content internal) {
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

    private void setIdentifiedFields(Content content,
            org.atlasapi.content.v2.model.Content internal) {
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
