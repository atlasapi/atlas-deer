package org.atlasapi.content;

import java.util.List;
import java.util.Set;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.ProtoBufUtils;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.serialization.protobuf.ContentProtos.Content.Builder;

import com.google.common.base.Function;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.intl.Countries;

final class ContentSerializationVisitor implements ContentVisitor<Builder> {
    
    private final BroadcastSerializer broadcastSerializer = new BroadcastSerializer();
    private final EncodingSerializer encodingSerializer = new EncodingSerializer();
    private final SegmentEventSerializer segmentEventSerializer = new SegmentEventSerializer();
    private final RestrictionSerializer restrictionSerializer = new RestrictionSerializer();
    private final TopicRefSerializer topicRefSerializer = new TopicRefSerializer();
    private final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();
    private final KeyPhraseSerializer keyPhraseSerializer = new KeyPhraseSerializer();
    private final CrewMemberSerializer crewMemberSerializer = new CrewMemberSerializer();
    private final ContainerSummarySerializer containerSummarySerializer = new ContainerSummarySerializer();
    private final ReleaseDateSerializer releaseDateSerializer = new ReleaseDateSerializer();
    private final CertificateSerializer certificateSerializer = new CertificateSerializer();
    
    private Builder visitIdentified(Identified ided) {
        Builder builder = ContentProtos.Content.newBuilder();
        if (ided.getId() != null) {
            builder.setId(ided.getId().longValue())
                .setType(ided.getClass().getSimpleName().toLowerCase());
        }
        if (ided.getLastUpdated() != null) {
            builder.setLastUpdated(ProtoBufUtils.serializeDateTime(ided.getLastUpdated()));
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
                .setSource(equivRef.getPublisher().key())
            );
        }
        return builder;
    }
    
    private Builder visitDescribed(Described content) {
        Builder builder = visitIdentified(content);
        if (content.getThisOrChildLastUpdated() != null) {
            builder.setChildLastUpdated(ProtoBufUtils.serializeDateTime(content.getThisOrChildLastUpdated()));
        }
        if (content.getPublisher() != null) {
            builder.setSource(content.getPublisher().key());
        }
        if (content.getFirstSeen() != null) {
            builder.setFirstSeen(ProtoBufUtils.serializeDateTime(content.getFirstSeen()));
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
            builder.addImagesBuilder().setUri(image.getCanonicalUri());
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
        
        for (TopicRef topicRef : content.getTopicRefs()) {
            builder.addTopicRefs(topicRefSerializer.serialize(topicRef));
        }
        
        if (content.getYear() != null) {
            builder.setYear(content.getYear());
        }
        return builder;
    }

    private Builder visitItem(Item item) {
        Builder builder = visitContent(item);
        if (item.getContainerRef() != null) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(item.getPublisher());
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
        builder.addAllEncodings(serializeEncoding(item.getManifestedAs()));
        builder.addAllSegmentEvents(serializeSegmentEvents(item.getSegmentEvents()));
        builder.addAllRestrictions(serializeRestrictions(item.getRestrictions()));
        return builder;
    }

    private Iterable<ContentProtos.SegmentEvent> serializeSegmentEvents(List<SegmentEvent> segmentEvents) {
        return Iterables.transform(segmentEvents, new Function<SegmentEvent, ContentProtos.SegmentEvent>() {
            @Override
            public ContentProtos.SegmentEvent apply(SegmentEvent segmentEvent) {
                return segmentEventSerializer.serialize(segmentEvent).build();
            }
        });
    }
    
    private Iterable<ContentProtos.Broadcast> serializeBroadcasts(Set<Broadcast> broadcasts) {
      return Iterables.transform(broadcasts, new Function<Broadcast, ContentProtos.Broadcast>() {
          @Override
          public ContentProtos.Broadcast apply(Broadcast broadcast) {
              return broadcastSerializer.serialize(broadcast).build();
          }
      });
    }
    
    private Iterable<ContentProtos.Encoding> serializeEncoding(Set<Encoding> encodings) {
      return Iterables.transform(encodings, new Function<Encoding, ContentProtos.Encoding>() {
          @Override
          public ContentProtos.Encoding apply(Encoding encoding) {
              return encodingSerializer.serialize(encoding).build();
          }
      });
    }

    private Iterable<ContentProtos.Restriction> serializeRestrictions(Set<Restriction> restrictions) {
        return Iterables.transform(restrictions, new Function<Restriction, ContentProtos.Restriction>() {
            @Override
            public ContentProtos.Restriction apply(Restriction restriction) {
                return restrictionSerializer.serialize(restriction).build();
            }
        });
    }

    
    private Builder visitContainer(Container container) {
        Builder builder = visitContent(container);
        ContentRefSerializer refSerializer = new ContentRefSerializer(container.getPublisher());
        for (ItemRef child : container.getItemRefs()) {
            builder.addChildren(refSerializer.serialize(child));
        }
        return builder;
    }

    @Override
    public Builder visit(Brand brand) {
        Builder builder = visitContainer(brand);
        ContentRefSerializer refSerializer = new ContentRefSerializer(brand.getPublisher());
        for (SeriesRef seriesRef : brand.getSeriesRefs()) {
            builder.addSecondaries(refSerializer.serialize(seriesRef));
        }
        return builder;
    }

    @Override
    public Builder visit(Series series) {
        Builder builder = visitContainer(series);
        if (series.getBrandRef() != null) {
            ContentRefSerializer refSerializer = new ContentRefSerializer(series.getPublisher());
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
            ContentRefSerializer refSerializer = new ContentRefSerializer(episode.getPublisher());
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
        return builder;
    }

    @Override
    public Builder visit(Film film) {
        Builder builder = visitItem(film);
        for (ReleaseDate releaseDate : film.getReleaseDates()) {
            builder.addReleaseDates(releaseDateSerializer .serialize(releaseDate));
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