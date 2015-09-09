package org.atlasapi.entity;

import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.Described;
import org.atlasapi.content.ImageSerializer;
import org.atlasapi.content.MediaType;
import org.atlasapi.content.PrioritySerializer;
import org.atlasapi.content.RelatedLinkSerializer;
import org.atlasapi.content.Specialization;
import org.atlasapi.content.SynopsesSerializer;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.CommonProtos;
import org.atlasapi.source.Sources;

import com.google.common.base.Optional;
import com.metabroadcast.common.base.Maybe;

public class DescribedSerializer<T extends Described> {

    public CommonProtos.Described serialize(T source) {
        CommonProtos.Described.Builder builder = CommonProtos.Described.newBuilder();

        builder.setIdentified(new IdentifiedSerializer<T>().serialize(source));

        if (source.getTitle() != null) {
            builder.setTitle(builder.getTitleBuilder().setValue(source.getTitle()));
        }
        if (source.getShortDescription() != null) {
            builder.setShortDescription(builder.getShortDescriptionBuilder()
                    .setValue(source.getShortDescription()));
        }
        if (source.getMediumDescription() != null) {
            builder.setMediumDescription(builder.getMediumDescriptionBuilder()
                    .setValue(source.getMediumDescription()));
        }
        if (source.getLongDescription() != null) {
            builder.setLongDescription(builder.getLongDescriptionBuilder()
                    .setValue(source.getLongDescription()));
        }
        if (source.getSynopses() != null) {
            builder.setSynopses(new SynopsesSerializer().serialize(source.getSynopses()));
        }
        if (source.getDescription() != null) {
            builder.setDescription(builder.getDescriptionBuilder().setValue(source.getDescription()));
        }
        if (source.getMediaType() != null) {
            builder.setMediaType(source.getMediaType().toKey());
        }
        if (source.getSpecialization() != null) {
            builder.setSpecialization(source.getSpecialization().toString());
        }
        if (source.getGenres() != null) {
            StreamSupport.stream(source.getGenres().spliterator(), false).forEach(builder::addGenre);
        }
        if (source.getSource() != null) {
            builder.setSource(source.getSource().key());
        }
        if (source.getImage() != null) {
            builder.setImage(source.getImage());
        }
        if (source.getImages() != null) {
            StreamSupport.stream(source.getImages().spliterator(), false)
                    .forEach(image -> builder.addImages(new ImageSerializer().serialize(image)));
        }
        if (source.getThumbnail() != null) {
            builder.setThumbnail(source.getThumbnail());
        }
        if (source.getFirstSeen() != null) {
            builder.setFirstSeen(new DateTimeSerializer().serialize(source.getFirstSeen()));
        }
        if (source.getLastFetched() != null) {
            builder.setLastFetched(new DateTimeSerializer().serialize(source.getLastFetched()));
        }
        if (source.getThisOrChildLastUpdated() != null) {
            builder.setThisOrChildLastUpdated(new DateTimeSerializer()
                    .serialize(source.getThisOrChildLastUpdated()));
        }
        builder.setScheduleOnly(source.isScheduleOnly());
        builder.setActivelyPublished(source.isActivelyPublished());
        if (source.getPresentationChannel() != null) {
            builder.setPresentationChannel(source.getPresentationChannel());
        }
        if (source.getPriority() != null) {
            builder.setPriority(new PrioritySerializer().serialize(source.getPriority()));
        }
        if(source.getRelatedLinks() != null) {
            StreamSupport.stream(source.getRelatedLinks().spliterator(), false)
                    .forEach(link -> builder.addRelatedLink(new RelatedLinkSerializer()
                            .serialize(link)));
        }

        return builder.build();
    }

    public T deserialize(CommonProtos.Described serialized, T target) {
        new IdentifiedSerializer<T>().deserialize(serialized.getIdentified(), target);

        if(serialized.hasTitle() && serialized.getTitle().hasValue()) {
            target.setTitle(serialized.getTitle().getValue());
        }
        if(serialized.hasShortDescription() && serialized.getShortDescription().hasValue()) {
            target.setShortDescription(serialized.getShortDescription().getValue());
        }
        if(serialized.hasMediumDescription() && serialized.getMediumDescription().hasValue()) {
            target.setMediumDescription(serialized.getMediumDescription().getValue());
        }
        if(serialized.hasLongDescription() && serialized.getMediumDescription().hasValue()) {
            target.setLongDescription(serialized.getLongDescription().getValue());
        }
        if(serialized.hasSynopses()) {
            target.setSynopses(new SynopsesSerializer().deserialize(serialized.getSynopses()
            ));
        }
        if(serialized.hasDescription() && serialized.getDescription().hasValue()) {
            target.setDescription(serialized.getDescription().getValue());
        }
        Optional<MediaType> mediaTypeOptional =
                MediaType.fromKey(serialized.getMediaType());
        if(mediaTypeOptional.isPresent()) {
            target.setMediaType(mediaTypeOptional.get());
        }
        Maybe<Specialization> specializationMaybe =
                Specialization.fromKey(serialized.getSpecialization());
        if(specializationMaybe.hasValue()) {
            target.setSpecialization(specializationMaybe.requireValue());
        }
        target.setGenres(serialized.getGenreList());
        Optional<Publisher> publisherOptional =
                Sources.fromPossibleKey(serialized.getSource());
        if(publisherOptional.isPresent()) {
            target.setPublisher(publisherOptional.get());
        }
        if(serialized.hasImage()) {
            target.setImage(serialized.getImage());
        }
        target.setImages(serialized.getImagesList().stream()
                .map(image -> new ImageSerializer().deserialize(image))
                .collect(Collectors.toList()));
        if(serialized.hasThumbnail()) {
            target.setThumbnail(serialized.getThumbnail());
        }
        if(serialized.hasFirstSeen() && serialized.getFirstSeen().hasMillis()) {
            target.setFirstSeen(new DateTimeSerializer().deserialize(serialized.getFirstSeen()));
        }
        if(serialized.hasLastFetched() && serialized.getLastFetched().hasMillis()) {
            target.setLastFetched(new DateTimeSerializer().deserialize(serialized.getLastFetched()));
        }
        if(serialized.hasThisOrChildLastUpdated() && serialized.getThisOrChildLastUpdated().hasMillis()) {
            target.setThisOrChildLastUpdated(new DateTimeSerializer()
                            .deserialize(serialized.getThisOrChildLastUpdated()));
        }
        if(serialized.hasScheduleOnly()) {
            target.setScheduleOnly(serialized.getScheduleOnly());
        }
        if(serialized.hasActivelyPublished()) {
            target.setActivelyPublished(serialized.getActivelyPublished());
        }
        if(serialized.hasPresentationChannel()) {
            target.setPresentationChannel(serialized.getPresentationChannel());
        }
        if(serialized.hasPriority()) {
            target.setPriority(new PrioritySerializer().deserialize(serialized.getPriority()
            ));
        }
        target.setRelatedLinks(serialized.getRelatedLinkList().stream()
                .map(link -> new RelatedLinkSerializer().deserialize(link))
                .collect(Collectors.toList()));

        return target;
    }
}
