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

import com.metabroadcast.common.base.Maybe;

import com.google.common.base.Optional;

public class DescribedSerializer<T extends Described> {

    private final IdentifiedSerializer<T> identifiedSerializer = new IdentifiedSerializer<>();
    private final SynopsesSerializer synopsesSerializer = new SynopsesSerializer();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();
    private final PrioritySerializer prioritySerializer = new PrioritySerializer();
    private final ImageSerializer imageSerializer = new ImageSerializer();
    private final RelatedLinkSerializer relatedLinkSerializer = new RelatedLinkSerializer();
    private final RatingSerializer ratingSerializer = new RatingSerializer();
    private final ReviewSerializer reviewSerializer = ReviewSerializer.create();

    public CommonProtos.Described serialize(T source) {
        CommonProtos.Described.Builder builder = CommonProtos.Described.newBuilder();

        builder.setIdentified(identifiedSerializer.serialize(source));

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
            builder.setSynopses(synopsesSerializer.serialize(source.getSynopses()));
        }
        if (source.getDescription() != null) {
            builder.setDescription(builder.getDescriptionBuilder()
                    .setValue(source.getDescription()));
        }
        if (source.getMediaType() != null) {
            builder.setMediaType(source.getMediaType().toKey());
        }
        if (source.getSpecialization() != null) {
            builder.setSpecialization(source.getSpecialization().toString());
        }
        if (source.getGenres() != null) {
            StreamSupport.stream(source.getGenres().spliterator(), false)
                    .forEach(builder::addGenre);
        }
        if (source.getSource() != null) {
            builder.setSource(source.getSource().key());
        }
        if (source.getImage() != null) {
            builder.setImage(source.getImage());
        }
        if (source.getImages() != null) {
            StreamSupport.stream(source.getImages().spliterator(), false)
                    .forEach(image -> builder.addImages(imageSerializer.serialize(image)));
        }
        if (source.getThumbnail() != null) {
            builder.setThumbnail(source.getThumbnail());
        }
        if (source.getFirstSeen() != null) {
            builder.setFirstSeen(dateTimeSerializer.serialize(source.getFirstSeen()));
        }
        if (source.getLastFetched() != null) {
            builder.setLastFetched(dateTimeSerializer.serialize(source.getLastFetched()));
        }
        if (source.getThisOrChildLastUpdated() != null) {
            builder.setThisOrChildLastUpdated(dateTimeSerializer
                    .serialize(source.getThisOrChildLastUpdated()));
        }
        builder.setScheduleOnly(source.isScheduleOnly());
        builder.setActivelyPublished(source.isActivelyPublished());
        if (source.getPresentationChannel() != null) {
            builder.setPresentationChannel(source.getPresentationChannel());
        }
        if (source.getPriority() != null) {
            builder.setPriority(prioritySerializer.serialize(source.getPriority()));
        }
        if (source.getRelatedLinks() != null) {
            StreamSupport.stream(source.getRelatedLinks().spliterator(), false)
                    .forEach(link -> builder.addRelatedLink(relatedLinkSerializer
                            .serialize(link)));
        }

        builder.addAllReviews(source.getReviews().stream()
                .map(reviewSerializer::serialize)
                .collect(Collectors.toList()));

        builder.addAllRatings(source.getRatings().stream()
                .map(ratingSerializer::serialize)
                .collect(Collectors.toList()));

        return builder.build();
    }

    public T deserialize(CommonProtos.Described serialized, T target) {
        identifiedSerializer.deserialize(serialized.getIdentified(), target);

        if (serialized.hasTitle() && serialized.getTitle().hasValue()) {
            target.setTitle(serialized.getTitle().getValue());
        }
        if (serialized.hasShortDescription() && serialized.getShortDescription().hasValue()) {
            target.setShortDescription(serialized.getShortDescription().getValue());
        }
        if (serialized.hasMediumDescription() && serialized.getMediumDescription().hasValue()) {
            target.setMediumDescription(serialized.getMediumDescription().getValue());
        }
        if (serialized.hasLongDescription() && serialized.getMediumDescription().hasValue()) {
            target.setLongDescription(serialized.getLongDescription().getValue());
        }
        if (serialized.hasSynopses()) {
            target.setSynopses(synopsesSerializer.deserialize(serialized.getSynopses()
            ));
        }
        if (serialized.hasDescription() && serialized.getDescription().hasValue()) {
            target.setDescription(serialized.getDescription().getValue());
        }
        Optional<MediaType> mediaTypeOptional =
                MediaType.fromKey(serialized.getMediaType());
        if (mediaTypeOptional.isPresent()) {
            target.setMediaType(mediaTypeOptional.get());
        }
        Maybe<Specialization> specializationMaybe =
                Specialization.fromKey(serialized.getSpecialization());
        if (specializationMaybe.hasValue()) {
            target.setSpecialization(specializationMaybe.requireValue());
        }
        target.setGenres(serialized.getGenreList());
        Optional<Publisher> publisherOptional =
                Sources.fromPossibleKey(serialized.getSource());
        if (publisherOptional.isPresent()) {
            target.setPublisher(publisherOptional.get());
        }
        if (serialized.hasImage()) {
            target.setImage(serialized.getImage());
        }
        target.setImages(serialized.getImagesList().stream()
                .map(imageSerializer::deserialize)
                .collect(Collectors.toList()));
        if (serialized.hasThumbnail()) {
            target.setThumbnail(serialized.getThumbnail());
        }
        if (serialized.hasFirstSeen() && serialized.getFirstSeen().hasMillis()) {
            target.setFirstSeen(dateTimeSerializer.deserialize(serialized.getFirstSeen()));
        }
        if (serialized.hasLastFetched() && serialized.getLastFetched().hasMillis()) {
            target.setLastFetched(dateTimeSerializer.deserialize(serialized.getLastFetched()));
        }
        if (serialized.hasThisOrChildLastUpdated() && serialized.getThisOrChildLastUpdated()
                .hasMillis()) {
            target.setThisOrChildLastUpdated(dateTimeSerializer
                    .deserialize(serialized.getThisOrChildLastUpdated()));
        }
        if (serialized.hasScheduleOnly()) {
            target.setScheduleOnly(serialized.getScheduleOnly());
        }
        if (serialized.hasActivelyPublished()) {
            target.setActivelyPublished(serialized.getActivelyPublished());
        }
        if (serialized.hasPresentationChannel()) {
            target.setPresentationChannel(serialized.getPresentationChannel());
        }
        if (serialized.hasPriority()) {
            target.setPriority(prioritySerializer.deserialize(serialized.getPriority()
            ));
        }
        target.setRelatedLinks(serialized.getRelatedLinkList().stream()
                .map(relatedLinkSerializer::deserialize)
                .collect(Collectors.toList()));

        // deserialization discards entities that failed to parse
        final java.util.Optional<Publisher> publisherOptionalJ8 = java.util.Optional.ofNullable(publisherOptional.orNull());
        target.setReviews(serialized.getReviewsList().stream()
                .map(reviewBuffer -> reviewSerializer.deserialize(publisherOptionalJ8, reviewBuffer))
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .collect(Collectors.toList()));

        // deserialization discards entities that failed to parse
        target.setRatings(serialized.getRatingsList().stream()
                .map(ratingSerializer::deserialize)
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .collect(Collectors.toList()));

        return target;
    }
}
