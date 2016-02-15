package org.atlasapi.eventV2;

import java.util.stream.Collectors;

import org.atlasapi.content.ContentRefSerializer;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.IdentifiedSerializer;
import org.atlasapi.entity.PersonSerializer;
import org.atlasapi.entity.Serializer;
import org.atlasapi.organisation.OrganisationRefSerializer;
import org.atlasapi.serialization.protobuf.EventProtos;
import org.atlasapi.source.Sources;
import org.atlasapi.topic.TopicSerializer;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;

public class EventV2Serializer implements Serializer<EventV2, byte[]> {

    private final IdentifiedSerializer<EventV2> identifiedSerializer = new IdentifiedSerializer<>();
    private final TopicSerializer topicSerializer = new TopicSerializer();
    private final DateTimeSerializer dateTimeSerializer = new DateTimeSerializer();
    private final PersonSerializer personSerializer = new PersonSerializer();
    private final OrganisationRefSerializer organisationRefSerializer = new OrganisationRefSerializer();
    private final ContentRefSerializer contentRefSerializer = new ContentRefSerializer(null);

    @Override
    public byte[] serialize(EventV2 event) {
        EventProtos.EventV2.Builder builder = EventProtos.EventV2.newBuilder();

        builder.setIdentified(identifiedSerializer.serialize(event));

        if (event.getTitle() != null) {
            builder.setTitle(builder.getTitleBuilder().setValue(event.getTitle()).build());
        }
        if (event.getSource() != null) {
            builder.setSource(event.getSource().key());
        }
        if (event.getVenue() != null) {
            builder.setVenue(topicSerializer.serializeToBuilder(event.getVenue()));
        }
        if (event.getStartTime() != null) {
            builder.setStartTime(dateTimeSerializer.serialize(event.getStartTime()));
        }
        if (event.getEndTime() != null) {
            builder.setEndTime(dateTimeSerializer.serialize(event.getEndTime()));
        }
        if (event.getParticipants() != null) {
            builder.addAllParticipant(event.getParticipants().stream()
                    .map(personSerializer::serialize)
                    .collect(Collectors.toList()));
        }
        if (event.getOrganisations() != null) {
            builder.addAllOrganisation(event.getOrganisations().stream()
                    .map(organisationRefSerializer::serialize)
                    .collect(Collectors.toList()));
        }
        if (event.getEventGroups() != null) {
            builder.addAllEventGroup(event.getEventGroups().stream()
                    .map(eventGroup -> topicSerializer.serializeToBuilder(eventGroup).build())
                    .collect(Collectors.toList()));
        }
        if (event.getContent() != null) {
            builder.addAllContent(event.getContent().stream()
                    .map(content -> contentRefSerializer.serialize(content).build())
                    .collect(Collectors.toList()));
        }

        return builder.build().toByteArray();
    }

    @Override
    public EventV2 deserialize(byte[] dest) {
        try {
            return deserializeInternal(dest);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }

    private EventV2 deserializeInternal(byte[] dest)
            throws com.google.protobuf.InvalidProtocolBufferException {
        EventProtos.EventV2 msg = EventProtos.EventV2.parseFrom(dest);

        EventV2.Builder<?, ?> builder = EventV2.builder();

        identifiedSerializer.deserialize(msg.getIdentified(), builder);

        if (msg.hasTitle()) {
            builder.withTitle(msg.getTitle().getValue());
        }
        if (msg.hasSource()) {
            builder.withSource(Sources.fromPossibleKey(msg.getSource()).get());
        }
        if (msg.hasVenue()) {
            builder.withVenue(topicSerializer.deserialize(msg.getVenue()));
        }
        if (msg.hasStartTime()) {
            builder.withStartTime(dateTimeSerializer.deserialize(msg.getStartTime()));
        }
        if (msg.hasEndTime()) {
            builder.withEndTime(dateTimeSerializer.deserialize(msg.getEndTime()));
        }
        builder.withParticipants(msg.getParticipantList().stream()
                .map(personSerializer::deserialize)
                .collect(Collectors.toList()));
        builder.withOrganisations(msg.getOrganisationList().stream()
                .map(organisationRefSerializer::deserialize)
                .collect(Collectors.toList()));
        builder.withEventGroups(msg.getEventGroupList().stream()
                .map(topicSerializer::deserialize)
                .collect(Collectors.toList()));
        builder.withContent(msg.getContentList().stream()
                .map(content -> (ItemRef) contentRefSerializer.deserialize(content))
                .collect(Collectors.toList()));

        return builder.build();
    }
}
