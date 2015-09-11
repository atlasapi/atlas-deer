package org.atlasapi.event;

import java.util.stream.Collectors;

import org.atlasapi.content.ContentRefSerializer;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.DateTimeSerializer;
import org.atlasapi.entity.IdentifiedSerializer;
import org.atlasapi.entity.PersonSerializer;
import org.atlasapi.entity.Serializer;
import org.atlasapi.serialization.protobuf.EventProtos;
import org.atlasapi.source.Sources;
import org.atlasapi.topic.TopicSerializer;

import com.google.common.base.Throwables;
import com.google.protobuf.InvalidProtocolBufferException;

public class EventSerializer implements Serializer<Event, byte[]> {

    @Override
    public byte[] serialize(Event event) {
        EventProtos.Event.Builder builder = EventProtos.Event.newBuilder();

        builder.setIdentified(new IdentifiedSerializer<Event>().serialize(event));

        if(event.getTitle() != null) {
            builder.setTitle(builder.getTitleBuilder().setValue(event.getTitle()).build());
        }
        if(event.getSource() != null) {
            builder.setSource(event.getSource().key());
        }
        if(event.getVenue() != null) {
            builder.setVenue(new TopicSerializer().serializeToBuilder(event.getVenue()));
        }
        if(event.getStartTime() != null) {
            builder.setStartTime(new DateTimeSerializer().serialize(event.getStartTime()));
        }
        if(event.getEndTime() != null) {
            builder.setEndTime(new DateTimeSerializer().serialize(event.getEndTime()));
        }
        if (event.getParticipants() != null) {
            builder.addAllParticipant(event.getParticipants().stream()
                    .map(participant -> new PersonSerializer().serialize(participant))
                    .collect(Collectors.toList()));
        }
        if (event.getOrganisations() != null) {
            builder.addAllOrganisation(event.getOrganisations().stream()
                    .map(organisation -> new OrganisationSerializer().serialize(organisation))
                    .collect(Collectors.toList()));
        }
        if (event.getEventGroups() != null) {
            builder.addAllEventGroup(event.getEventGroups().stream()
                    .map(eventGroup -> new TopicSerializer().serializeToBuilder(eventGroup).build())
                    .collect(Collectors.toList()));
        }
        if (event.getContent() != null) {
            builder.addAllContent(event.getContent().stream()
                    .map(content -> new ContentRefSerializer(null).serialize(content).build())
                    .collect(Collectors.toList()));
        }

        return builder.build().toByteArray();
    }

    @Override
    public Event deserialize(byte[] dest) {
        try {
            return deserializeInternal(dest);
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }

    private Event deserializeInternal(byte[] dest)
            throws com.google.protobuf.InvalidProtocolBufferException {
        EventProtos.Event msg = EventProtos.Event.parseFrom(dest);

        Event.Builder<?, ?> builder = Event.builder();

        new IdentifiedSerializer<Event>().deserialize(msg.getIdentified(), builder);

        if (msg.hasTitle()) {
            builder.withTitle(msg.getTitle().getValue());
        }
        if (msg.hasSource()) {
            builder.withSource(Sources.fromPossibleKey(msg.getSource()).get());
        }
        if (msg.hasVenue()) {
            builder.withVenue(new TopicSerializer().deserialize(msg.getVenue()));
        }
        if (msg.hasStartTime()) {
            builder.withStartTime(new DateTimeSerializer().deserialize(msg.getStartTime()));
        }
        if (msg.hasEndTime()) {
            builder.withEndTime(new DateTimeSerializer().deserialize(msg.getEndTime()));
        }
        builder.withParticipants(msg.getParticipantList().stream()
                .map(participant -> new PersonSerializer().deserialize(participant))
                .collect(Collectors.toList()));
        builder.withOrganisations(msg.getOrganisationList().stream()
                .map(organisation -> new OrganisationSerializer().deserialize(organisation))
                .collect(Collectors.toList()));
        builder.withEventGroups(msg.getEventGroupList().stream()
                .map(group -> new TopicSerializer().deserialize(group))
                .collect(Collectors.toList()));
        builder.withContent(msg.getContentList().stream()
                .map(content -> (ItemRef) new ContentRefSerializer(null).deserialize(content))
                .collect(Collectors.toList()));

        return builder.build();
    }
}
