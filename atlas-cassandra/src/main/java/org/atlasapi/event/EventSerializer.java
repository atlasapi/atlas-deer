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

        if(event.title() != null) {
            builder.setTitle(builder.getTitleBuilder().setValue(event.title()).build());
        }
        if(event.getSource() != null) {
            builder.setSource(event.getSource().key());
        }
        if(event.venue() != null) {
            builder.setVenue(new TopicSerializer().serializeToBuilder(event.venue()));
        }
        if(event.startTime() != null) {
            builder.setStartTime(new DateTimeSerializer().serialize(event.startTime()));
        }
        if(event.endTime() != null) {
            builder.setEndTime(new DateTimeSerializer().serialize(event.endTime()));
        }
        if (event.participants() != null) {
            builder.addAllParticipant(event.participants().stream()
                    .map(participant -> new PersonSerializer().serialize(participant))
                    .collect(Collectors.toList()));
        }
        if (event.organisations() != null) {
            builder.addAllOrganisation(event.organisations().stream()
                    .map(organisation -> new OrganisationSerializer().serialize(organisation))
                    .collect(Collectors.toList()));
        }
        if (event.eventGroups() != null) {
            builder.addAllEventGroup(event.eventGroups().stream()
                    .map(eventGroup -> new TopicSerializer().serializeToBuilder(eventGroup).build())
                    .collect(Collectors.toList()));
        }
        if (event.content() != null) {
            builder.addAllContent(event.content().stream()
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

        Event.Builder<?> builder = Event.builder();

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
