package org.atlasapi.schedule;

import org.atlasapi.content.BroadcastRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.JacksonMessageSerializer;

import com.metabroadcast.common.queue.MessagingException;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.Timestamp;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import static org.junit.Assert.assertEquals;

public class ScheduleUpdateMessageTest {

    @Test
    public void testDeSerializeMessage() throws MessagingException {

        JacksonMessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);

        ScheduleUpdate update = new ScheduleUpdate(
                Publisher.METABROADCAST,
                ScheduleRef.forChannel(Id.valueOf(1), new Interval(1, 2))
                        .addEntry(
                                Id.valueOf(1),
                                new BroadcastRef("a", Id.valueOf(1), new Interval(1, 2))
                        )
                        .build(),
                ImmutableSet.of(new BroadcastRef("b", Id.valueOf(2), new Interval(1, 2)))
        );
        ScheduleUpdateMessage msg
                = new ScheduleUpdateMessage(
                "1",
                Timestamp.of(DateTime.now(DateTimeZones.UTC)),
                update
        );

        byte[] serialized = serializer.serialize(msg);

        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);

        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(
                msg.getScheduleUpdate().getSource(),
                deserialized.getScheduleUpdate().getSource()
        );
        assertEquals(
                msg.getScheduleUpdate().getSchedule(),
                deserialized.getScheduleUpdate().getSchedule()
        );
        assertEquals(
                msg.getScheduleUpdate().getStaleBroadcasts(),
                deserialized.getScheduleUpdate().getStaleBroadcasts()
        );

    }

    @Test
    public void testDeSerializeMessageWithoutStaleBroadcasts() throws MessagingException {

        JacksonMessageSerializer<ScheduleUpdateMessage> serializer
                = JacksonMessageSerializer.forType(ScheduleUpdateMessage.class);

        ScheduleUpdate update = new ScheduleUpdate(
                Publisher.METABROADCAST,
                ScheduleRef.forChannel(Id.valueOf(1), new Interval(1, 2))
                        .addEntry(
                                Id.valueOf(1),
                                new BroadcastRef("a", Id.valueOf(1), new Interval(1, 2))
                        )
                        .build(),
                ImmutableSet.<BroadcastRef>of()
        );
        ScheduleUpdateMessage msg
                = new ScheduleUpdateMessage(
                "1",
                Timestamp.of(DateTime.now(DateTimeZones.UTC)),
                update
        );

        byte[] serialized = serializer.serialize(msg);

        ScheduleUpdateMessage deserialized = serializer.deserialize(serialized);

        assertEquals(msg.getMessageId(), deserialized.getMessageId());
        assertEquals(msg.getTimestamp(), deserialized.getTimestamp());
        assertEquals(
                msg.getScheduleUpdate().getSource(),
                deserialized.getScheduleUpdate().getSource()
        );
        assertEquals(
                msg.getScheduleUpdate().getSchedule(),
                deserialized.getScheduleUpdate().getSchedule()
        );
        assertEquals(
                msg.getScheduleUpdate().getStaleBroadcasts(),
                deserialized.getScheduleUpdate().getStaleBroadcasts()
        );

    }
}
