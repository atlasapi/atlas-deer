package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class BroadcastWriterIT extends AbstractNeo4jIT {

    private ContentWriter contentWriter;
    private BroadcastWriter broadcastWriter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentWriter = ContentWriter.create();
        broadcastWriter = BroadcastWriter.create();
    }

    @Test
    public void writeBroadcasts() throws Exception {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(broadcast));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("channelId").asLong(), is(broadcast.getChannelId().longValue()));
        assertThat(record.get("startDateTime").asString(),
                is(broadcast.getTransmissionTime().toString()));
        assertThat(record.get("endDateTime").asString(),
                is(broadcast.getTransmissionEndTime().toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeOldBroadcastsWhenWritingBroadcasts() throws Exception {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(broadcast));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        Broadcast newBroadcast = new Broadcast(
                Id.valueOf(1L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        item.setBroadcasts(ImmutableSet.of(newBroadcast));

        broadcastWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("channelId").asLong(), is(newBroadcast.getChannelId().longValue()));
        assertThat(record.get("startDateTime").asString(),
                is(newBroadcast.getTransmissionTime().toString()));
        assertThat(record.get("endDateTime").asString(),
                is(newBroadcast.getTransmissionEndTime().toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void updateExistingBroadcast() throws Exception {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(broadcast));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        Broadcast updatedBroadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(2)
        );

        item.setBroadcasts(ImmutableSet.of(updatedBroadcast));

        broadcastWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("channelId").asLong(),
                is(updatedBroadcast.getChannelId().longValue()));
        assertThat(record.get("startDateTime").asString(),
                is(updatedBroadcast.getTransmissionTime().toString()));
        assertThat(record.get("endDateTime").asString(),
                is(updatedBroadcast.getTransmissionEndTime().toString()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeAllExistingBroadcastsWhenItemHasNone() throws Exception {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(broadcast));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        item.setBroadcasts(ImmutableSet.of());

        broadcastWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeAllExistingBroadcasts() throws Exception {
        Broadcast broadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(broadcast));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        broadcastWriter.deleteBroadcasts(item.getId(), session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void doNotWriteBroadcastsThatAreNotActivelyPublished() throws Exception {
        Broadcast nonPublishedBroadcast = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );
        nonPublishedBroadcast.setIsActivelyPublished(false);

        Broadcast publishedBroadcast = new Broadcast(
                Id.valueOf(1L),
                DateTime.now(DateTimeZone.UTC).plusHours(2),
                DateTime.now(DateTimeZone.UTC).plusHours(3)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(
                nonPublishedBroadcast, publishedBroadcast
        ));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("channelId").asLong(),
                is(publishedBroadcast.getChannelId().longValue()));

        assertThat(result.hasNext(), is(false));
    }

    @Test
    public void removeExistingBroadcastsThatAreNoLongerActivelyPublished() throws Exception {
        Broadcast broadcastA = new Broadcast(
                Id.valueOf(0L),
                DateTime.now(DateTimeZone.UTC).minusHours(1),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Broadcast broadcastB = new Broadcast(
                Id.valueOf(1L),
                DateTime.now(DateTimeZone.UTC).plusHours(2),
                DateTime.now(DateTimeZone.UTC).plusHours(3)
        );

        Item item = new Item(Id.valueOf(10L), Publisher.METABROADCAST);
        item.setBroadcasts(ImmutableSet.of(
                broadcastA, broadcastB
        ));

        contentWriter.writeContent(item, session);
        broadcastWriter.write(item, session);

        broadcastA.setIsActivelyPublished(false);

        item.setBroadcasts(ImmutableSet.of(
                broadcastA, broadcastB
        ));

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} })-[:HAS_BROADCAST]->(b:Broadcast)"
                        + "RETURN b.channelId AS channelId, "
                        + "b.startDateTime AS startDateTime, b.endDateTime AS endDateTime",
                ImmutableMap.of("id", item.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("channelId").asLong(),
                is(broadcastB.getChannelId().longValue()));

        assertThat(result.hasNext(), is(false));
    }
}
