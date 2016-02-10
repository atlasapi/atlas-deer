package org.atlasapi.content;

import java.io.File;
import java.nio.file.Files;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.Id;
import org.atlasapi.serialization.protobuf.ContentProtos;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.LocalDate;
import org.junit.Test;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class BroadcastSerializerTest {

    private final BroadcastSerializer serializer = new BroadcastSerializer();

    @Test
    public void testDeSerializeBroadcast() {
        Broadcast broadcast = getBroadcast();

        ContentProtos.Broadcast serialized = serializer.serialize(broadcast).build();

        Broadcast deserialized = serializer.deserialize(serialized);

        checkBroadcastTimes(deserialized, broadcast);
        checkBroadcast(deserialized, broadcast);
    }

    @Test
    public void testDeserializeBlackoutRestriction() {
        DateTime start = new DateTime(DateTimeZones.UTC);
        DateTime end = start.plusHours(1);
        Broadcast broadcast = new Broadcast(Id.valueOf(1), start, end);
        broadcast.setId(Id.valueOf(1234));

        broadcast.setBlackoutRestriction(new BlackoutRestriction(true));

        ContentProtos.Broadcast serialized = serializer.serialize(broadcast).build();

        Broadcast deserialized = serializer.deserialize(serialized);

        assertThat(deserialized.getBlackoutRestriction().get().getAll(), is(true));
    }

    @Test
    public void testDeSerializeBroadcastWithScheduleDate() {
        DateTime start = new DateTime(DateTimeZones.UTC);
        DateTime end = start.plusHours(1);
        Broadcast broadcast = new Broadcast(Id.valueOf(1), start, end);
        broadcast.setScheduleDate(new LocalDate(DateTimeZones.UTC));

        ContentProtos.Broadcast serialized = serializer.serialize(broadcast).build();

        Broadcast deserialised = serializer.deserialize(serialized);

        checkBroadcastTimes(deserialised, broadcast);
        checkBroadcast(deserialised, broadcast);
    }

    @Test
    public void testDeserializationIsBackwardsCompatible() throws Exception {
        File file = new File("src/test/resources/protoc/legacy-serialized-broadcast.bin");
        byte[] msg = Files.readAllBytes(file.toPath());

        ContentProtos.Broadcast deserialized = ContentProtos.Broadcast.parseFrom(msg);
        Broadcast broadcast = serializer.deserialize(deserialized);

        checkBroadcast(broadcast, getBroadcast());
        checkBroadcastTimes(
                broadcast,
                DateTime.parse("2015-09-08T14:44:32.596Z"),
                DateTime.parse("2015-09-08T15:44:32.596Z"),
                DateTime.parse("2015-09-08T14:44:32.596Z")
        );
    }

    private void checkBroadcast(Broadcast actual, Broadcast expected) {
        assertThat(actual.getChannelId(), is(expected.getChannelId()));

        assertThat(actual.getId(), is(expected.getId()));
        assertThat(actual.getCanonicalUri(), is(expected.getCanonicalUri()));
        assertThat(actual.getAliases(), is(expected.getAliases()));
        assertThat(actual.getEquivalentTo(), is(expected.getEquivalentTo()));

        assertThat(actual.getScheduleDate(), is(expected.getScheduleDate()));
        assertThat(actual.getSourceId(), is(expected.getSourceId()));
        assertThat(actual.isActivelyPublished(), is(expected.isActivelyPublished()));
        assertThat(actual.getRepeat(), is(expected.getRepeat()));
        assertThat(actual.getSubtitled(), is(expected.getSubtitled()));
        assertThat(actual.getSigned(), is(expected.getSigned()));
        assertThat(actual.getAudioDescribed(), is(expected.getAudioDescribed()));
        assertThat(actual.getHighDefinition(), is(expected.getHighDefinition()));
        assertThat(actual.getWidescreen(), is(expected.getWidescreen()));
        assertThat(actual.getSurround(), is(expected.getSurround()));
        assertThat(actual.getLive(), is(expected.getLive()));
        assertThat(actual.getNewSeries(), is(expected.getNewSeries()));
        assertThat(actual.getPremiere(), is(expected.getPremiere()));
        assertThat(actual.is3d(), is(expected.is3d()));
        assertThat(actual.getVersionId(), is(expected.getVersionId()));
    }

    private void checkBroadcastTimes(Broadcast actual, Broadcast expected) {
        assertThat(actual.getTransmissionTime(), is(expected.getTransmissionTime()));
        assertThat(actual.getTransmissionEndTime(), is(expected.getTransmissionEndTime()));
        assertThat(actual.getLastUpdated(), is(expected.getLastUpdated()));
    }

    private void checkBroadcastTimes(Broadcast actual, DateTime transmissionTime,
            DateTime transmissionEndTime, DateTime lastUpdated) {
        assertThat(actual.getTransmissionTime(), is(transmissionTime));
        assertThat(actual.getTransmissionEndTime(), is(transmissionEndTime));
        assertThat(actual.getLastUpdated(), is(lastUpdated));
    }

    private Broadcast getBroadcast() {
        DateTime start = new DateTime(DateTimeZones.UTC);
        DateTime end = start.plusHours(1);
        Broadcast broadcast = new Broadcast(Id.valueOf(1), start, end);
        broadcast.setId(Id.valueOf(1234));
        broadcast.setCanonicalUri("uri");
        broadcast.setAliases(ImmutableSet.of(new Alias("a", "alias1"), new Alias("b", "alias2")));
        broadcast.setLastUpdated(start);

        broadcast.setScheduleDate(null);
        broadcast.withId("sourceId");
        broadcast.setIsActivelyPublished(null);
        broadcast.setRepeat(true);
        broadcast.setSubtitled(false);
        broadcast.setSigned(false);
        broadcast.setAudioDescribed(true);
        broadcast.setHighDefinition(false);
        broadcast.setWidescreen(true);
        broadcast.setSurround(false);
        broadcast.setLive(true);
        broadcast.setNewSeries(false);
        broadcast.setPremiere(true);
        broadcast.set3d(true);
        broadcast.setVersionId("version");
        return broadcast;
    }
}
