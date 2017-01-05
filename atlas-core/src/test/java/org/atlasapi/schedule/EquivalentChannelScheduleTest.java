package org.atlasapi.schedule;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.Test;

import static org.hamcrest.MatcherAssert.assertThat;
import static org.hamcrest.collection.IsIterableContainingInOrder.contains;
import static org.hamcrest.core.Is.is;
import static org.mockito.Mockito.mock;

public class EquivalentChannelScheduleTest {

    @Test
    public void testWithLimitedBroadcasts() throws Exception {
        Channel channel = mock(Channel.class);
        Publisher source = Publisher.BBC_KIWI;
        Id channelId = Id.valueOf(1L);
        DateTime startTime = DateTime.now();
        DateTime endTime = startTime.plusHours(5);

        DateTime broadcast1StartTime = startTime;
        DateTime broadcast1EndTime = startTime.plusMinutes(30);
        Broadcast broadcast1 = new Broadcast(channelId, broadcast1StartTime, broadcast1EndTime);

        DateTime broadcast2StartTime = broadcast1EndTime;
        DateTime broadcast2EndTime = broadcast2StartTime.plusMinutes(120);
        Broadcast broadcast2 = new Broadcast(channelId, broadcast2StartTime, broadcast2EndTime);

        DateTime broadcast3StartTime = broadcast2EndTime;
        DateTime broadcast3EndTime = broadcast3StartTime.plusMinutes(150);
        Broadcast broadcast3 = new Broadcast(channelId, broadcast3StartTime, broadcast3EndTime);

        Id item1Id = Id.valueOf(2L);
        Item item1 = new Item(item1Id, source);

        Id item2Id = Id.valueOf(3L);
        Item item2 = new Item(item2Id, source);

        Id item3Id = Id.valueOf(4L);
        Item item3 = new Item(item3Id, source);

        ResourceRef itemRef1 = new ItemRef(item1Id, source, "sortKey", DateTime.now());
        ResourceRef itemRef2 = new ItemRef(item1Id, source, "sortKey", DateTime.now());
        ResourceRef itemRef3 = new ItemRef(item1Id, source, "sortKey", DateTime.now());

        EquivalenceGraph graph1 = EquivalenceGraph.valueOf(itemRef1);
        EquivalenceGraph graph2 = EquivalenceGraph.valueOf(itemRef2);
        EquivalenceGraph graph3 = EquivalenceGraph.valueOf(itemRef3);

        Equivalent<Item> equivalentItem1 = new Equivalent<>(graph1, ImmutableList.of(item1));
        Equivalent<Item> equivalentItem2 = new Equivalent<>(graph2, ImmutableList.of(item2));
        Equivalent<Item> equivalentItem3 = new Equivalent<>(graph3, ImmutableList.of(item3));

        EquivalentScheduleEntry scheduleEntry1 = EquivalentScheduleEntry.create(
                broadcast1,
                item1Id,
                equivalentItem1
        );
        EquivalentScheduleEntry scheduleEntry2 = EquivalentScheduleEntry.create(
                broadcast2,
                item2Id,
                equivalentItem2
        );
        EquivalentScheduleEntry scheduleEntry3 = EquivalentScheduleEntry.create(
                broadcast3,
                item3Id,
                equivalentItem3
        );

        EquivalentChannelSchedule objectUnderTest = new EquivalentChannelSchedule(
                channel,
                new Interval(startTime, endTime),
                ImmutableList.of(scheduleEntry3, scheduleEntry2, scheduleEntry1)
        );

        EquivalentChannelSchedule scheduleWith1Broadcast = objectUnderTest.withLimitedBroadcasts(1);
        EquivalentChannelSchedule scheduleWith2Broadcasts = objectUnderTest.withLimitedBroadcasts(2);
        EquivalentChannelSchedule scheduleWithAllBroadcasts = objectUnderTest.withLimitedBroadcasts(
                5);

        assertThat(
                Iterables.getOnlyElement(scheduleWith1Broadcast.getEntries()),
                is(scheduleEntry1)
        );
        assertThat(scheduleWith1Broadcast.getInterval().getEnd(), is(broadcast1EndTime));

        assertThat(scheduleWith2Broadcasts.getEntries(), contains(scheduleEntry1, scheduleEntry2));
        assertThat(scheduleWith2Broadcasts.getInterval().getEnd(), is(broadcast2EndTime));

        assertThat(
                scheduleWithAllBroadcasts.getEntries(),
                contains(scheduleEntry1, scheduleEntry2, scheduleEntry3)
        );
        assertThat(scheduleWithAllBroadcasts.getInterval().getEnd(), is(broadcast3EndTime));

    }
}
