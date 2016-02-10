package org.atlasapi.schedule;

import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.testing.AbstractTester;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public final class EquivalentScheduleStoreTester
        extends AbstractTester<EquivalentScheduleStoreSubjectGenerator> {

    public void testWritingNewSchedule() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        getSubjectGenerator().getContentStore().writeContent(item);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item));
    }

    public void testUpdatingASchedule() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(channel, interval).withId("sid1");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        Broadcast broadcast2 = new Broadcast(channel, interval).withId("sid2");
        item2.addBroadcast(broadcast2);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();
        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of(
                                broadcast1.toRef()
                        )
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item2));
        assertThat(Iterables.getOnlyElement(schedule.getEntries()).getBroadcast(), is(broadcast2));
    }

    public void testWritingRepeatedItem() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 18, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(
                channel,
                new Interval(interval.getStart(), interval.getStart().plusHours(1))
        ).withId("sid1");
        Broadcast broadcast2 = new Broadcast(
                channel,
                new Interval(interval.getEnd().minusHours(1), interval.getEnd())
        ).withId("sid2");
        item.addBroadcast(broadcast1);
        item.addBroadcast(broadcast2);

        getSubjectGenerator().getContentStore().writeContent(item);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast1.toRef())
                .addEntry(item.getId(), broadcast2.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        ImmutableList<EquivalentScheduleEntry> entries = schedule.getEntries();

        Item first = Iterables.getOnlyElement(entries.get(0).getItems().getResources());
        assertThat(Iterables.getOnlyElement(first.getBroadcasts()), is(broadcast1));
        Item second = Iterables.getOnlyElement(entries.get(1).getItems().getResources());
        assertThat(Iterables.getOnlyElement(second.getBroadcasts()), is(broadcast2));
    }

    public void testWritingNewScheduleWithEquivalentItems() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(item.toRef(),
                        ImmutableSet.<ResourceRef>of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );

        getSubjectGenerator().getContentStore().writeContent(item);
        getSubjectGenerator().getContentStore().writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }

    public void testResolvingScheduleFiltersItemsAccordingToSelectedSources() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(
                item.toRef(),
                ImmutableSet.<ResourceRef>of(equiv.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        getSubjectGenerator().getContentStore().writeContent(item);
        getSubjectGenerator().getContentStore().writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(equiv));
    }

    public void testWritingNewScheduleWithEquivalentItemsChoosesBestEquivalent() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast.withId("sid1"));
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Item otherEquiv = new Item(Id.valueOf(3), Publisher.BBC);
        otherEquiv.addBroadcast(new Broadcast(
                channel,
                interval.getEnd(),
                interval.getEnd().plusHours(1)
        ).withId("sid2"));
        otherEquiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(item.toRef(),
                        ImmutableSet.<ResourceRef>of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );
        getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(
                otherEquiv.toRef(),
                ImmutableSet.<ResourceRef>of(item.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        getSubjectGenerator().getContentStore().writeContent(item);
        getSubjectGenerator().getContentStore().writeContent(equiv);
        getSubjectGenerator().getContentStore().writeContent(otherEquiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(
                        ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }

    public void testUpdatingEquivalences() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast.withId("sid1"));
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Item otherEquiv = new Item(Id.valueOf(3), Publisher.PA);
        otherEquiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        getSubjectGenerator().getEquivalenceGraphStore().updateEquivalences(
                otherEquiv.toRef(),
                ImmutableSet.<ResourceRef>of(
                        item.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        getSubjectGenerator().getContentStore().writeContent(item);
        getSubjectGenerator().getContentStore().writeContent(equiv);
        getSubjectGenerator().getContentStore().writeContent(otherEquiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(new ScheduleUpdate(
                Publisher.METABROADCAST,
                scheduleRef,
                ImmutableSet.<BroadcastRef>of()
        ));

        EquivalenceGraphUpdate equivUpdate = getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(
                        item.toRef(),
                        ImmutableSet.<ResourceRef>of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
                .get();

        getSubjectGenerator().getEquivalentScheduleStore().updateEquivalences(equivUpdate);

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(broadcastItems.getResources().size(), is(2));
        assertThat(broadcastItems.getResources(), hasItems(item, equiv));
    }

    public void testResolvingAnEmptySchedule() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC),
                new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC)
        );

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        assertThat(schedule.getEntries().size(), is(0));
        assertThat(schedule.getChannel(), is(channel));

    }

    public void testResolvingScheduleFiltersByRequestedInterval() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);

        DateTime one = new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 03, 21, 18, 00, 00, 000, DateTimeZones.UTC);
        DateTime four = new DateTime(2014, 03, 21, 19, 00, 00, 000, DateTimeZones.UTC);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast1 = new Broadcast(channel, one, two).withId("12");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast2 = new Broadcast(channel, two, three).withId("23");
        item2.addBroadcast(broadcast2);

        Item item3 = new Item(Id.valueOf(3), Publisher.METABROADCAST);
        item3.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast3 = new Broadcast(channel, three, four).withId("34");
        item3.addBroadcast(broadcast3);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);
        getSubjectGenerator().getContentStore().writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef
                .forChannel(channel.getId(), new Interval(one, four))
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        new Interval(one.plusMinutes(30), one.plusMinutes(30)),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item1));

        resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        new Interval(one.plusMinutes(30), three),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        assertThat(schedule.getEntries().size(), is(2));
        assertThat(
                Iterables.getOnlyElement(schedule.getEntries().get(0).getItems().getResources()),
                is(item1)
        );
        assertThat(
                Iterables.getOnlyElement(schedule.getEntries().get(1).getItems().getResources()),
                is(item2)
        );

        resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        new Interval(one.plusMinutes(30), three.plusMinutes(30)),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        assertThat(schedule.getEntries().size(), is(3));
        assertThat(
                Iterables.getOnlyElement(schedule.getEntries().get(0).getItems().getResources()),
                is(item1)
        );
        assertThat(
                Iterables.getOnlyElement(schedule.getEntries().get(1).getItems().getResources()),
                is(item2)
        );
        assertThat(
                Iterables.getOnlyElement(schedule.getEntries().get(2).getItems().getResources()),
                is(item3)
        );

        resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel), new Interval(three, three),
                        Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST)
                ));

        schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item3));

    }

    public void testResolvingScheduleFromMultipleChannels() throws Exception {

        Channel channel1 = Channel.builder(Publisher.BBC).build();
        channel1.setId(1L);

        Channel channel2 = Channel.builder(Publisher.BBC).build();
        channel2.setId(2L);

        DateTime one = new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 03, 21, 18, 00, 00, 000, DateTimeZones.UTC);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast1 = new Broadcast(channel1, one, two).withId("12");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast2 = new Broadcast(channel2, two, three).withId("23");
        item2.addBroadcast(broadcast2);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef scheduleRef = ScheduleRef
                .forChannel(channel1.getId(), new Interval(one, three))
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel1, channel2), new Interval(one, three),
                        Publisher.METABROADCAST, ImmutableSet.of(Publisher.METABROADCAST)
                ));

        assertThat(resolved.channelSchedules().size(), is(2));
        EquivalentChannelSchedule sched1 = resolved.channelSchedules().get(0);
        EquivalentChannelSchedule sched2 = resolved.channelSchedules().get(1);

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(sched1.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item1));

        broadcastItems = Iterables.getOnlyElement(sched2.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item2));

    }

    public void testResolvingScheduleFromMultipleChannelsWithCountParameter() throws Exception {

        Channel channel1 = Channel.builder(Publisher.BBC).build();
        channel1.setId(1L);

        Channel channel2 = Channel.builder(Publisher.BBC).build();
        channel2.setId(2L);

        DateTime start = DateTime.now();
        DateTime startPlus1h = start.plusHours(1);
        DateTime startPlus2h = start.plusHours(2);
        DateTime startPlus3h = start.plusHours(3);
        DateTime startPlus4h = start.plusHours(4);
        DateTime startPlus25h = start.plusHours(25);
        DateTime startPlus26h = start.plusHours(26);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Item item3 = new Item(Id.valueOf(3), Publisher.METABROADCAST);
        item3.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        Broadcast broadcast1Channel1 = new Broadcast(channel1, start, startPlus1h).withId("11");
        item1.addBroadcast(broadcast1Channel1);
        Broadcast broadcast2Channel1 = new Broadcast(
                channel1,
                startPlus1h,
                startPlus2h
        ).withId("12");
        item2.addBroadcast(broadcast2Channel1);
        Broadcast broadcast3Channel1 = new Broadcast(
                channel1,
                startPlus2h,
                startPlus3h
        ).withId("13");
        item3.addBroadcast(broadcast3Channel1);
        Broadcast broadcast4Channel1 = new Broadcast(
                channel1,
                startPlus3h,
                startPlus4h
        ).withId("14");
        item2.addBroadcast(broadcast4Channel1);

        Broadcast broadcast1Channel2 = new Broadcast(channel2, start, startPlus1h).withId("21");
        item1.addBroadcast(broadcast1Channel2);
        Broadcast broadcast2Channel2 = new Broadcast(channel2, startPlus1h, startPlus25h).withId(
                "22");
        item2.addBroadcast(broadcast2Channel2);
        Broadcast broadcast3Channel2 = new Broadcast(channel2, startPlus25h, startPlus26h).withId(
                "23");
        item3.addBroadcast(broadcast3Channel2);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);
        getSubjectGenerator().getContentStore().writeContent(item3);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(start, startPlus3h))
                .addEntry(item1.getId(), broadcast1Channel1.toRef())
                .addEntry(item2.getId(), broadcast2Channel1.toRef())
                .addEntry(item3.getId(), broadcast3Channel1.toRef())
                .addEntry(item2.getId(), broadcast4Channel1.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.<BroadcastRef>of()
                ));

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel2.getId(), new Interval(start, startPlus26h))
                .addEntry(item1.getId(), broadcast1Channel2.toRef())
                .addEntry(item2.getId(), broadcast2Channel2.toRef())
                .addEntry(item3.getId(), broadcast3Channel2.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1, channel2),
                        start,
                        3,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
        );

        assertThat(resolved.channelSchedules().size(), is(2));
        EquivalentChannelSchedule sched1 = resolved.channelSchedules().get(0);
        EquivalentChannelSchedule sched2 = resolved.channelSchedules().get(1);

        assertThat(sched1.getEntries().size(), is(3));

        assertThat(sched2.getEntries().size(), is(2));

        assertThat(sched1.getInterval().getEnd(), is(startPlus3h));
        assertThat(sched2.getInterval().getEnd(), is(startPlus25h));

    }

    public void testWritingScheduleRemovesExtraneousBroadcasts() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);

        DateTime one = new DateTime(2014, 03, 21, 16, 00, 00, 000, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 03, 21, 17, 00, 00, 000, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 03, 21, 18, 00, 00, 000, DateTimeZones.UTC);
        DateTime four = new DateTime(2014, 03, 21, 19, 00, 00, 000, DateTimeZones.UTC);

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast1 = new Broadcast(channel, one, two).withId("sid1");
        Broadcast broadcast2 = new Broadcast(channel, two, three).withId("sid2");
        Broadcast broadcast3 = new Broadcast(channel, three, four).withId("sid3");
        item.addBroadcast(broadcast1);
        item.addBroadcast(broadcast2);
        item.addBroadcast(broadcast3);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast4 = new Broadcast(channel, one, two).withId("sid4");
        Broadcast broadcast5 = new Broadcast(channel, three, four).withId("sid5");
        equiv.addBroadcast(broadcast4);
        equiv.addBroadcast(broadcast5);

        getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(item.toRef(),
                        ImmutableSet.<ResourceRef>of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );

        getSubjectGenerator().getContentStore().writeContent(item);
        getSubjectGenerator().getContentStore().writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), new Interval(one, two))
                .addEntry(item.getId(), broadcast1.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(new ScheduleUpdate(
                Publisher.METABROADCAST,
                scheduleRef,
                ImmutableSet.<BroadcastRef>of()
        ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        new Interval(one, two),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        ImmutableMap<Id, Item> items = Maps.uniqueIndex(
                broadcastItems.getResources(),
                Identifiables.toId()
        );
        assertThat(items.size(), is(2));
        assertThat(
                Iterables.getOnlyElement(items.get(item.getId()).getBroadcasts()),
                is(broadcast1)
        );
        assertThat(
                Iterables.getOnlyElement(items.get(equiv.getId()).getBroadcasts()),
                is(broadcast4)
        );
    }

    public void testUpdatingContentUpdatesContent() throws Exception {
        Channel channel1 = Channel.builder(Publisher.BBC).build();
        channel1.setId(1L);

        DateTime now = DateTime.now();
        DateTime nowPlus1h = now.plusHours(1);
        DateTime nowPlus2h = now.plusHours(2);
        DateTime nowMinus25h = now.minusHours(25);
        DateTime nowMinus26h = now.minusHours(26);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        item1.setTitle("Item1 title");

        Item item2 = new Item(Id.valueOf(2), Publisher.BBC_KIWI);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        item2.setTitle("Item2 title");

        Broadcast broadcast1 = new Broadcast(channel1, now, nowPlus1h).withId("11");
        Broadcast broadcast2 = new Broadcast(channel1, nowPlus1h, nowPlus2h).withId("12");

        item1.addBroadcast(broadcast1);

        item2.addBroadcast(broadcast2);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(now, nowPlus2h))
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(
                new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.<BroadcastRef>of()
                )
        );

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(nowMinus26h, nowMinus25h))
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(
                new ScheduleUpdate(
                        Publisher.BBC_KIWI,
                        scheduleRef2,
                        ImmutableSet.<BroadcastRef>of()
                )
        );

        EquivalentChannelSchedule currentMbSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentMbSched.getEntries().size(), is(1));
        assertThat(
                currentMbSched.getEntries().get(0).getItems().getResources(),
                is(ImmutableSet.of(item1))
        );

        EquivalentChannelSchedule currentBbcSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.BBC_KIWI,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentBbcSched.getEntries().size(), is(1));
        assertThat(
                currentBbcSched.getEntries().get(0).getItems().getResources(),
                is(ImmutableSet.of(item2))
        );

        EquivalenceGraphUpdate equivUpdate = getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(
                        item1.toRef(),
                        ImmutableSet.<ResourceRef>of(item2.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                ).get();
        EquivalenceGraphUpdate equivUpdate2 = getSubjectGenerator().getEquivalenceGraphStore()
                .updateEquivalences(
                        item2.toRef(),
                        ImmutableSet.<ResourceRef>of(item1.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                ).get();

        getSubjectGenerator().getEquivalentScheduleStore().updateEquivalences(equivUpdate);
        getSubjectGenerator().getEquivalentScheduleStore().updateEquivalences(equivUpdate2);

        currentMbSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentMbSched.getEntries().size(), is(1));
        assertThat(
                currentMbSched.getEntries().get(0).getItems().getResources(),
                is(ImmutableSet.of(item1, item2))
        );

        currentBbcSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentBbcSched.getEntries().size(), is(1));
        assertThat(
                currentBbcSched.getEntries().get(0).getItems().getResources(),
                is(ImmutableSet.of(item1, item2))
        );

        item1.setTitle("New Item 1 title");

        getSubjectGenerator().getContentStore().writeContent(item1);

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateContent(ImmutableSet.of(item1, item2));

        currentMbSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentMbSched.getEntries().size(), is(1));
        assertThat(
                Iterables.getOnlyElement(
                        Iterables.filter(
                                currentMbSched.getEntries().get(0).getItems().getResources(),
                                i -> i.getId().equals(item1.getId())
                        )
                ).getTitle(),
                is(item1.getTitle())
        );

        currentBbcSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentBbcSched.getEntries().size(), is(1));
        assertThat(
                Iterables.getOnlyElement(
                        Iterables.filter(
                                currentBbcSched.getEntries().get(0).getItems().getResources(),
                                i -> i.getId().equals(item1.getId())
                        )
                ).getTitle(),
                is(item1.getTitle())
        );

    }

    public void testUpdatingWithContentWithIncorrectBroadcastsDoesnBlowUp() throws Exception {
        Channel channel1 = Channel.builder(Publisher.BBC).build();
        channel1.setId(1L);

        DateTime now = DateTime.now();
        DateTime nowPlus1h = now.plusHours(1);
        DateTime nowPlus2h = now.plusHours(2);
        DateTime nowMinus25h = now.minusHours(25);
        DateTime nowMinus26h = now.minusHours(26);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        item1.setTitle("Item1 title");

        Item item2 = new Item(Id.valueOf(2), Publisher.BBC_KIWI);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        item2.setTitle("Item2 title");

        Broadcast broadcast1 = new Broadcast(channel1, now, nowPlus1h).withId("11");
        Broadcast broadcast11 = new Broadcast(channel1, now, nowPlus1h).withId("111");
        Broadcast broadcast2 = new Broadcast(channel1, nowPlus1h, nowPlus2h).withId("12");

        item1.addBroadcast(broadcast1);
        item1.addBroadcast(broadcast11);

        item2.addBroadcast(broadcast2);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(now, nowPlus2h))
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(
                new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.<BroadcastRef>of()
                )
        );

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(nowMinus26h, nowMinus25h))
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore().updateSchedule(
                new ScheduleUpdate(
                        Publisher.BBC_KIWI,
                        scheduleRef2,
                        ImmutableSet.<BroadcastRef>of()
                )
        );
        getSubjectGenerator().getEquivalentScheduleStore()
                .updateContent(ImmutableSet.of(item1, item2));

        EquivalentChannelSchedule currentMbSched = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel1),
                        new Interval(now, nowPlus2h),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC_KIWI)
                )
        ).channelSchedules().get(0);

        assertThat(currentMbSched.getEntries().size(), is(1));
        assertThat(
                currentMbSched.getEntries().get(0).getItems().getResources(),
                is(ImmutableSet.of(item1, item2))
        );

    }

    public void testUpdatingAScheduleRemovesStaleBroadcastsEvenIfNotPresentInStaleBroadcastsInUpdate()
            throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        DateTime beforeStart = new DateTime(2014, 3, 21, 0, 0, 0, 0, DateTimeZones.UTC);
        DateTime start = new DateTime(2014, 3, 21, 1, 0, 0, 0, DateTimeZones.UTC);
        DateTime midday = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime fourPm = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2014, 3, 21, 23, 0, 0, 0, DateTimeZones.UTC);
        DateTime afterEnd = new DateTime(2014, 3, 22, 23, 30, 0, 0, DateTimeZones.UTC);
        Interval interval = new Interval(
                start,
                end
        );

        Item beforeItem = new Item(Id.valueOf(5), Publisher.METABROADCAST);
        Broadcast beforeBroadcast1 = new Broadcast(channel, beforeStart, start).withId("sid5");
        beforeItem.addBroadcast(beforeBroadcast1);

        Item afterItem = new Item(Id.valueOf(6), Publisher.METABROADCAST);
        Broadcast afterBroadcast = new Broadcast(channel, end, afterEnd).withId("sid6");
        afterItem.addBroadcast(afterBroadcast);

        getSubjectGenerator().getContentStore().writeContent(beforeItem);
        getSubjectGenerator().getContentStore().writeContent(afterItem);

        ScheduleRef scheduleRefBefore = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(beforeStart, start)
        )
                .addEntry(beforeItem.getId(), beforeBroadcast1.toRef())
                .build();

        ScheduleRef scheduleRefAfter = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(end, afterEnd)
        )
                .addEntry(afterItem.getId(), afterBroadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRefBefore,
                        ImmutableSet.<BroadcastRef>of()
                ));
        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRefAfter,
                        ImmutableSet.<BroadcastRef>of()
                ));

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(channel, start, midday).withId("sid1");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        Broadcast broadcast2 = new Broadcast(channel, midday, fourPm).withId("sid2");
        item2.addBroadcast(broadcast2);

        Item item3 = new Item(Id.valueOf(3), Publisher.METABROADCAST);
        Broadcast broadcast3 = new Broadcast(channel, fourPm, end).withId("sid3");
        item3.addBroadcast(broadcast3);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);
        getSubjectGenerator().getContentStore().writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        Item item4 = new Item(Id.valueOf(4), Publisher.METABROADCAST);
        Broadcast broadcast4 = new Broadcast(channel, midday, end).withId("sid4");
        item4.addBroadcast(broadcast4);

        getSubjectGenerator().getContentStore().writeContent(item4);
        ScheduleRef scheduleRef2 = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item4.getId(), broadcast4.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        assertThat(schedule.getEntries().size(), is(2));
        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(0).getItems().getResources()
                ),
                is(item1)
        );
        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(1).getItems().getResources()
                ),
                is(item4)
        );
        assertThat(
                schedule.getEntries().get(0).getBroadcast().getTransmissionTime(),
                is(start)
        );
        assertThat(
                schedule.getEntries().get(0).getBroadcast().getTransmissionEndTime(),
                is(midday)
        );
        assertThat(
                schedule.getEntries().get(1).getBroadcast().getTransmissionTime(),
                is(midday)
        );
        assertThat(
                schedule.getEntries().get(1).getBroadcast().getTransmissionEndTime(),
                is(end)
        );

        EquivalentChannelSchedule beforeSchedule
                = Iterables.getOnlyElement(
                get(
                        getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                                ImmutableList.of(channel),
                                new Interval(beforeStart, start.minusSeconds(1)),
                                Publisher.METABROADCAST,
                                ImmutableSet.of(Publisher.METABROADCAST)
                        )).channelSchedules()
        );

        assertThat(beforeSchedule.getEntries().size(), is(1));

        EquivalentChannelSchedule afterSchedule
                = Iterables.getOnlyElement(
                get(
                        getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                                ImmutableList.of(channel),
                                new Interval(end.plusSeconds(1), afterEnd),
                                Publisher.METABROADCAST,
                                ImmutableSet.of(Publisher.METABROADCAST)
                        )).channelSchedules()
        );

        assertThat(afterSchedule.getEntries().size(), is(1));

    }

    public void testUpdatingAScheduleRemovesStaleBroadcastsEvenIfNotPresentInStaleBroadcastsInUpdateAndBroadcastMoved()
            throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        DateTime start = new DateTime(2014, 3, 21, 0, 0, 0, 0, DateTimeZones.UTC);
        DateTime midday = new DateTime(2014, 3, 21, 12, 0, 0, 0, DateTimeZones.UTC);
        DateTime fourPm = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime end = new DateTime(2014, 3, 22, 0, 0, 0, 0, DateTimeZones.UTC);
        Interval interval = new Interval(
                start,
                end
        );

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast1 = new Broadcast(channel, start, midday).withId("sid1");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        Broadcast broadcast2 = new Broadcast(channel, midday, fourPm).withId("sid2");
        item2.addBroadcast(broadcast2);

        Item item3 = new Item(Id.valueOf(3), Publisher.METABROADCAST);
        Broadcast broadcast3 = new Broadcast(channel, fourPm, end).withId("sid3");
        item3.addBroadcast(broadcast3);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);
        getSubjectGenerator().getContentStore().writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.<BroadcastRef>of()
                ));

        Item item4 = new Item(Id.valueOf(4), Publisher.METABROADCAST);
        Broadcast broadcast4 = new Broadcast(channel, midday, end).withId("sid3");
        item4.addBroadcast(broadcast4);

        getSubjectGenerator().getContentStore().writeContent(item4);
        ScheduleRef scheduleRef2 = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item4.getId(), broadcast4.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved
                = get(getSubjectGenerator().getEquivalentScheduleStore()
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        assertThat(schedule.getEntries().size(), is(2));
        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(0).getItems().getResources()
                ),
                is(item1)
        );
        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(1).getItems().getResources()
                ),
                is(item4)
        );
        assertThat(
                schedule.getEntries().get(0).getBroadcast().getTransmissionTime(),
                is(start)
        );
        assertThat(
                schedule.getEntries().get(0).getBroadcast().getTransmissionEndTime(),
                is(midday)
        );
        assertThat(
                schedule.getEntries().get(1).getBroadcast().getTransmissionTime(),
                is(midday)
        );
        assertThat(
                schedule.getEntries().get(1).getBroadcast().getTransmissionEndTime(),
                is(end)
        );

    }

    /**
     * If two broadcasts exist on consecutive schedule days with the same ID, and
     * the one on the second day is marked as stale, the first broadcast should
     * not be removed. In the case where the update interval spans from the first
     * day to the second day, the broadcast on the first day was being removed.
     *
     * TODO: If the remaining, valid, broadcasts is within the update window, but
     *       on the same calendar day as the stale broadcast, it will be removed.
     */
    public void testOnlyRemovesStaleBroadcastFromOutsideInterval() throws Exception {


        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        DateTime item1BroadcastStart = new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC);
        DateTime item1BroadcastEnd = new DateTime(2015, 10, 25, 22, 0, 0, 0, DateTimeZones.UTC);
        DateTime item2BroadcastStart = new DateTime(2015, 10, 26, 19, 30, 0, 0, DateTimeZones.UTC);
        DateTime item2BroadcastEnd = new DateTime(2015, 10, 26, 23, 0, 0, 0, DateTimeZones.UTC);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);

        Broadcast item1Broadcast = new Broadcast(channel, item1BroadcastStart, item1BroadcastEnd).withId("bid1");
        Broadcast item2Broadcast = new Broadcast(channel, item2BroadcastStart, item2BroadcastEnd).withId("bid1");

        item1.addBroadcast(item1Broadcast);
        item2.addBroadcast(item2Broadcast);

        getSubjectGenerator().getContentStore().writeContent(item1);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef update1 = ScheduleRef.forChannel(channel.getId(), new Interval(item1BroadcastStart, item2BroadcastEnd))
                .addEntry(item1.getId(), item1Broadcast.toRef())
                .addEntry(item2.getId(), item2Broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                             .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, update1, ImmutableSet.<BroadcastRef>of()));

        Broadcast item2NewBroadcast = new Broadcast(channel, item2BroadcastStart, item2BroadcastEnd).withId("bid2");
        item2.setBroadcasts(ImmutableSet.of(item2NewBroadcast));

        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef update2 = ScheduleRef.forChannel(channel.getId(), new Interval(item1BroadcastStart, item2BroadcastEnd))
                .addEntry(item1.getId(), item1Broadcast.toRef())
                .addEntry(item2.getId(), item2NewBroadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST, update1, ImmutableSet.<BroadcastRef>of(item2Broadcast.toRef())));

        EquivalentSchedule resolved = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel),
                        new Interval(item1BroadcastStart, item1BroadcastEnd),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
        );

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        assertThat(schedule.getEntries().size(), is(1));
    }

    public void testDoesnRemoveStaleBroadcastsFromOutsideInterval() throws Exception {

        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        /*
        The test case here is testing that when there's a broadcast that spans multiple rows gets
        shortened to be only one row will get deleted from the correct row under certain conditions.
        Example sequence of updates:
        Update 1
        item1 -> (2015-10-25T19:30:00.000Z -> 2015-10-26T10:00:00.000Z)
        item2 -> (2015-10-26T10:00:00.000Z -> 2015-10-26T12:00:00.000Z)
        Update 2
        item1 -> (2015-10-25T19:30:00.000Z -> 2015-10-26T00:00:00.000Z)
        item3 -> (2015-10-26T00:00:00.000Z -> 2015-10-26T06:00:00.000Z)
        Update 3
        item4 -> (2015-10-26T06:00:00.000Z -> 2015-10-26T10:00:00.000Z)
        item2 -> (2015-10-26T10:00:00.000Z -> 2015-10-26T12:00:00.000Z)

        After update2 the broadcast for item1 will be modified in the row for 2015-10-25 with updated
        end time, but will be orphaned in the row for 2015-10-26 with incorrect times.
        During update 3 for row for 2015-10-26, this broadcast is considered stale(it's not in the update for the interval)
        and it's get deleted. The problem is that it is deleted from the interval of the broadcast(which includes 2015-10-25)
        rather than interval of the update, which causes a correct broadcast from 2015-10-25 to be deleted
        The solution for this is to only delete stale broadcasts from update intervals, not the broadcast intervals
         */
        DateTime item1Broadcast1Start = new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC);
        DateTime item1Broadcast1End = new DateTime(2015, 10, 26, 10, 0, 0, 0, DateTimeZones.UTC);
        DateTime item1Broadcast2Start = item1Broadcast1Start;
        DateTime item1Broadcast2End = new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC);

        DateTime item2BroadcastStart = item1Broadcast1End;
        DateTime item2BroadcastEnd = new DateTime(2015, 10, 26, 12, 0, 0, 0, DateTimeZones.UTC);

        DateTime item3BroadcastStart = item1Broadcast2End;
        DateTime item3BroadcastEnd = new DateTime(2015, 10, 26, 6, 0, 0, 0, DateTimeZones.UTC);

        DateTime item4BroadcastStart = item3BroadcastEnd;
        DateTime item4BroadcastEnd = item2BroadcastStart;

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        Item item3 = new Item(Id.valueOf(3), Publisher.METABROADCAST);
        Item item4 = new Item(Id.valueOf(4), Publisher.METABROADCAST);

        Broadcast item1Broadcast1 = new Broadcast(channel, item1Broadcast1Start, item1Broadcast1End)
                .withId("bid1");
        //it's the same broadcast that got shortened
        Broadcast item1Broadcast2 = new Broadcast(channel, item1Broadcast2Start, item1Broadcast2End)
                .withId("bid1");

        Broadcast item2Broadcast = new Broadcast(
                channel,
                item2BroadcastStart,
                item2BroadcastEnd
        ).withId("bid2");

        Broadcast item3Broadcast = new Broadcast(
                channel,
                item3BroadcastStart,
                item3BroadcastEnd
        ).withId("bid3");

        Broadcast item4Broadcast = new Broadcast(
                channel,
                item4BroadcastStart,
                item4BroadcastEnd
        ).withId("bid4");

        item1.addBroadcast(item1Broadcast1);
        getSubjectGenerator().getContentStore().writeContent(item1);

        item2.addBroadcast(item2Broadcast);
        getSubjectGenerator().getContentStore().writeContent(item2);

        ScheduleRef update1 = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(item1Broadcast1Start, item2BroadcastEnd)
        )
                .addEntry(item1.getId(), item1Broadcast1.toRef())
                .addEntry(item2.getId(), item2Broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        update1,
                        ImmutableSet.<BroadcastRef>of()
                ));

        item1.setBroadcasts(ImmutableSet.of(item1Broadcast2));
        getSubjectGenerator().getContentStore().writeContent(item1);

        item3.addBroadcast(item3Broadcast);
        getSubjectGenerator().getContentStore().writeContent(item3);

        ScheduleRef update2 = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(item1Broadcast2Start, item3BroadcastEnd)
        )
                .addEntry(item1.getId(), item1Broadcast2.toRef())
                .addEntry(item3.getId(), item3Broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        update2,
                        ImmutableSet.<BroadcastRef>of()
                ));

        item4.addBroadcast(item4Broadcast);
        getSubjectGenerator().getContentStore().writeContent(item4);

        ScheduleRef update3 = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(item4BroadcastStart, item2BroadcastEnd)
        )
                .addEntry(item4.getId(), item4Broadcast.toRef())
                .addEntry(item2.getId(), item2Broadcast.toRef())
                .build();

        getSubjectGenerator().getEquivalentScheduleStore()
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        update3,
                        ImmutableSet.<BroadcastRef>of()
                ));

        EquivalentSchedule resolved = get(
                getSubjectGenerator().getEquivalentScheduleStore().resolveSchedules(
                        ImmutableList.of(channel),
                        new Interval(item1Broadcast1Start, item2BroadcastEnd),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
        );

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        assertThat(schedule.getEntries().size(), is(4));
        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(0).getItems().getResources()
                ),
                is(item1)
        );

        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(1).getItems().getResources()
                ),
                is(item3)
        );

        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(2).getItems().getResources()
                ),
                is(item4)
        );

        assertThat(
                Iterables.getOnlyElement(
                        schedule.getEntries().get(3).getItems().getResources()
                ),
                is(item2)
        );
    }

    private <T> T get(ListenableFuture<T> future) throws Exception {
        return Futures.get(future, 10, TimeUnit.SECONDS, Exception.class);
    }

}
