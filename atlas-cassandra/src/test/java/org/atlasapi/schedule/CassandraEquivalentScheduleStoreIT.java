package org.atlasapi.schedule;

import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identifiables;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.TestCassandraPersistenceModule;

import com.metabroadcast.common.time.DateTimeZones;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

public class CassandraEquivalentScheduleStoreIT {

    private static TestCassandraPersistenceModule module;
    
    private EquivalenceGraphStore equivalenceGraphStore;
    private ContentStore contentStore;
    private EquivalentScheduleStore equivalentScheduleStore;

    @BeforeClass
    public static void init() throws Exception {
        module = new TestCassandraPersistenceModule();
        module.startAsync().awaitRunning(1, TimeUnit.MINUTES);
    }

    @Before
    public void setUp() throws Exception {
        module.reset();

        equivalenceGraphStore = module.contentEquivalenceGraphStore();
        contentStore = module.contentStore();
        equivalentScheduleStore = module.equivalentScheduleStore();
    }

    @AfterClass
    public static void cleanUp() throws Exception {
        module.reset();
    }

    @Test
    public void testWritingNewSchedule() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        contentStore.writeContent(item);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
                .resolveSchedules(
                        ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item));
    }

    @Test
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();
        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of(
                                broadcast1.toRef()
                        )
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testWritingRepeatedItem() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 18, 0, 0, 0, DateTimeZones.UTC)
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

        contentStore.writeContent(item);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast1.toRef())
                .addEntry(item.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testWritingNewScheduleWithEquivalentItems() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        equivalenceGraphStore
                .updateEquivalences(item.toRef(),
                        ImmutableSet.of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testResolvingScheduleFiltersItemsAccordingToSelectedSources() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        equivalenceGraphStore.updateEquivalences(
                item.toRef(),
                ImmutableSet.of(equiv.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testResolvingScheduleSetsEquivalentToOnItems() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast = new Broadcast(channel, interval).withId("sid");
        item.addBroadcast(broadcast);

        Item equiv = new Item(Id.valueOf(2), Publisher.BBC);
        equiv.addBroadcast(broadcast);
        equiv.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));

        equivalenceGraphStore.updateEquivalences(
                item.toRef(),
                ImmutableSet.of(equiv.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        ImmutableSet<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems()
                .getResources();

        assertThat(broadcastItems.size(), is(2));

        for (Item broadcastItem : broadcastItems) {
            EquivalenceRef equivalentTo = Iterables.getOnlyElement(broadcastItem.getEquivalentTo());

            if (broadcastItem.getId().equals(item.getId())) {
                assertThat(equivalentTo.getId(), is(equiv.getId()));
                assertThat(equivalentTo.getSource(), is(equiv.getSource()));
            } else {
                assertThat(equivalentTo.getId(), is(item.getId()));
                assertThat(equivalentTo.getSource(), is(item.getSource()));
            }
        }
    }

    @Test
    public void testWritingNewScheduleWithEquivalentItemsChoosesBestEquivalent() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
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

        equivalenceGraphStore
                .updateEquivalences(item.toRef(),
                        ImmutableSet.of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );
        equivalenceGraphStore.updateEquivalences(
                otherEquiv.toRef(),
                ImmutableSet.of(item.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        contentStore.writeContent(otherEquiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testUpdatingEquivalences() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
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

        equivalenceGraphStore.updateEquivalences(
                otherEquiv.toRef(),
                ImmutableSet.of(
                        item.toRef()),
                ImmutableSet.of(Publisher.METABROADCAST)
        );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);
        contentStore.writeContent(otherEquiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(new ScheduleUpdate(
                Publisher.METABROADCAST,
                scheduleRef,
                ImmutableSet.of()
        ));

        EquivalenceGraphUpdate equivUpdate = equivalenceGraphStore
                .updateEquivalences(
                        item.toRef(),
                        ImmutableSet.of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
                .get();

        equivalentScheduleStore
                .updateEquivalences(equivUpdate.getAllGraphs());

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testResolvingAnEmptySchedule() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);
        Interval interval = new Interval(
                new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC)
        );

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
                .resolveSchedules(ImmutableList.of(channel),
                        interval,
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
                ));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());
        assertThat(schedule.getEntries().size(), is(0));
        assertThat(schedule.getChannel(), is(channel));

    }

    @Test
    public void testResolvingScheduleFiltersByRequestedInterval() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);

        DateTime one = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 3, 21, 18, 0, 0, 0, DateTimeZones.UTC);
        DateTime four = new DateTime(2014, 3, 21, 19, 0, 0, 0, DateTimeZones.UTC);

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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);
        contentStore.writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef
                .forChannel(channel.getId(), new Interval(one, four))
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = getEquivalentSchedule(channel, one.plusMinutes(30), one.plusMinutes(30));

        EquivalentChannelSchedule schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        Equivalent<Item> broadcastItems = Iterables.getOnlyElement(schedule.getEntries())
                .getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item1));

        resolved
                = getEquivalentSchedule(channel, one.plusMinutes(30), three);

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
                = getEquivalentSchedule(channel, one.plusMinutes(30), three.plusMinutes(30));

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
                = getEquivalentSchedule(channel, three, three);

        schedule = Iterables.getOnlyElement(resolved.channelSchedules());

        broadcastItems = Iterables.getOnlyElement(schedule.getEntries()).getItems();
        assertThat(Iterables.getOnlyElement(broadcastItems.getResources()), is(item3));

    }

    @Test
    public void testResolvingScheduleFromMultipleChannels() throws Exception {

        Channel channel1 = Channel.builder(Publisher.BBC).build();
        channel1.setId(1L);

        Channel channel2 = Channel.builder(Publisher.BBC).build();
        channel2.setId(2L);

        DateTime one = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 3, 21, 18, 0, 0, 0, DateTimeZones.UTC);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item1.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast1 = new Broadcast(channel1, one, two).withId("12");
        item1.addBroadcast(broadcast1);

        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);
        item2.setThisOrChildLastUpdated(DateTime.now(DateTimeZones.UTC));
        Broadcast broadcast2 = new Broadcast(channel2, two, three).withId("23");
        item2.addBroadcast(broadcast2);

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);

        ScheduleRef scheduleRef = ScheduleRef
                .forChannel(channel1.getId(), new Interval(one, three))
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);
        contentStore.writeContent(item3);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(start, startPlus3h))
                .addEntry(item1.getId(), broadcast1Channel1.toRef())
                .addEntry(item2.getId(), broadcast2Channel1.toRef())
                .addEntry(item3.getId(), broadcast3Channel1.toRef())
                .addEntry(item2.getId(), broadcast4Channel1.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.of()
                ));

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel2.getId(), new Interval(start, startPlus26h))
                .addEntry(item1.getId(), broadcast1Channel2.toRef())
                .addEntry(item2.getId(), broadcast2Channel2.toRef())
                .addEntry(item3.getId(), broadcast3Channel2.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(
                equivalentScheduleStore.resolveSchedules(
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

    @Test
    public void testWritingScheduleRemovesExtraneousBroadcasts() throws Exception {

        Channel channel = Channel.builder(Publisher.BBC).build();
        channel.setId(1L);

        DateTime one = new DateTime(2014, 3, 21, 16, 0, 0, 0, DateTimeZones.UTC);
        DateTime two = new DateTime(2014, 3, 21, 17, 0, 0, 0, DateTimeZones.UTC);
        DateTime three = new DateTime(2014, 3, 21, 18, 0, 0, 0, DateTimeZones.UTC);
        DateTime four = new DateTime(2014, 3, 21, 19, 0, 0, 0, DateTimeZones.UTC);

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

        equivalenceGraphStore
                .updateEquivalences(item.toRef(),
                        ImmutableSet.of(equiv.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                );

        contentStore.writeContent(item);
        contentStore.writeContent(equiv);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), new Interval(one, two))
                .addEntry(item.getId(), broadcast1.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(new ScheduleUpdate(
                Publisher.METABROADCAST,
                scheduleRef,
                ImmutableSet.of()
        ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(now, nowPlus2h))
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(
                new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.of()
                )
        );

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(nowMinus26h, nowMinus25h))
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(
                new ScheduleUpdate(
                        Publisher.BBC_KIWI,
                        scheduleRef2,
                        ImmutableSet.of()
                )
        );

        EquivalentChannelSchedule currentMbSched = get(
                equivalentScheduleStore.resolveSchedules(
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
                equivalentScheduleStore.resolveSchedules(
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

        EquivalenceGraphUpdate equivUpdate = equivalenceGraphStore
                .updateEquivalences(
                        item1.toRef(),
                        ImmutableSet.of(item2.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                ).get();
        EquivalenceGraphUpdate equivUpdate2 = equivalenceGraphStore
                .updateEquivalences(
                        item2.toRef(),
                        ImmutableSet.of(item1.toRef()),
                        ImmutableSet.of(Publisher.METABROADCAST)
                ).get();

        equivalentScheduleStore
                .updateEquivalences(equivUpdate.getAllGraphs());

        equivalentScheduleStore
                .updateEquivalences(equivUpdate2.getAllGraphs());

        currentMbSched = get(
                equivalentScheduleStore.resolveSchedules(
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
                equivalentScheduleStore.resolveSchedules(
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

        contentStore.writeContent(item1);

        equivalentScheduleStore
                .updateContent(ImmutableSet.of(item1, item2));

        currentMbSched = get(
                equivalentScheduleStore.resolveSchedules(
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
                equivalentScheduleStore.resolveSchedules(
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

    @Test
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);

        ScheduleRef scheduleRef1 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(now, nowPlus2h))
                .addEntry(item1.getId(), broadcast1.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(
                new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        scheduleRef1,
                        ImmutableSet.of()
                )
        );

        ScheduleRef scheduleRef2 = ScheduleRef
                .forChannel(channel1.getId(), new Interval(nowMinus26h, nowMinus25h))
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore.updateSchedule(
                new ScheduleUpdate(
                        Publisher.BBC_KIWI,
                        scheduleRef2,
                        ImmutableSet.of()
                )
        );
        equivalentScheduleStore
                .updateContent(ImmutableSet.of(item1, item2));

        EquivalentChannelSchedule currentMbSched = get(
                equivalentScheduleStore.resolveSchedules(
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

    @Test
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

        contentStore.writeContent(beforeItem);
        contentStore.writeContent(afterItem);

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

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRefBefore,
                        ImmutableSet.of()
                ));
        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRefAfter,
                        ImmutableSet.of()
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);
        contentStore.writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        Item item4 = new Item(Id.valueOf(4), Publisher.METABROADCAST);
        Broadcast broadcast4 = new Broadcast(channel, midday, end).withId("sid4");
        item4.addBroadcast(broadcast4);

        contentStore.writeContent(item4);
        ScheduleRef scheduleRef2 = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item4.getId(), broadcast4.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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
                getEquivalentSchedule(channel, beforeStart, start.minusSeconds(1)).channelSchedules()
        );

        assertThat(beforeSchedule.getEntries().size(), is(1));

        EquivalentChannelSchedule afterSchedule
                = Iterables.getOnlyElement(
                getEquivalentSchedule(channel, end.plusSeconds(1), afterEnd).channelSchedules()
        );

        assertThat(afterSchedule.getEntries().size(), is(1));

    }

    @Test
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

        contentStore.writeContent(item1);
        contentStore.writeContent(item2);
        contentStore.writeContent(item3);

        ScheduleRef scheduleRef = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .addEntry(item3.getId(), broadcast3.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef,
                        ImmutableSet.of()
                ));

        Item item4 = new Item(Id.valueOf(4), Publisher.METABROADCAST);
        Broadcast broadcast4 = new Broadcast(channel, midday, end).withId("sid3");
        item4.addBroadcast(broadcast4);

        contentStore.writeContent(item4);
        ScheduleRef scheduleRef2 = ScheduleRef.forChannel(channel.getId(), interval)
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item4.getId(), broadcast4.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(Publisher.METABROADCAST,
                        scheduleRef2,
                        ImmutableSet.of()
                ));

        EquivalentSchedule resolved
                = get(equivalentScheduleStore
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

    @Test
    public void testDoesNotRemoveStaleBroadcastsFromOutsideInterval() throws Exception {

        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("bid1");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        ScheduleRef broadcastUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        broadcastUpdate,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        ScheduleRef updateThatShouldNotDeleteBroadcast = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 17, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 18, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateThatShouldNotDeleteBroadcast,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        ScheduleRef updateThatShouldDeleteBroadcast = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateThatShouldDeleteBroadcast,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void testBroadcastSpanningMultipleRowsGettingShortenedToOneRowIsRemovedFromOtherRows()
            throws Exception {

        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 26, 10, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("bid1");

        Broadcast updatedBroadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("bid1");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        ScheduleRef updateA = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 12, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateA,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 10, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        item.setBroadcasts(ImmutableSet.of(updatedBroadcast));
        contentStore.writeContent(item);

        ScheduleRef updateB = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 6, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), updatedBroadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateB,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 30, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        assertThat(
                getScheduleEntries(channel,
                        new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 10, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void testBroadcastsInAdjacentDaysWithSameIdsDoNotPreventRemovalOfStaleBroadcasts()
            throws Exception {

        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        Item item1 = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Item item2 = new Item(Id.valueOf(2), Publisher.METABROADCAST);

        Broadcast broadcast1 = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("bid1");

        Broadcast broadcast2 = new Broadcast(
                channel,
                new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 26, 3, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("bid1");

        item1.addBroadcast(broadcast1);
        contentStore.writeContent(item1);

        item2.addBroadcast(broadcast2);
        contentStore.writeContent(item2);

        ScheduleRef update1 = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 3, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item1.getId(), broadcast1.toRef())
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        update1,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 3, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        ScheduleRef update2 = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 3, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item2.getId(), broadcast2.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        update2,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 26, 2, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 3, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );
    }

    @Test
    public void testWritingBroadcastThatHasChangedSourceIdWritesTheCorrectClusteringKey()
            throws Exception {

        Channel channel = Channel.builder(Publisher.METABROADCAST).build();
        channel.setId(1L);

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcastWithOriginalId = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        Broadcast broadcastWithUpdatedId = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("changedId");

        item.addBroadcast(broadcastWithUpdatedId);
        contentStore.writeContent(item);

        ScheduleRef updateThatAddsBroadcast = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcastWithOriginalId.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateThatAddsBroadcast,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
                        .size(),
                is(1)
        );

        // If the broadcast has been written with the wrong source ID we will fail to remove it
        // because we will be trying to remove the wrong key
        ScheduleRef updateThatRemovesBroadcast = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        updateThatRemovesBroadcast,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void updatingTheScheduleUsesTheCurrentBroadcastIdsWhenDeterminingStales()
            throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        // The existing broadcast has the same ID as the broadcast in the update. If we use
        // the broadcast ID when computing stale broadcasts instead of using the ID of the broadcast
        // we resolved from the content store (which is the most up-to-date one) we will think this
        // ID is not stale and therefore won't remove this broadcast. This test asserts that this
        // doesn't happen
        Broadcast existingBroadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 18, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("firstId");

        Broadcast broadcastInUpdate = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("firstId");

        Broadcast broadcastInStore = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("secondId");

        Item existingItem = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        Item item = new Item(Id.valueOf(2), Publisher.METABROADCAST);

        existingItem.setBroadcasts(ImmutableSet.of(existingBroadcast));
        item.setBroadcasts(ImmutableSet.of(broadcastInStore));

        contentStore.writeContent(existingItem);
        contentStore.writeContent(item);

        // This update adds the existing broadcast
        ScheduleRef firstUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 18, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(existingItem.getId(), existingBroadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        firstUpdate,
                        ImmutableSet.of()
                ));

        // This update should add the new broadcast and remove the existing one
        ScheduleRef secondUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 18, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcastInUpdate.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        secondUpdate,
                        ImmutableSet.of()
                ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 18, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void updatingEquivalencesDoesNotChangeSchedule() throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        Broadcast updatedBroadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 21, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        ScheduleRef initialUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        initialUpdate,
                        ImmutableSet.of()
                ));

        item.setBroadcasts(ImmutableSet.of(updatedBroadcast));
        equivalentScheduleStore.updateEquivalences(ImmutableSet.of(
                EquivalenceGraph.valueOf(item.toRef())
        ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 21, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void updatingEquivalencesOnMissingRowDoesNotChangeSchedule() throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        equivalentScheduleStore.updateEquivalences(ImmutableSet.of(
                EquivalenceGraph.valueOf(item.toRef())
        ));

        assertThat(
                getScheduleEntries(
                        channel,
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
                        .isEmpty(),
                is(true)
        );
    }

    @Test
    public void updatingScheduleDeletesRowsWithNullBroadcasts() throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        equivalentScheduleStore.updateEquivalences(ImmutableSet.of(
                EquivalenceGraph.valueOf(item.toRef())
        ));

        ScheduleRef initialUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 0, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 26, 0, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        initialUpdate,
                        ImmutableSet.of()
                ));

        boolean scheduleHasRows = module.getSession()
                .execute("SELECT * FROM equivalent_schedule")
                .iterator()
                .hasNext();

        assertThat(scheduleHasRows, is(false));
    }

    @Test
    public void updatingEquivalencesUpdatesContent() throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setTitle("oldTitle");
        item.setThisOrChildLastUpdated(DateTime.now());

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        ScheduleRef initialUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        initialUpdate,
                        ImmutableSet.of()
                ));

        item.setTitle("updatedTitle");
        contentStore.writeContent(item);

        EquivalenceGraph graph = EquivalenceGraph.valueOf(item.toRef());

        equivalentScheduleStore.updateEquivalences(ImmutableSet.of(graph));

        ImmutableList<EquivalentScheduleEntry> scheduleEntries = getScheduleEntries(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        );

        EquivalentScheduleEntry entry = Iterables.getOnlyElement(scheduleEntries);

        Item actualItem = Iterables.getOnlyElement(entry.getItems().getResources());
        assertThat(actualItem.getTitle(), is(item.getTitle()));
    }

    @Test
    public void updatingEquivalencesUpdatesGraph() throws Exception {
        Channel channel = Channel.builder(Publisher.METABROADCAST).withId(1L).build();

        Item item = new Item(Id.valueOf(1), Publisher.METABROADCAST);
        item.setThisOrChildLastUpdated(DateTime.now());

        Broadcast broadcast = new Broadcast(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        )
                .withId("id");

        item.addBroadcast(broadcast);
        contentStore.writeContent(item);

        ScheduleRef initialUpdate = ScheduleRef.forChannel(
                channel.getId(),
                new Interval(
                        new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                        new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
                )
        )
                .addEntry(item.getId(), broadcast.toRef())
                .build();

        equivalentScheduleStore
                .updateSchedule(new ScheduleUpdate(
                        Publisher.METABROADCAST,
                        initialUpdate,
                        ImmutableSet.of()
                ));

        Item equivItem = new Item(Id.valueOf(2L), Publisher.BBC);
        equivItem.setThisOrChildLastUpdated(DateTime.now());
        contentStore.writeContent(equivItem);

        EquivalenceGraph graph = EquivalenceGraph.valueOf(ImmutableSet.of(
                EquivalenceGraph.Adjacents
                        .valueOf(equivItem.toRef()),
                EquivalenceGraph.Adjacents
                        .valueOf(item.toRef())
                        .copyWithOutgoing(equivItem.toRef())
        ));

        equivalentScheduleStore.updateEquivalences(ImmutableSet.of(graph));

        ImmutableList<EquivalentScheduleEntry> scheduleEntries = getScheduleEntries(
                channel,
                new DateTime(2015, 10, 25, 19, 0, 0, 0, DateTimeZones.UTC),
                new DateTime(2015, 10, 25, 20, 0, 0, 0, DateTimeZones.UTC)
        );

        EquivalentScheduleEntry entry = Iterables.getOnlyElement(scheduleEntries);
        assertThat(entry.getItems().getGraph(), is(graph));
    }

    private ImmutableList<EquivalentScheduleEntry> getScheduleEntries(
            Channel channel, DateTime start, DateTime end
    ) throws Exception {
        return Iterables.getOnlyElement(
                getEquivalentSchedule(channel, start, end)
                        .channelSchedules()
        )
                .getEntries();
    }

    private EquivalentSchedule getEquivalentSchedule(
            Channel channel, DateTime start, DateTime end
    ) throws Exception {
        return get(
                equivalentScheduleStore.resolveSchedules(
                        ImmutableList.of(channel),
                        new Interval(start, end),
                        Publisher.METABROADCAST,
                        ImmutableSet.of(Publisher.METABROADCAST)
                )
        );
    }

    private <T> T get(ListenableFuture<T> future) throws Exception {
        return Futures.get(future, 10, TimeUnit.SECONDS, Exception.class);
    }
}
