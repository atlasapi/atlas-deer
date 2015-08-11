package org.atlasapi.schedule;

import static org.hamcrest.Matchers.any;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isA;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;
import static org.mockito.Matchers.argThat;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.times;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import java.util.List;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableSet;
import com.netflix.astyanax.serializers.AsciiSerializer;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.AstyanaxCassandraContentStore;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentHasher;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.CassandraHelper;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.ScheduleEntry;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.ArgumentCaptor;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.ids.SequenceGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.TimeMachine;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.connectionpool.exceptions.BadRequestException;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

@RunWith(MockitoJUnitRunner.class)
public class CassandraScheduleStoreIT {

    private static final String SCHEDULE_CF_NAME = "schedule";
    private static final String CONTENT_CF_NAME = "content";
    private static final String CONTENT_ALIASES_CF_NAME = "content_aliases";

    private static final AstyanaxContext<Keyspace> context =
            CassandraHelper.testCassandraContext();

    //hasher is mock till we have a non-Mongo based one.
    @Mock private ContentHasher hasher;
    @Mock private MessageSender<ResourceUpdatedMessage> contentUpdateSender; 
    @Mock private MessageSender<ScheduleUpdateMessage> scheduleUpdateSender; 
    
    private final Clock clock = new TimeMachine();
    
    private AstyanaxCassandraContentStore contentStore;
    private CassandraScheduleStore store;
    
    private final Publisher source = Publisher.METABROADCAST;
    private final Channel channel = Channel.builder(Publisher.BBC).build();

    @BeforeClass
    public static void setup() throws ConnectionException {
        context.start();
        tearDown();
        CassandraHelper.createKeyspace(context);
        CassandraHelper.createColumnFamily(context,
                SCHEDULE_CF_NAME,
                StringSerializer.get(),
                StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_CF_NAME, LongSerializer.get(), StringSerializer.get());
        CassandraHelper.createColumnFamily(context, CONTENT_ALIASES_CF_NAME, StringSerializer.get(), StringSerializer.get(), LongSerializer.get());
    }

    @AfterClass
    public static void tearDown() throws ConnectionException {
        try {
            context.getClient().dropKeyspace();
        } catch (BadRequestException ire) { }
    }
    
    @Before
    public void setUp() {
        channel.setCanonicalUri("channel");
        channel.setId(1234L);
        contentStore = AstyanaxCassandraContentStore
                .builder(context, CONTENT_CF_NAME, hasher, contentUpdateSender, new SequenceGenerator())
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
        store = CassandraScheduleStore
                .builder(context, SCHEDULE_CF_NAME, contentStore, scheduleUpdateSender)
                .withReadConsistency(ConsistencyLevel.CL_ONE)
                .withWriteConsistency(ConsistencyLevel.CL_ONE)
                .withClock(clock)
                .build();
    }

    @After
    public void clearCf() throws ConnectionException {
        context.getClient().truncateColumnFamily(SCHEDULE_CF_NAME);
        context.getClient().truncateColumnFamily(CONTENT_CF_NAME);
        context.getClient().truncateColumnFamily(CONTENT_ALIASES_CF_NAME);
    }

    @Test
    public void testWritingAndResolvingANewSchedule() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,23,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(itemAndBroadcast(null, "one", source, channel, start, middle)), 
            ScheduleHierarchy.itemOnly(itemAndBroadcast(null, "two", source, channel, middle, end))
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content, Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        
        assertThat(results.size(), is(2));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,30,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(1L));
    }

    @Test
    public void testReWritingASchedule() throws Exception {

        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,23,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);

        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);

        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1),
            ScheduleHierarchy.itemOnly(episode2)
        );

        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content, Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));

        DateTime newMiddle = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);

        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");

        episode1 = itemAndBroadcast(null, "one", source, channel, start, newMiddle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, newMiddle, end);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1),
            ScheduleHierarchy.itemOnly(episode3)
        );

        results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        assertTrue(Iterables.all(results, WriteResult.<Content,Content>writtenFilter()));

        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC),
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC)
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());

        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));

        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(two.getBroadcasts()).isActivelyPublished());
    }


    @Test
    public void testSendAllStaleBroadcastsSchedule() throws Exception {

        DateTime start = new DateTime(2013,5,31,14,0,0,0,DateTimeZones.UTC);
        DateTime middle = new DateTime(2013,5,31,23,0,0,0,DateTimeZones.UTC);
        DateTime end = new DateTime(2013,6,1,14,0,0,0,DateTimeZones.UTC);

        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);

        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.of(
                ScheduleHierarchy.itemOnly(episode1),
                ScheduleHierarchy.itemOnly(episode2)
        );

        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content, Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));

        DateTime newMiddle = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.UTC);

        //items 1 and 2 change
        when(hasher.hash(argThat(isA(Content.class))))
                .thenReturn("differentOne")
                .thenReturn("oneDifferent")
                .thenReturn("differentTwo")
                .thenReturn("twoDifferent");

        episode1 = itemAndBroadcast(null, "one", source, channel, start, newMiddle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, newMiddle, end);
        hiers = ImmutableList.of(
                ScheduleHierarchy.itemOnly(episode1),
                ScheduleHierarchy.itemOnly(episode3)
        );

        results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        assertTrue(Iterables.all(results, WriteResult.<Content,Content>writtenFilter()));


        ItemAndBroadcast episode4 = itemAndBroadcast(null, "four", source, channel, newMiddle, end);
        hiers = ImmutableList.<ScheduleHierarchy>of(
                ScheduleHierarchy.itemOnly(episode1),
                ScheduleHierarchy.itemOnly(episode4)
        );
        store.writeSchedule(hiers, channel, writtenInterval);

        ArgumentCaptor<ScheduleUpdateMessage> captor = ArgumentCaptor.forClass(ScheduleUpdateMessage.class);
        verify(scheduleUpdateSender, times(3)).sendMessage(captor.capture());

        assertThat(
                captor.getAllValues().get(2).getScheduleUpdate().getStaleBroadcasts(),
                containsInAnyOrder(
                        new BroadcastRef("three", Id.valueOf(1234), new Interval(newMiddle, end)),
                        new BroadcastRef("two", Id.valueOf(1234), new Interval(middle, end))
                )
        );

    }


    @Test
    public void testBroadcastMovedToAnotherBlockAndBack() throws Exception {

        DateTime start = new DateTime(2015,5,30,22,30,0,0,DateTimeZones.UTC);
        DateTime end = new DateTime(2015,5,30,23,0,0,0,DateTimeZones.UTC);

        DateTime newStart = new DateTime(2015,6,1,0,00,0,0,DateTimeZones.UTC);
        DateTime newEnd = new DateTime(2015,6,1,0,30,0,0,DateTimeZones.UTC);

        ItemAndBroadcast episode1Slot1 = itemAndBroadcast(1, "one", source, channel, start, end);
        ItemAndBroadcast episode1Slot2 = itemAndBroadcast(1, "one", source, channel, newStart, newEnd);
        ItemAndBroadcast episode2Slot1 = itemAndBroadcast(2, "two", source, channel, start, end);

        ImmutableList<ScheduleHierarchy> hiers1 = ImmutableList.of(
                ScheduleHierarchy.itemOnly(episode1Slot1)
        );
        ImmutableList<ScheduleHierarchy> hiers2 = ImmutableList.of(
                ScheduleHierarchy.itemOnly(episode1Slot2)
        );

        ImmutableList<ScheduleHierarchy> hiers3 = ImmutableList.of(
                ScheduleHierarchy.itemOnly(episode2Slot1)
        );

        Interval writtenInterval = new Interval(start, end);
        Interval newWrittenInterval = new Interval(newStart, newEnd);
        when(hasher.hash(argThat(isA(Content.class))))
                .thenReturn("1")
                .thenReturn("2")
                .thenReturn("3")
                .thenReturn("4")
                .thenReturn("5")
                .thenReturn("6")
                .thenReturn("7")
                .thenReturn("9")
                .thenReturn("10")
                .thenReturn("11")
                .thenReturn("12");

        //write a broadcast for a block
        store.writeSchedule(hiers1, channel, writtenInterval);
        //move this broadcast to the next day
        store.writeSchedule(hiers2, channel, newWrittenInterval);
        //overwrite the broadcast
        store.writeSchedule(hiers3, channel, writtenInterval);
        //write it again
        store.writeSchedule(hiers1, channel, writtenInterval);

        Item content = (Item)Iterables.getOnlyElement(Futures.get(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))), Exception.class).getResources());
        Schedule schedule = Futures.get(store.resolve(ImmutableSet.of(channel), writtenInterval, source), Exception.class);

        ItemAndBroadcast itemAndBroadcast = Iterables.getOnlyElement(Iterables.getOnlyElement(schedule.channelSchedules()).getEntries());

        assertThat(itemAndBroadcast.getBroadcast().getSourceId(), is("one"));
        assertThat(itemAndBroadcast.getItem().getId(), is(Id.valueOf(1)));
        assertThat(Iterables.getOnlyElement(content.getBroadcasts()).isActivelyPublished(), is(true));
    }
    
    @Test
    public void testReWritingScheduleUpdatesOverlappingBroadcastsInDifferentSegment() throws Exception {

        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode2)
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content, Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));

        DateTime newEnd = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);
        
        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");
        
        episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, middle, newEnd);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode3)
        );
        
        results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        assertTrue(Iterables.all(results, WriteResult.<Content,Content>writtenFilter()));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(2L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));
        
        requestedInterval = new Interval(
            new DateTime(2013,05,31,23,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,06,01,22,40,0,0,DateTimeZones.UTC) 
        );
        schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(0));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(two.getBroadcasts()).isActivelyPublished());
    }
    
    @Test
    public void testWritingAnItemWithManyBroadcastsOnlyHasTheRelevantBroadcastInTheResolvedSchedule() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        Item item1 = item(1, source, "item");
        Broadcast broadcast1 = broadcast("one", channel, start, middle);
        Broadcast broadcast2 = broadcast("two", channel, middle, end);
        Broadcast broadcast3 = broadcast("three", channel, new DateTime(DateTimeZones.LONDON), new DateTime(DateTimeZones.LONDON));
        
        item1.addBroadcast(broadcast1);
        item1.addBroadcast(broadcast2);
        item1.addBroadcast(broadcast3);
        
        ItemAndBroadcast iab1 = new ItemAndBroadcast(item1, broadcast1);
        ItemAndBroadcast iab2 = new ItemAndBroadcast(item1.copy(), broadcast2);
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(iab1), 
            ScheduleHierarchy.itemOnly(iab2)
        );
        Interval writtenInterval = new Interval(start, end);
        
        when(hasher.hash(argThat(any(Content.class)))).thenReturn("one", "two", "three");
        
        List<WriteResult<? extends Content, Content>> results
            = store.writeSchedule(hiers, channel, writtenInterval);

        verify(hasher, never()).hash(argThat(is(any(Content.class))));
        assertThat(results.size(), is(1));

        Schedule schedule = future(store.resolve(ImmutableList.of(channel), writtenInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        assertThat(channelSchedule.getEntries().size(), is(2));
        ItemAndBroadcast fst = channelSchedule.getEntries().get(0);
        ItemAndBroadcast snd = channelSchedule.getEntries().get(1);
        
        Item resolved1 = fst.getItem();
        assertThat(Iterables.getOnlyElement(resolved1.getBroadcasts()), is(fst.getBroadcast()));

        Item resolved2 = snd.getItem();
        assertThat(Iterables.getOnlyElement(resolved2.getBroadcasts()), is(snd.getBroadcast()));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(1))));
        Item item = (Item) resolved.toMap().get(Id.valueOf(1)).get();
        assertThat(item.getBroadcasts().size(), is(3));
        assertThat(item.getBroadcasts(), hasItems(broadcast1, broadcast2, broadcast3));
    }

    @Test
    @Ignore
    // TODO Known issue: if the update interval doesn't cover a now stale broadcast
    // in a subsequent segment then that broadcast won't be updated in the
    // subsequent segment, it will be updated in the store though.
    public void testReWritingScheduleUpdatesOverlappingBroadcastsInDifferentSegmentEvenWhenUpdateIntervalDoesntCoverSegment() throws Exception {
        
        DateTime start = new DateTime(2013,05,31,14,0,0,0,DateTimeZones.LONDON);
        DateTime middle = new DateTime(2013,05,31,20,0,0,0,DateTimeZones.LONDON);
        DateTime end = new DateTime(2013,06,01,14,0,0,0,DateTimeZones.LONDON);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode2 = itemAndBroadcast(null, "two", source, channel, middle, end);
        
        ImmutableList<ScheduleHierarchy> hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode2)
        );
        
        Interval writtenInterval = new Interval(start, end);
        List<WriteResult<? extends Content, Content>> results = store.writeSchedule(hiers, channel, writtenInterval);
        assertThat(results.size(), is(2));
        
        DateTime newEnd = new DateTime(2013,05,31,23,30,0,0,DateTimeZones.LONDON);
        
        //items 1 and 2 change 
        when(hasher.hash(argThat(isA(Content.class))))
            .thenReturn("differentOne")
            .thenReturn("oneDifferent")
            .thenReturn("differentTwo")
            .thenReturn("twoDifferent");
        
        episode1 = itemAndBroadcast(null, "one", source, channel, start, middle);
        ItemAndBroadcast episode3 = itemAndBroadcast(null, "three", source, channel, middle, newEnd);
        hiers = ImmutableList.<ScheduleHierarchy>of(
            ScheduleHierarchy.itemOnly(episode1), 
            ScheduleHierarchy.itemOnly(episode3)
        );
        
        results = store.writeSchedule(hiers, channel, new Interval(start, newEnd));
        assertThat(results.size(), is(2));
        assertTrue(Iterables.all(results, WriteResult.<Content,Content>writtenFilter()));
        
        Interval requestedInterval = new Interval(
            new DateTime(2013,05,31,10,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,05,31,22,40,0,0,DateTimeZones.UTC) 
        );
        Schedule schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        ChannelSchedule channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(2));
        assertThat(channelSchedule.getEntries().get(0).getItem().getId().longValue(), is(1L));
        assertThat(channelSchedule.getEntries().get(1).getItem().getId().longValue(), is(3L));
        
        requestedInterval = new Interval(
            new DateTime(2013,05,31,23,0,0,0,DateTimeZones.UTC), 
            new DateTime(2013,06,01,22,40,0,0,DateTimeZones.UTC) 
        );
        schedule = future(store.resolve(ImmutableList.of(channel), requestedInterval, source));
        channelSchedule = Iterables.getOnlyElement(schedule.channelSchedules());
        
        assertThat(channelSchedule.getChannel(), is(channel));
        assertThat(channelSchedule.getInterval(), is(requestedInterval));
        assertThat(channelSchedule.getEntries().size(), is(0));
        
        Resolved<Content> resolved = future(contentStore.resolveIds(ImmutableList.of(Id.valueOf(2))));
        Item two = (Item) resolved.getResources().first().get();
        assertFalse(Iterables.getOnlyElement(two.getBroadcasts()).isActivelyPublished());
    }

    private <T> T future(ListenableFuture<T> resolve) throws Exception {
        return Futures.get(resolve, 1, TimeUnit.SECONDS, Exception.class);
    }

    private ItemAndBroadcast itemAndBroadcast(Integer id, String alias, Publisher source, Channel channel, DateTime start, DateTime end) {
        Item item = item(id, source, alias);
        Broadcast broadcast = broadcast(alias, channel, start, end);
        item.addBroadcast(broadcast);
        return new ItemAndBroadcast(item, broadcast);
    }

    private Broadcast broadcast(String broadacstId, Channel channel, DateTime start, DateTime end) {
        Broadcast b = new Broadcast(channel, start, end);
        b.withId(broadacstId);
        return b;
    }

    private Item item(Integer id, Publisher source, String alias) {
        Item item = new Item();
        if (id != null) {
            item.setId(id);
        }
        item.addAlias(new Alias("uri", alias));
        item.setPublisher(source);
        return item;
    }
    
}
