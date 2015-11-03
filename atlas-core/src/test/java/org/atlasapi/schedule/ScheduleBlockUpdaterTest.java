package org.atlasapi.schedule;

import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.Matchers.containsInAnyOrder;
import static org.hamcrest.Matchers.hasItem;
import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.util.List;

import com.google.common.collect.ImmutableSet;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Episode;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleBlockUpdaterTest {

    private final ScheduleBlockUpdater updater = new ScheduleBlockUpdater(); 

    private final Publisher source = Publisher.METABROADCAST;
    private final Channel channel = Channel.builder(Publisher.BBC).build();

    @Before
    public void setUp() {
        channel.setCanonicalUri("channel");
        channel.setId(1L);
    }
    
    @Test
    public void testNoStaleEntriesWhenPreviousScheduleIsEmpty() throws Exception {
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
                new ChannelSchedule(channel, interval, ImmutableList.<ItemAndBroadcast>of()));
        
        ItemAndBroadcast updateEntry = itemAndBroadcast(1, source, channel, "one", interval);
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), ImmutableList.of(updateEntry), channel, interval);
        
        assertTrue(scheduleUpdate.getStaleEntries().isEmpty());
        assertThat(Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks().get(0).getEntries()), is(updateEntry));
    }
    
    @Test
    public void testNoStaleEntriesWhenPreviousScheduleMatchesCurrent() throws Exception {
        
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        ItemAndBroadcast updateEntry = itemAndBroadcast(1, source, channel, "one", interval);
        
        List<ItemAndBroadcast> entries = ImmutableList.of(updateEntry);
        List<ChannelSchedule> currentSchedule = ImmutableList.of(new ChannelSchedule(channel, interval, entries));
        
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule,ImmutableList.<ChannelSchedule>of(), ImmutableList.of(updateEntry), channel, interval);

        assertTrue(scheduleUpdate.getStaleEntries().isEmpty());
        
        ChannelSchedule updatedBlock = Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks());
        assertThat(Iterables.getOnlyElement(updatedBlock.getEntries()), is(updateEntry));
    }

    @Test
    public void testStaleEntryReplacedWhenBroadcastIdChanges() throws Exception {
        Interval interval = new Interval(0, 100, DateTimeZones.UTC);
        
        ItemAndBroadcast currentEntry = itemAndBroadcast(1, METABROADCAST, channel, "one", interval);
        
        ItemAndBroadcast updatedEntry = new ItemAndBroadcast(
            currentEntry.getItem().copy(), 
            currentEntry.getBroadcast().copy().withId("different")
        );
        
        List<ItemAndBroadcast> updatedSchedule = ImmutableList.of(updatedEntry);
        List<ChannelSchedule> currentSchedule = ImmutableList.of(new ChannelSchedule(channel, interval, ImmutableList.of(currentEntry)));
        
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), updatedSchedule, channel, interval);
        
        ChannelSchedule updatedBlock = Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks());
        assertThat(Iterables.getOnlyElement(updatedBlock.getEntries()), is(updatedEntry));
        assertThat(scheduleUpdate.getStaleEntries(), hasItem(currentEntry));
    }

    @Test
    public void testUpdatesWhenBroadcastOverlapUpdateIntervalStart() throws Exception {
        Interval interval = new Interval(50, 150, DateTimeZones.UTC);
        
        ItemAndBroadcast currentEntry = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 100, DateTimeZones.UTC));
        List<ChannelSchedule> currentSchedule = ImmutableList.of(new ChannelSchedule(channel, interval, ImmutableList.of(currentEntry)));
        
        List<ItemAndBroadcast> updateEntries = ImmutableList.of(new ItemAndBroadcast(
                currentEntry.getItem().copy(),
                currentEntry.getBroadcast().copy().withId("different")
        ));
        
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), updateEntries, channel, interval);
        
        assertThat(scheduleUpdate.getStaleEntries(), hasItem(currentEntry));
    }
    
    @Test
    public void testPutsBroadcastSpanningTwoBlocksInBothBlocks() throws Exception {
        Interval interval1 = new Interval(50, 150, DateTimeZones.UTC);
        Interval interval2 = new Interval(150, 250, DateTimeZones.UTC);

        List<ChannelSchedule> currentSchedule = ImmutableList.of(
            new ChannelSchedule(channel, interval1, ImmutableList.<ItemAndBroadcast>of()),
            new ChannelSchedule(channel, interval2, ImmutableList.<ItemAndBroadcast>of())
        );
        
        ItemAndBroadcast updateEntry = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(100, 200, DateTimeZones.UTC));
        
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), ImmutableList.of(updateEntry), channel, new Interval(interval1.getStart(), interval2.getEnd()));
        
        assertThat(Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks().get(0).getEntries()), is(updateEntry));
        assertThat(Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks().get(1).getEntries()), is(updateEntry));
        assertTrue(scheduleUpdate.getStaleEntries().isEmpty());
    }

    @Test
    public void testDoesntPutBroadcastInOneBlockInBothBlocks() throws Exception {
        Interval interval1 = new Interval(0, 100, DateTimeZones.UTC);
        Interval interval2 = new Interval(100, 200, DateTimeZones.UTC);
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
            new ChannelSchedule(channel, interval1, ImmutableList.<ItemAndBroadcast>of()),
            new ChannelSchedule(channel, interval2, ImmutableList.<ItemAndBroadcast>of())
        );
        
        ItemAndBroadcast updateEntry = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(25, 100, DateTimeZones.UTC));

        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), ImmutableList.of(updateEntry), channel, new Interval(interval1.getStart(), interval2.getEnd()));
        
        assertThat(Iterables.getOnlyElement(scheduleUpdate.getUpdatedBlocks().get(0).getEntries()), is(updateEntry));
        assertTrue(scheduleUpdate.getUpdatedBlocks().get(1).getEntries().isEmpty());

        assertTrue(scheduleUpdate.getStaleEntries().isEmpty());
    }

    @Test
    public void testItemsAreSortedInBlock() throws Exception {
        Interval interval1 = new Interval(0, 200, DateTimeZones.UTC);
        
        Interval overwrittenInterval = new Interval(100, 200, DateTimeZones.UTC);
        
        ItemAndBroadcast episodeAndBroadcast1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 100, DateTimeZones.UTC));
        ItemAndBroadcast episodeAndBroadcast2 = itemAndBroadcast(2, METABROADCAST, channel, "two", overwrittenInterval);
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
            new ChannelSchedule(channel, interval1, ImmutableList.of(
                episodeAndBroadcast1, episodeAndBroadcast2
            ))
        );

        ItemAndBroadcast episodeAndBroadcast3 = itemAndBroadcast(3, METABROADCAST, channel, "two", overwrittenInterval);
        
        ScheduleBlocksUpdate scheduleUpdate = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), ImmutableList.of(episodeAndBroadcast3), channel, overwrittenInterval);
        
        assertThat(scheduleUpdate.getUpdatedBlocks().get(0).getEntries().get(0), is(episodeAndBroadcast1));
        assertThat(scheduleUpdate.getUpdatedBlocks().get(0).getEntries().get(1), is(episodeAndBroadcast3));

        assertThat(scheduleUpdate.getStaleContent(), hasItem(episodeAndBroadcast2));
    }

    @Test
    public void testItemWithUpdatedBroadcastTimeDoesntAppearTwice() throws Exception {
        Interval interval = new Interval(0, 200, DateTimeZones.UTC);
        
        ItemAndBroadcast iab1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 100, DateTimeZones.UTC));
        ItemAndBroadcast iab2 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(5, 105, DateTimeZones.UTC));
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
                new ChannelSchedule(channel, interval, ImmutableList.of(iab1))
        );
        
        ScheduleBlocksUpdate updatedSchedule = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), ImmutableList.of(iab2), channel, interval);
        
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(0), is(iab2));

        assertTrue(updatedSchedule.getStaleEntries().isEmpty());
    }

    @Test
    public void testItemCanAppearMultipleTimesInASchedule() throws Exception {
        Interval interval1 = new Interval(0, 300, DateTimeZones.UTC);
        
        ItemAndBroadcast iab1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 100, DateTimeZones.UTC));
        ItemAndBroadcast iab2 = itemAndBroadcast(1, METABROADCAST, channel, "two", new Interval(100, 200, DateTimeZones.UTC));
        ItemAndBroadcast iab3 = itemAndBroadcast(1, METABROADCAST, channel, "three", new Interval(200, 300, DateTimeZones.UTC));
        
        ItemAndBroadcast iab4 = itemAndBroadcast(2, METABROADCAST, channel, "four", new Interval(100, 200, DateTimeZones.UTC));
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
            new ChannelSchedule(channel, interval1, ImmutableList.of(iab1, iab2, iab3))
        );
        
        Interval overwrittenInterval = new Interval(100, 300, DateTimeZones.UTC);
        
        List<ItemAndBroadcast> updateEntries = ImmutableList.of(iab4, iab3);

        ScheduleBlocksUpdate update = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), updateEntries, channel, overwrittenInterval);
        
        ChannelSchedule block = update.getUpdatedBlocks().get(0);
        assertThat(block.getEntries().size(), is(3));
        assertThat(block.getEntries().get(0), is(iab1));
        assertThat(block.getEntries().get(1), is(iab4));
        assertThat(block.getEntries().get(2), is(iab3));
        
        assertThat(update.getStaleEntries(), hasItem(iab2));
    }
    
    @Test
    public void testStaleEntryOnlyAppearsOnceInStaleEntries() throws Exception {
        Interval interval = utcInterval(0, 200);
        
        ItemAndBroadcast episode1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode1Copy = new ItemAndBroadcast(
            episode1.getItem().copy(), 
            episode1.getBroadcast().copy()
        );
        
        List<ChannelSchedule> currentSchedule = ImmutableList.of(
            new ChannelSchedule(channel, utcInterval(0,100), ImmutableList.of(
                episode1
            )),
            new ChannelSchedule(channel, utcInterval(100,200), ImmutableList.of(
                episode1Copy
            ))
        );
        
        ItemAndBroadcast episode2 = itemAndBroadcast(2, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));
        List<ItemAndBroadcast> updateEntries = ImmutableList.of(episode2);
        
        ScheduleBlocksUpdate updatedSchedule = updater.updateBlocks(currentSchedule, ImmutableList.<ChannelSchedule>of(), updateEntries, channel, interval);
        
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(0), is(episode2));
        assertThat(updatedSchedule.getUpdatedBlocks().get(1).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(1).getEntries().get(0), is(episode2));
        
        assertThat(Iterables.getOnlyElement(updatedSchedule.getStaleContent()), is(episode1));
    }

    @Test
    public void testPastBlocksAppearInStaleEntries() throws Exception {
        Interval interval = utcInterval(0, 200);

        ItemAndBroadcast episode1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode2 = itemAndBroadcast(2, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode3 = itemAndBroadcast(1, METABROADCAST, channel, "two", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode4 = itemAndBroadcast(1, METABROADCAST, channel, "three", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode1Copy = new ItemAndBroadcast(
                episode1.getItem().copy(),
                episode1.getBroadcast().copy()
        );

        List<ChannelSchedule> currentSchedule = ImmutableList.of(
                new ChannelSchedule(channel, utcInterval(0, 100), ImmutableList.of(
                        episode1
                )),
                new ChannelSchedule(channel, utcInterval(100, 200), ImmutableList.of(
                        episode1Copy
                ))
        );

        List<ChannelSchedule> pastSchedule = ImmutableList.of(
                new ChannelSchedule(channel, utcInterval(0, 100), ImmutableList.of(
                        episode3
                )),
                new ChannelSchedule(channel, utcInterval(100, 200), ImmutableList.of(
                        episode4
                ))
        );

        List<ItemAndBroadcast> updateEntries = ImmutableList.of(episode2);

        ScheduleBlocksUpdate updatedSchedule = updater.updateBlocks(currentSchedule, pastSchedule, updateEntries, channel, interval);

        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(0), is(episode2));
        assertThat(updatedSchedule.getUpdatedBlocks().get(1).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(1).getEntries().get(0), is(episode2));

        assertThat(updatedSchedule.getStaleEntries(), containsInAnyOrder(episode3, episode4));
        assertThat(updatedSchedule.getStaleContent(), containsInAnyOrder(episode1));
    }

    @Test
    public void testDontMarkAsStaleBroadcastsWhereContentIdChanged() throws Exception {
        Interval interval = utcInterval(0, 200);

        ItemAndBroadcast episode1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));
        ItemAndBroadcast episode2 = itemAndBroadcast(2, METABROADCAST, channel, "one", new Interval(0, 200, DateTimeZones.UTC));

        List<ChannelSchedule> pastSchedule = ImmutableList.of(
                new ChannelSchedule(channel, utcInterval(0, 200), ImmutableList.of(
                        episode1
                ))
        );

        List<ItemAndBroadcast> updateEntries = ImmutableList.of(episode2);

        ScheduleBlocksUpdate updatedSchedule = updater.updateBlocks(pastSchedule, ImmutableList.of(), updateEntries, channel, interval);

        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().size(), is(1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(0), is(episode2));

        assertThat(updatedSchedule.getStaleEntries().isEmpty(), is(true));

        assertThat(updatedSchedule.getStaleContent().size(), is(1));
        assertThat(updatedSchedule.getStaleContent(), containsInAnyOrder(episode1));
    }

    @Test
    public void testDontMarkAsStaleBroadcastsOutsideTheUpdateInterval() throws Exception {
        DateTime episode1Start = new DateTime(2015, 10, 27, 11, 0, 0,  DateTimeZone.UTC);
        DateTime episode1End = new DateTime(2015, 10, 27, 12, 0, 0,  DateTimeZone.UTC);

        DateTime episode2Start = episode1End;
        DateTime episode2End = new DateTime(2015, 10, 28, 0, 0, 0,  DateTimeZone.UTC);
        DateTime episode3Start = episode2Start;
        DateTime episode3End = new DateTime(2015, 10, 28, 12, 0, 0, DateTimeZone.UTC);

        DateTime episode4Start = episode2Start;
        DateTime episode4End = new DateTime(2015, 10, 28, 6, 0 , 0, DateTimeZone.UTC);

        DateTime episode5Start = episode4Start;
        DateTime episode5End = episode3End;

        ItemAndBroadcast episode1 = itemAndBroadcast(1, METABROADCAST, channel, "one", new Interval(episode1Start, episode1End));
        ItemAndBroadcast episode2 = itemAndBroadcast(2, METABROADCAST, channel, "two", new Interval(episode2Start, episode2End));
        ItemAndBroadcast episode3 = itemAndBroadcast(3, METABROADCAST, channel, "three", new Interval(episode3Start, episode3End));
        ItemAndBroadcast episode4 = itemAndBroadcast(4, METABROADCAST, channel, "four", new Interval(episode4Start, episode4End));
        ItemAndBroadcast episode5 = itemAndBroadcast(5, METABROADCAST, channel, "five", new Interval(episode5Start, episode5End));

        List<ChannelSchedule> pastSchedule = ImmutableList.of(
                new ChannelSchedule(
                        channel,
                        new Interval(episode1Start, episode2End),
                        ImmutableList.of(
                                episode1,
                                episode2
                        )
                ),
                new ChannelSchedule(
                        channel,
                        new Interval(episode3Start, episode3End),
                        ImmutableList.of(
                                episode3
                        )
                )
        );

        List<ItemAndBroadcast> updateEntries = ImmutableList.of(episode2, episode4, episode5);

        ScheduleBlocksUpdate updatedSchedule = updater.updateBlocks(
                pastSchedule,
                ImmutableList.of(),
                updateEntries,
                channel,
                new Interval(episode2Start, episode5End)
        );

        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().size(), is(4));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(0), is(episode1));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(1), is(episode2));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(2), is(episode4));
        assertThat(updatedSchedule.getUpdatedBlocks().get(0).getEntries().get(3), is(episode5));

        assertThat(updatedSchedule.getStaleEntries().size(), is(1));
        assertThat(updatedSchedule.getStaleEntries() , is(ImmutableSet.of(episode3)));


    }

    private Interval utcInterval(int startInstant, int endInstant) {
        return new Interval(startInstant, endInstant, DateTimeZones.UTC);
    }
    
    private ItemAndBroadcast itemAndBroadcast(int id, Publisher source, Channel channel, String bId, Interval interval) {
        Episode episode = episode(id, source);
        Broadcast broadcast = broadcast(channel, bId, interval);
        episode.addBroadcast(broadcast);
        return new ItemAndBroadcast(episode, broadcast);
    }

    private Broadcast broadcast(Channel channel, String bId, Interval interval) {
        Broadcast b = new Broadcast(channel, interval.getStart(), interval.getEnd());
        b.withId(bId);
        return b;
    }

    private Episode episode(int id, Publisher source) {
        Episode episode = new Episode();
        episode.setId(id);
        episode.setPublisher(source);
        return episode;
    }

}
