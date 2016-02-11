package org.atlasapi.query.v4.schedule;

import java.util.List;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.schedule.ChannelSchedule;

import com.google.common.collect.ImmutableList;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleMergerImplTest {

    private ScheduleMerger merger;

    @Mock private Channel channel;

    @Before
    public void setUp() {
        merger = new ScheduleMergerImpl();
        when(channel.getId()).thenReturn(Id.valueOf(42));
    }

    /*
    Overrides      ------
    Original    -----
     */
    @Test
    public void overlappingOverrideTruncatesExistingShowEndTime() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart, overrideEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(2));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));
    }

    /*
    Overrides      ------     ------
    Original    -----
     */
    @Test
    public void moreOverridesThanRegularItems() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart0 = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd0 = overrideStart0.plus(Duration.standardHours(2));
        DateTime overrideStart1 = overrideEnd0.plus(Duration.standardHours(3));
        DateTime overrideEnd1 = overrideStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart, overrideEnd1);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast0 = new Broadcast(channel, overrideStart0, overrideEnd0);
        Broadcast overrideBroadcast1 = new Broadcast(channel, overrideStart1, overrideEnd1);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast0),
                new ItemAndBroadcast(overrideItem, overrideBroadcast1)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart0));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart0));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd0));

        ItemAndBroadcast tailingOverride = entries.get(2);
        Broadcast tailingBroadcast = tailingOverride.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideStart1));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(overrideEnd1));
    }

    /*
    Overrides      ------
    Original    -----        ------
     */
    @Test
    public void moreRegularsThanOverrides() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));

        DateTime overrideStart = originalStart0.plus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(2));

        DateTime originalStart1 = overrideEnd.plus(Duration.standardHours(3));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast originalBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast originalBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(24));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast0),
                new ItemAndBroadcast(originalItem, originalBroadcast1)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(25));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast tailingOriginal = entries.get(2);
        Broadcast tailingBroadcast = tailingOriginal.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(originalStart1));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides         -------
    Original     ------------------
     */
    @Test
    public void originalWrappingOverrideOnlyKeepsPrefixPart() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(3));
        DateTime overrideStart = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd = originalEnd.minus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, originalEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(2));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));
    }

    /*
    Overrides      ------
    Original    -----  ------
     */
    @Test
    public void originalsAtStartAndEndOfOverride() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));

        DateTime overrideStart = originalEnd0.minus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(3));

        DateTime originalStart1 = overrideEnd.minus(Duration.standardHours(1));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast originalBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast originalBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(24));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast0),
                new ItemAndBroadcast(originalItem, originalBroadcast1)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(25));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedFirstOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedFirstOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast truncatedTailingOriginal = entries.get(2);
        Broadcast tailingBroadcast = truncatedTailingOriginal.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideEnd));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides ------  --------
    Original
     */
    @Test
    public void emptyOriginalScheduleReturnsAllOverrideItems() {
        DateTime overrideStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime overrideEnd0 = overrideStart0.plus(Duration.standardHours(2));
        DateTime overrideStart1 = overrideEnd0.plus(Duration.standardHours(3));
        DateTime overrideEnd1 = overrideStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(overrideStart0, overrideEnd1);

        Broadcast overrideBroadcast0 = new Broadcast(channel, overrideStart0, overrideEnd0);
        Broadcast overrideBroadcast1 = new Broadcast(channel, overrideStart1, overrideEnd1);

        ChannelSchedule originalSchedule = new ChannelSchedule(
                channel, interval, ImmutableList.of()
        );

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast0),
                new ItemAndBroadcast(overrideItem, overrideBroadcast1)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(2));

        ItemAndBroadcast override0 = entries.get(0);
        Broadcast broadcast0 = override0.getBroadcast();
        assertThat(broadcast0.getTransmissionTime(), is(overrideStart0));
        assertThat(broadcast0.getTransmissionEndTime(), is(overrideEnd0));

        ItemAndBroadcast override1 = entries.get(1);
        Broadcast broadcast1 = override1.getBroadcast();
        assertThat(broadcast1.getTransmissionTime(), is(overrideStart1));
        assertThat(broadcast1.getTransmissionEndTime(), is(overrideEnd1));
    }

    /*
    Overrides
    Original ------  --------
     */
    @Test
    public void emptyOverrideScheduleReturnsAllOriginalItems() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));
        DateTime originalStart1 = originalEnd0.plus(Duration.standardHours(3));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast overrideBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast overrideBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, overrideBroadcast0),
                new ItemAndBroadcast(originalItem, overrideBroadcast1)
        ));

        ChannelSchedule overrideSchedule = new ChannelSchedule(
                channel, interval, ImmutableList.of()
        );

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(2));

        ItemAndBroadcast original0 = entries.get(0);
        Broadcast broadcast0 = original0.getBroadcast();
        assertThat(broadcast0.getTransmissionTime(), is(originalStart0));
        assertThat(broadcast0.getTransmissionEndTime(), is(originalEnd0));

        ItemAndBroadcast original1 = entries.get(1);
        Broadcast broadcast1 = original1.getBroadcast();
        assertThat(broadcast1.getTransmissionTime(), is(originalStart1));
        assertThat(broadcast1.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides      ------
    Original           ------
     */
    @Test
    public void overlappingOverrideTruncatesExistingShowStartTime() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart = originalStart.minus(Duration.standardHours(1));
        DateTime overrideEnd = originalStart.plus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, overrideEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(2));

        ItemAndBroadcast override = entries.get(0);
        Broadcast broadcast = override.getBroadcast();
        assertThat(broadcast.getTransmissionTime(), is(overrideStart));
        assertThat(broadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast truncatedOriginal = entries.get(1);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(overrideEnd));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(originalEnd));
    }


    /*
    Overrides      --------------
    Original           ------
     */
    @Test
    public void overrideDeletesShowCompletelyContainedWithin() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart = originalStart.minus(Duration.standardHours(1));
        DateTime overrideEnd = originalEnd.plus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, overrideEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(1));

        ItemAndBroadcast override = entries.get(0);
        Broadcast broadcast = override.getBroadcast();
        assertThat(broadcast.getTransmissionTime(), is(overrideStart));
        assertThat(broadcast.getTransmissionEndTime(), is(overrideEnd));
    }

    /*
    Overrides      ------    ------
    Original           --------
     */
    @Test
    public void originalWedgedBetweenTwoOverrides() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(3));

        DateTime overrideStart0 = originalStart.minus(Duration.standardHours(1));
        DateTime overrideEnd0 = originalStart.plus(Duration.standardHours(1));

        DateTime overrideStart1 = originalEnd.minus(Duration.standardHours(1));
        DateTime overrideEnd1 = originalEnd.plus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, overrideEnd1);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast0 = new Broadcast(channel, overrideStart0, overrideEnd0);
        Broadcast overrideBroadcast1 = new Broadcast(channel, overrideStart1, overrideEnd1);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast0),
                new ItemAndBroadcast(overrideItem, overrideBroadcast1)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast firstOverride = entries.get(0);
        Broadcast firstOverrideBroadcast = firstOverride.getBroadcast();
        assertThat(firstOverrideBroadcast.getTransmissionTime(), is(overrideStart0));
        assertThat(firstOverrideBroadcast.getTransmissionEndTime(), is(overrideEnd0));

        ItemAndBroadcast truncatedOriginal = entries.get(1);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(overrideEnd0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart1));

        ItemAndBroadcast tailingOverride = entries.get(2);
        Broadcast tailingBroadcast = tailingOverride.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideStart1));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(overrideEnd1));
    }

    /*
    Overrides      ------          ------
    Original                  --------
     */
    @Test
    public void prefixedOverrideBeforeAnotherRelevantOne() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));

        DateTime overrideEnd0 = originalStart.minus(Duration.standardHours(1));
        DateTime overrideStart0 = overrideEnd0.minus(Duration.standardHours(2));

        DateTime overrideStart1 = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd1 = originalEnd.plus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, overrideEnd1);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast overrideBroadcast0 = new Broadcast(channel, overrideStart0, overrideEnd0);
        Broadcast overrideBroadcast1 = new Broadcast(channel, overrideStart1, overrideEnd1);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast0),
                new ItemAndBroadcast(overrideItem, overrideBroadcast1)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast firstOverride = entries.get(0);
        Broadcast firstOverrideBroadcast = firstOverride.getBroadcast();
        assertThat(firstOverrideBroadcast.getTransmissionTime(), is(overrideStart0));
        assertThat(firstOverrideBroadcast.getTransmissionEndTime(), is(overrideEnd0));

        ItemAndBroadcast truncatedOriginal = entries.get(1);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart1));

        ItemAndBroadcast tailingOverride = entries.get(2);
        Broadcast tailingBroadcast = tailingOverride.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideStart1));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(overrideEnd1));
    }

    /*
    Overrides      --------------
    Original   -----  ------  -------
     */
    @Test
    public void overrideDeletesCompletelyContainedOriginalBetweenOtherOriginals() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));

        DateTime overrideStart = originalEnd0.minus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(5));

        DateTime originalStart1 = overrideEnd.minus(Duration.standardHours(1));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));

        DateTime originalShadowedStart = originalEnd0.plus(Duration.standardHours(1));
        DateTime originalShadowedEnd = originalEnd1.minus(Duration.standardHours(1));

        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast originalBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast originalBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);
        Broadcast originalBroadcast2 = new Broadcast(
                channel, originalShadowedStart, originalShadowedEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(24));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast0),
                new ItemAndBroadcast(originalItem, originalBroadcast1),
                new ItemAndBroadcast(originalItem, originalBroadcast2)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(25));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedFirstOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedFirstOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast truncatedTailingOriginal = entries.get(2);
        Broadcast tailingBroadcast = truncatedTailingOriginal.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideEnd));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides            ------
    Original   -------             ------
     */
    @Test
    public void overrideInsertedIntoScheduleWhenNoConflicts() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));

        DateTime overrideStart = originalEnd0.plus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(2));

        DateTime originalStart1 = overrideEnd.plus(Duration.standardHours(1));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast originalBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast originalBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(24));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast0),
                new ItemAndBroadcast(originalItem, originalBroadcast1)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(25));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedFirstOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedFirstOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(originalEnd0));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast truncatedTailingOriginal = entries.get(2);
        Broadcast tailingBroadcast = truncatedTailingOriginal.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(originalStart1));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides        ------
    Original   -------0
     */
    @Test
    public void followOnBelongsToPreviousShow() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart, overrideEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast followOnBroadcast = new Broadcast(channel, originalEnd, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast),
                new ItemAndBroadcast(originalItem, followOnBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast followOn = entries.get(1);
        Broadcast mergedFollowOnBroadcast = followOn.getBroadcast();
        assertThat(mergedFollowOnBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(mergedFollowOnBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(2);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));
    }

    /*
    Overrides              ------
    Original   -------0
     */
    @Test
    public void nonConflictingFollowOnKeepsItsOriginalTimes() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(2));
        DateTime overrideStart = originalEnd.plus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(2));
        Interval interval = new Interval(originalStart, overrideEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast followOnBroadcast = new Broadcast(channel, originalEnd, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(1));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast),
                new ItemAndBroadcast(originalItem, followOnBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(1));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(originalEnd));

        ItemAndBroadcast followOn = entries.get(1);
        Broadcast mergedFollowOnBroadcast = followOn.getBroadcast();
        assertThat(mergedFollowOnBroadcast.getTransmissionTime(), is(originalEnd));
        assertThat(mergedFollowOnBroadcast.getTransmissionEndTime(), is(originalEnd));

        ItemAndBroadcast insertedOverride = entries.get(2);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));
    }

    /*
    Overrides       ------------
    Original   -------  ----0 ------
     */
    @Test
    public void overrideRemovesShadowedOriginalWithFollowOn() {
        DateTime originalStart0 = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd0 = originalStart0.plus(Duration.standardHours(2));

        DateTime overrideStart = originalEnd0.minus(Duration.standardHours(1));
        DateTime overrideEnd = overrideStart.plus(Duration.standardHours(5));

        DateTime originalStart1 = overrideEnd.minus(Duration.standardHours(1));
        DateTime originalEnd1 = originalStart1.plus(Duration.standardHours(2));

        DateTime originalShadowedStart = originalEnd0.plus(Duration.standardHours(1));
        DateTime originalShadowedEnd = originalShadowedStart.plus(Duration.standardHours(1));

        Interval interval = new Interval(originalStart0, originalEnd1);

        Broadcast originalBroadcast0 = new Broadcast(channel, originalStart0, originalEnd0);
        Broadcast originalBroadcast1 = new Broadcast(channel, originalStart1, originalEnd1);
        Broadcast originalMiddleFollowOn = new Broadcast(
                channel, originalShadowedEnd, originalShadowedEnd);
        Broadcast originalShadowed = new Broadcast(
                channel, originalShadowedStart, originalShadowedEnd);

        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        when(originalItem.getId()).thenReturn(Id.valueOf(24));
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast0),
                new ItemAndBroadcast(originalItem, originalShadowed),
                new ItemAndBroadcast(originalItem, originalMiddleFollowOn),
                new ItemAndBroadcast(originalItem, originalBroadcast1)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        when(overrideItem.getId()).thenReturn(Id.valueOf(25));
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedFirstOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedFirstOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart0));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(1);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));

        ItemAndBroadcast truncatedTailingOriginal = entries.get(2);
        Broadcast tailingBroadcast = truncatedTailingOriginal.getBroadcast();
        assertThat(tailingBroadcast.getTransmissionTime(), is(overrideEnd));
        assertThat(tailingBroadcast.getTransmissionEndTime(), is(originalEnd1));
    }

    /*
    Overrides       ------------
    Original   -----------------------0
     */
    @Test
    public void overlappingOriginalWithFollowOnKeepsFollowOn() {
        DateTime originalStart = DateTime.parse("2016-02-10T14:42:41.242Z");
        DateTime originalEnd = originalStart.plus(Duration.standardHours(3));
        DateTime overrideStart = originalStart.plus(Duration.standardHours(1));
        DateTime overrideEnd = originalEnd.minus(Duration.standardHours(1));
        Interval interval = new Interval(originalStart, originalEnd);

        Broadcast originalBroadcast = new Broadcast(channel, originalStart, originalEnd);
        Broadcast followOnBroadcast = new Broadcast(channel, originalEnd, originalEnd);
        Broadcast overrideBroadcast = new Broadcast(channel, overrideStart, overrideEnd);

        Item originalItem = mock(Item.class, "originalItem");
        when(originalItem.copy()).thenReturn(originalItem);
        ChannelSchedule originalSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(originalItem, originalBroadcast),
                new ItemAndBroadcast(originalItem, followOnBroadcast)
        ));

        Item overrideItem = mock(Item.class, "overrideItem");
        when(overrideItem.copy()).thenReturn(overrideItem);
        ChannelSchedule overrideSchedule = new ChannelSchedule(channel, interval, ImmutableList.of(
                new ItemAndBroadcast(overrideItem, overrideBroadcast)
        ));

        ChannelSchedule merged = merger.merge(originalSchedule, overrideSchedule);

        List<ItemAndBroadcast> entries = merged.getEntries();
        assertThat(entries.size(), is(3));

        ItemAndBroadcast truncatedOriginal = entries.get(0);
        Broadcast truncatedBroadcast = truncatedOriginal.getBroadcast();
        assertThat(truncatedBroadcast.getTransmissionTime(), is(originalStart));
        assertThat(truncatedBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast followOn = entries.get(1);
        Broadcast movedFollowOnBroadcast = followOn.getBroadcast();
        assertThat(movedFollowOnBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(movedFollowOnBroadcast.getTransmissionEndTime(), is(overrideStart));

        ItemAndBroadcast insertedOverride = entries.get(2);
        Broadcast insertedBroadcast = insertedOverride.getBroadcast();
        assertThat(insertedBroadcast.getTransmissionTime(), is(overrideStart));
        assertThat(insertedBroadcast.getTransmissionEndTime(), is(overrideEnd));
    }
}
