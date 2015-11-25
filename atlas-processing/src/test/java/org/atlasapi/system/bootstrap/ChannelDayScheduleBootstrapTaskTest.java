package org.atlasapi.system.bootstrap;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ChannelSchedule;
import org.atlasapi.schedule.Schedule;
import org.atlasapi.schedule.ScheduleHierarchy;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.metabroadcast.common.scheduling.UpdateProgress;
import com.metabroadcast.common.time.DateTimeZones;

@RunWith(MockitoJUnitRunner.class)
public class ChannelDayScheduleBootstrapTaskTest {
    
    @Mock ScheduleResolver resolver;
    @Mock ScheduleWriter writer;
    @Mock ContentStore contentStore;

    @Test
    public void testRunningTask() throws Exception {
        Publisher src = Publisher.METABROADCAST;

        Channel chan = Channel.builder(Publisher.BBC).build();
        chan.setId(123L);
        
        LocalDate day = new LocalDate();
        Interval interval = new Interval(day.toDateTimeAtStartOfDay(DateTimeZones.UTC), 
                day.plusDays(1).toDateTimeAtStartOfDay(DateTimeZones.UTC));
        
        ChannelIntervalScheduleBootstrapTask task 
            = new ChannelIntervalScheduleBootstrapTask(resolver, writer, contentStore, src, chan, interval, 
                    Optional.absent());
        
        Brand brand1 = new Brand(Id.valueOf(11), src);
        Item item1 = new Item(Id.valueOf(1), src);
        item1.setContainerRef(brand1.toRef());
        Broadcast broadcast1 = new Broadcast(chan, interval);
        
        Brand brand2 = new Brand(Id.valueOf(21), src);
        Series series2 = new Series(Id.valueOf(22), src);
        Episode item2 = new Episode(Id.valueOf(2), src);
        item2.setContainerRef(brand2.toRef());
        item2.setSeriesRef(series2.toRef());
        Broadcast broadcast2 = new Broadcast(chan, interval);
        
        ItemAndBroadcast iab1 = new ItemAndBroadcast(item1, broadcast1);
        ItemAndBroadcast iab2 = new ItemAndBroadcast(item2, broadcast2);
        
        when(resolver.resolve(ImmutableSet.of(chan), interval, src))
            .thenReturn(Futures.immediateFuture(new Schedule(ImmutableList.of(
                new ChannelSchedule(chan, interval, ImmutableList.of(iab1,iab2))
            ), interval)));
        
        when(contentStore.resolveIds(ImmutableSet.of(item1.getId(), item2.getId())))
            .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.<Content>of(item1, item2))));
        when(contentStore.resolveIds(ImmutableSet.of(brand1.getId(), brand2.getId(), series2.getId())))
            .thenReturn(Futures.immediateFuture(Resolved.valueOf(ImmutableSet.<Content>of(brand1, brand2, series2))));
        
        UpdateProgress call = task.call();
        assertThat(call.getProcessed(), is(2));
        assertThat(call.getFailures(), is(0));
        
        verify(writer).writeSchedule(ImmutableList.of(
            ScheduleHierarchy.brandAndItem(brand1, iab1), 
            ScheduleHierarchy.brandSeriesAndItem(brand2, series2, iab2)
        ), chan, interval);
        
    }

}
