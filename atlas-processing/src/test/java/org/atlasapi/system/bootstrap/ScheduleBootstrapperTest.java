package org.atlasapi.system.bootstrap;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.media.entity.Schedule;
import org.joda.time.Interval;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.*;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleBootstrapperTest {


    @Mock
    ListeningExecutorService executorService;
    @Mock
    ChannelIntervalScheduleBootstrapTaskFactory taskFactory;


    @InjectMocks
    ScheduleBootstrapper objectUnderTest;
    @Test
    public void testBootstrapSchedules() throws Exception {
        Channel channel1 = mock(Channel.class);
        Channel channel2 = mock(Channel.class);
        Channel channel3 = mock(Channel.class);

        Interval interval = new Interval(1, 2);

        Publisher source = Publisher.BBC_KIWI;

        ChannelIntervalScheduleBootstrapTask task1 = mock(ChannelIntervalScheduleBootstrapTask.class);
        ChannelIntervalScheduleBootstrapTask task2 = mock(ChannelIntervalScheduleBootstrapTask.class);
        ChannelIntervalScheduleBootstrapTask task3 = mock(ChannelIntervalScheduleBootstrapTask.class);

        when(taskFactory.create(source, channel1, interval)).thenReturn(task1);
        when(taskFactory.create(source, channel2, interval)).thenReturn(task2);
        when(taskFactory.create(source, channel3, interval)).thenReturn(task3);

        UpdateProgress up1 = new UpdateProgress(1, 0);
        UpdateProgress up2 = new UpdateProgress(2, 0);
        UpdateProgress up3 = new UpdateProgress(3, 0);

        when(executorService.submit(task1)).thenReturn(Futures.immediateFuture(up1));
        when(executorService.submit(task2)).thenReturn(Futures.immediateFuture(up2));
        when(executorService.submit(task3)).thenReturn(Futures.immediateFuture(up3));

        objectUnderTest.bootstrapSchedules(
                ImmutableSet.of(
                        channel1,
                        channel2,
                        channel3
                ),
                interval,
                source
        );

        assertThat(objectUnderTest.getProgress().getProcessed(), is(3));
        assertThat(objectUnderTest.getProgress().getFailures(), is(0));
    }




}