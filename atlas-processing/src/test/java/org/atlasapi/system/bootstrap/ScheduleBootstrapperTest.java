package org.atlasapi.system.bootstrap;

import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListeningExecutorService;
import com.metabroadcast.common.scheduling.UpdateProgress;
import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Mockito.mock;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ScheduleBootstrapperTest {

    private @Mock ListeningExecutorService executorService;
    private @Mock ChannelIntervalScheduleBootstrapTaskFactory taskFactory;
    private @Mock ScheduleBootstrapWithContentMigrationTaskFactory bootstrapWithMigrationTaskFactory;
    private @Mock EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory equivTaskFactory;

    private ScheduleBootstrapper objectUnderTest;

    @Before
    public void setUp() throws Exception {
        objectUnderTest = new ScheduleBootstrapper(
                executorService,
                taskFactory,
                bootstrapWithMigrationTaskFactory,
                equivTaskFactory,
                taskFactory
        );
    }

    @Test
    public void testBootstrapSchedules() throws Exception {
        Channel channel1 = mock(Channel.class);
        Channel channel2 = mock(Channel.class);
        Channel channel3 = mock(Channel.class);
        when(channel1.getId()).thenReturn(Id.valueOf(1));
        when(channel2.getId()).thenReturn(Id.valueOf(2));
        when(channel3.getId()).thenReturn(Id.valueOf(3));


        Interval interval = new Interval(1, 2);

        Publisher source = Publisher.BBC_KIWI;

        ChannelIntervalScheduleBootstrapTask task1 = mock(ChannelIntervalScheduleBootstrapTask.class);
        ChannelIntervalScheduleBootstrapTask task2 = mock(ChannelIntervalScheduleBootstrapTask.class);
        ChannelIntervalScheduleBootstrapTask task3 = mock(ChannelIntervalScheduleBootstrapTask.class);

        when(taskFactory.create(source, channel1, interval)).thenReturn(task1);
        when(taskFactory.create(source, channel2, interval)).thenReturn(task2);
        when(taskFactory.create(source, channel3, interval)).thenReturn(task3);

        UpdateProgress up = new UpdateProgress(1, 0);

        when(executorService.submit(any(ChannelIntervalScheduleBootstrapTask.class))).thenReturn(Futures.immediateFuture(up));

        ScheduleBootstrapper.Status progress = objectUnderTest.bootstrapSchedules(
                ImmutableSet.of(
                        channel1,
                        channel2,
                        channel3
                ),
                interval,
                source,
                false,
                false,
                false
        );

        assertThat(progress.getProcessed(), is(3));
        assertThat(progress.getFailures(), is(0));
    }

}