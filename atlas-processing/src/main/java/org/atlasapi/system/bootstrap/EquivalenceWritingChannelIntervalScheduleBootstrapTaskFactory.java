package org.atlasapi.system.bootstrap;

import static com.google.common.base.Preconditions.checkNotNull;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.EquivalentScheduleWriter;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;
import org.joda.time.Interval;

import com.google.common.base.Optional;

public class EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory
        implements SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> {

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentStore contentStore;
    private final EquivalentScheduleWriter equivalenceWriter;
    private final EquivalenceGraphStore graphStore;

    public EquivalenceWritingChannelIntervalScheduleBootstrapTaskFactory(
            ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter,
            ContentStore contentStore,
            EquivalentScheduleWriter equivalenceWriter,
            EquivalenceGraphStore equivGraphStore
    ) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
        this.equivalenceWriter = equivalenceWriter;
        this.graphStore = equivGraphStore;
    }

    @Override
    public ChannelIntervalScheduleBootstrapTask create(Publisher source, Channel channel,
            Interval interval) {
        return new ChannelIntervalScheduleBootstrapTask(
                scheduleResolver,
                scheduleWriter,
                contentStore,
                source,
                channel,
                interval,
                Optional.<ContentVisitor<?>>absent(),
                Optional.of(equivalenceWriter), Optional.of(graphStore));
    }
}
