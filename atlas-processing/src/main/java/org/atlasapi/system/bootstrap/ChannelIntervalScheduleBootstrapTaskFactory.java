package org.atlasapi.system.bootstrap;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ContentVisitor;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.schedule.ScheduleResolver;
import org.atlasapi.schedule.ScheduleWriter;

import com.google.common.base.Optional;
import org.joda.time.Interval;

import static com.google.common.base.Preconditions.checkNotNull;

public class ChannelIntervalScheduleBootstrapTaskFactory
        implements SourceChannelIntervalFactory<ChannelIntervalScheduleBootstrapTask> {

    private final ScheduleResolver scheduleResolver;
    private final ScheduleWriter scheduleWriter;
    private final ContentStore contentStore;

    public ChannelIntervalScheduleBootstrapTaskFactory(ScheduleResolver scheduleResolver,
            ScheduleWriter scheduleWriter, ContentStore contentStore) {
        this.scheduleResolver = checkNotNull(scheduleResolver);
        this.scheduleWriter = checkNotNull(scheduleWriter);
        this.contentStore = checkNotNull(contentStore);
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
                Optional.<ContentVisitor<?>>absent()
        );
    }

}
