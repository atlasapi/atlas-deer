package org.atlasapi.schedule;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;

import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.Interval;

public interface ScheduleIndex {

    ListenableFuture<ScheduleRef> resolveSchedule(Publisher publisher, Channel channel,
            Interval scheduleInterval);

}
