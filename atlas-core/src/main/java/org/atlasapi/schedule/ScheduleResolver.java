package org.atlasapi.schedule;

import org.atlasapi.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.Interval;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * Resolves a schedules of {@link org.atlasapi.media.entity.Item}s for given
 * {@link Channel}s in an interval {@link Interval}.
 * 
 */
public interface ScheduleResolver {

    /**
     * Resolve a schedule for each of the provided Channels for the given
     * Interval according to a Publisher.
     * 
     * For a non-empty Interval, all the Items in a given
     * {@link org.org.atlasapi.schedule.ChannelSchedule} will have a
     * {@link org.atlasapi.media.entity.Broadcast} on the relevant Channel which
     * either starts or ends strictly within the Interval.
     * 
     * For an empty Interval (where the start instant is the end instant), all
     * the Items will have a {@link org.atlasapi.media.entity.Broadcast} which
     * either contains or starts at the Interval instant.
     * 
     * @param channels
     *            - the Channels for which the schedules should be resolved.
     * @param interval
     *            - the Interval over which the schedules are required.
     * @param source - the Publisher of the schedule data.
     * @return - a {@link Schedule}, containing one {@code ChannelSchedule} per requested channel.
     */
    ListenableFuture<Schedule> resolve(Iterable<Channel> channels,
            Interval interval, Publisher source);

}
