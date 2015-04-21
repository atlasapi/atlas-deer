package org.atlasapi.schedule;

import java.util.Set;

import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Publisher;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.util.concurrent.ListenableFuture;

/**
 * <p>Resolves {@link EquivalentSchedule}s - schedules where sets of equivalent
 * {@link org.atlasapi.content.Item Item}s are present for each
 * {@link org.atlasapi.content.Broadcast Broadcast}.</p>
 * 
 * <p>Schedules are resolvable for a given {@link Interval} on a {@link Channel}
 * according to a particular {@link Publisher source}.</p>
 */
public interface EquivalentScheduleResolver {

    /**
     * <p>Resolve a schedule, according to a {@link Publisher}, of equivalent
     * {@link org.atlasapi.content.Item Item}s broadcast on the provided
     * {@link Channel}s over the given {@link Interval}.</p>
     * 
     * <p>Only Items from the selected sources are returned in the schedule.</p>
     * 
     * <p>The returned {@link EquivalentSchedule} must contain a (possibly
     * empty) {@link EquivalentChannelSchedule} for each requested
     * {@link Channel}.</p>
     * 
     * @param channels
     *            - channels for which to return schedules.
     * @param interval
     *            - interval over which the schedule should be resolved.
     * @param source
     *            - the schedule source.
     * @param selectedSources
     *            - the sources of which Items should be returned.
     * @return a {@link ListenableFuture} of the resolved schedule.
     */
    // Must source ∈ selectedSources?
    ListenableFuture<EquivalentSchedule> resolveSchedules(Iterable<Channel> channels,
            Interval interval, Publisher source, Set<Publisher> selectedSources);


    /**
     * <p>Resolve a schedule, according to a {@link Publisher}, of equivalent
     * {@link org.atlasapi.content.Item Item}s broadcast on the provided
     * {@link Channel}s over the given {@link Interval}.</p>
     *
     * <p>Only Items from the selected sources are returned in the schedule.</p>
     *
     * <p>The returned {@link EquivalentSchedule} must contain a (possibly
     * empty) {@link EquivalentChannelSchedule} for each requested
     * {@link Channel}.</p>
     *
     * @param channels
     *            - channels for which to return schedules.
     * @param start
     *            - start time of the schedule
     * @param count
     *            - number of schedule items to return. This will return at most {@code count} items
     *            or up 24h of schedule.
     * @param source
     *            - the schedule source.
     * @param selectedSources
     *            - the sources of which Items should be returned.
     * @return a {@link ListenableFuture} of the resolved schedule.
     */
    // Must source ∈ selectedSources?
    ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            DateTime start,
            Integer count,
            Publisher source,
            Set<Publisher> selectedSources
    );

}
