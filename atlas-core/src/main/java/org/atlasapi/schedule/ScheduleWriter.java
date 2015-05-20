package org.atlasapi.schedule;

import java.util.List;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.joda.time.Interval;

/**
 * A {@code ScheduleWriter} is responsible for persisting
 * {@link org.atlasapi.media.entity.Item Item}s and their related
 * {@link Content} hierarchies in a manner suitable for later resolution in
 * {@link org.atlasapi.schedule.media.entity.Schedule Schedule} form by {@link Channel}
 * and {@link Interval} via a {@link ScheduleResolver}.
 * 
 */
public interface ScheduleWriter {

    /**
     * Write a schedule of Items. The Items are presented in a hierarchy along
     * with their Containers. Each Item must have a
     * {@link org.atlasapi.media.entity.Broadcast Broadcast} on the provided
     * Channel within the provided Interval. The Hierarchies must be ordered
     * such that the relevant Broadcasts are contiguous.
     * 
     * All the {@link Content} must be from the same
     * {@link org.atlasapi.media.entity.Publisher Source}.
     * 
     * All Content must be persisted to a
     * {@link org.atlasapi.media.content.ContentStore ContentStore}.
     * 
     * A {@code ScheduleWriter} is also responsible for maintaining
     * {@code Broadcast} "actively published" flag status in the
     * {@link ContentStore}.
     * 
     * @param items
     *            - an list of {@link ScheduleHierarchy}s ordered by Broadcast
     *            transmission start time.
     * @param channel
     *            - the channel for which the schedule is to be written.
     * @param interval
     *            - the interval for which the schedule is provided.
     * @return write results for <strong>every</strong> piece of Content written
     *         from the hierarchies.
     * @throws WriteException
     *             if the write is not successfully performed.
     * @throws IllegalArgumentException
     *             if the presented schedule is not contiguous or the content is
     *             not source-homogenous
     */
    List<WriteResult<? extends Content, Content>> writeSchedule(List<ScheduleHierarchy> items,
            Channel channel,
            Interval interval) throws WriteException;

}
