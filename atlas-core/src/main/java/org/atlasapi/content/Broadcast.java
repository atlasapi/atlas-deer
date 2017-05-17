package org.atlasapi.content;

import javax.annotation.Nullable;

import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.hashing.Hashable;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.schedule.ScheduleBroadcastFilter;

import com.metabroadcast.common.time.IntervalOrdering;

import com.google.common.base.Objects;
import com.google.common.base.Optional;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * A time and channel at which a Version is/was receivable.
 */
public class Broadcast extends Identified implements Hashable {

    private static final Ordering<Broadcast> START_TIME_ORDERING = new BroadcastStartTimeOrdering();

    private final Id channelId;
    private final Interval transmissionInterval;
    private final Duration broadcastDuration;

    private LocalDate scheduleDate;
    private Boolean activelyPublished;
    private String sourceId; //Should probably be called sourceAlias.
    private String versionId;
    private Boolean repeat;
    private Boolean subtitled;
    private Boolean signed;
    private Boolean audioDescribed;
    private Boolean highDefinition;
    private Boolean widescreen;
    private Boolean surround;
    private Boolean live;
    private Boolean newSeries;
    private Boolean newEpisode;
    private Boolean newOneOff;
    private Boolean premiere;
    private Boolean continuation;
    private Boolean is3d;
    private Optional<BlackoutRestriction> blackoutRestriction = Optional.absent();
    private Boolean revisedRepeat;

    public Broadcast(Id channelId, Interval interval, Boolean activelyPublished) {
        this.channelId = checkNotNull(channelId);
        this.transmissionInterval = checkNotNull(interval);
        this.broadcastDuration = transmissionInterval.toDuration();
        this.activelyPublished = activelyPublished;
    }

    public Broadcast(Id channelId, DateTime start, DateTime end) {
        this(channelId, start, end, true);
    }

    public Broadcast(Id channelId, DateTime start, DateTime end, Boolean activelyPublished) {
        this(channelId, new Interval(start, end), activelyPublished);
    }

    public Broadcast(Id channelId, DateTime start, Duration duration) {
        this(channelId, start, duration, true);
    }

    public Broadcast(Id channelId, DateTime start, Duration duration, Boolean activelyPublished) {
        this(channelId, new Interval(start, start.plus(duration)), activelyPublished);
    }

    public Broadcast(Id channelId, Interval interval) {
        this(channelId, interval, true);
    }

    public Broadcast(Channel channel, DateTime start, DateTime end, Boolean activelyPublished) {
        this(channel.getId(), new Interval(start, end), activelyPublished);
    }

    public Broadcast(Channel channel, DateTime start, DateTime end) {
        this(channel.getId(), start, end, true);
    }

    public Broadcast(Channel channel, DateTime start, Duration duration) {
        this(channel.getId(), start, duration, true);
    }

    public Broadcast(Channel channel, Interval interval) {
        this(channel.getId(), interval, true);
    }

    public Broadcast(Channel channel, DateTime start, Duration duration,
            Boolean activelyPublished) {
        this(channel.getId(), new Interval(start, start.plus(duration)), activelyPublished);
    }

    public static Predicate<Broadcast> channelFilter(final Channel channel) {
        return new BroadcastChannelFilter(channel);
    }

    public static Predicate<Broadcast> intervalFilter(final Interval interval) {
        return new BroadcastIntervalFilter(interval);
    }

    public static Ordering<Broadcast> startTimeOrdering() {
        return START_TIME_ORDERING;
    }

    @FieldName("transmission_time")
    public DateTime getTransmissionTime() {
        return transmissionInterval.getStart();
    }

    @FieldName("transmission_end_time")
    public DateTime getTransmissionEndTime() {
        return transmissionInterval.getEnd();
    }

    @FieldName("broadcast_duration")
    public Duration getBroadcastDuration() {
        return this.broadcastDuration;
    }

    @FieldName("channel_id")
    public Id getChannelId() {
        return channelId;
    }

    @Nullable
    @FieldName("schedule_date")
    public LocalDate getScheduleDate() {
        return scheduleDate;
    }

    public void setScheduleDate(LocalDate scheduleDate) {
        this.scheduleDate = scheduleDate;
    }

    @Nullable
    @FieldName("source_id")
    public String getSourceId() {
        return sourceId;
    }

    @Nullable
    @FieldName("actively_published")
    public Boolean isActivelyPublished() {
        return activelyPublished;
    }

    public void setIsActivelyPublished(Boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }

    @Nullable
    @FieldName("repeat")
    public Boolean getRepeat() {
        return repeat;
    }

    public void setRepeat(Boolean repeat) {
        this.repeat = repeat;
    }

    @Nullable
    @FieldName("subtitled")
    public Boolean getSubtitled() {
        return subtitled;
    }

    public void setSubtitled(Boolean subtitled) {
        this.subtitled = subtitled;
    }

    @Nullable
    @FieldName("signed")
    public Boolean getSigned() {
        return signed;
    }

    public void setSigned(Boolean signed) {
        this.signed = signed;
    }

    @Nullable
    @FieldName("audio_described")
    public Boolean getAudioDescribed() {
        return audioDescribed;
    }

    public void setAudioDescribed(Boolean audioDescribed) {
        this.audioDescribed = audioDescribed;
    }

    @Nullable
    @FieldName("high_definition")
    public Boolean getHighDefinition() {
        return highDefinition;
    }

    public void setHighDefinition(Boolean highDefinition) {
        this.highDefinition = highDefinition;
    }

    @Nullable
    @FieldName("widescreen")
    public Boolean getWidescreen() {
        return widescreen;
    }

    public void setWidescreen(Boolean widescreen) {
        this.widescreen = widescreen;
    }

    @Nullable
    @FieldName("surround")
    public Boolean getSurround() {
        return surround;
    }

    public void setSurround(Boolean surround) {
        this.surround = surround;
    }

    @Nullable
    @FieldName("live")
    public Boolean getLive() {
        return live;
    }

    public void setLive(Boolean live) {
        this.live = live;
    }

    @Nullable
    @FieldName("premiere")
    public Boolean getPremiere() {
        return premiere;
    }

    public void setPremiere(Boolean premiere) {
        this.premiere = premiere;
    }

    @Nullable
    @FieldName("continuation")
    public Boolean getContinuation() {
        return continuation;
    }

    public void setContinuation(Boolean continuation) {
        this.continuation = continuation;
    }

    @Nullable
    @FieldName("new_series")
    public Boolean getNewSeries() {
        return newSeries;
    }

    public void setNewSeries(Boolean newSeries) {
        this.newSeries = newSeries;
    }

    @Nullable
    @FieldName("new_episode")
    public Boolean getNewEpisode() {
        return newEpisode;
    }

    public void setNewEpisode(Boolean newEpisode) {
        this.newEpisode = newEpisode;
    }

    @Nullable
    @FieldName("new_one_off")
    public Boolean getNewOneOff() {
        return newOneOff;
    }

    public void setNewOneOff(Boolean newOneOff) {
        this.newOneOff = newOneOff;
    }

    @Nullable
    @FieldName("three_d")
    public Boolean is3d() {
        return is3d;
    }

    public void set3d(Boolean is3d) {
        this.is3d = is3d;
    }

    @Nullable
    @FieldName("version_id")
    public String getVersionId() {
        return versionId;
    }

    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }

    @FieldName("transmission_interval")
    public Interval getTransmissionInterval() {
        return transmissionInterval;
    }

    public Optional<BlackoutRestriction> getBlackoutRestriction() {
        return blackoutRestriction;
    }

    public void setBlackoutRestriction(@Nullable BlackoutRestriction blackoutRestriction) {
        this.blackoutRestriction = Optional.fromNullable(blackoutRestriction);
    }

    @Nullable
    @FieldName("revised_repeat")
    public Boolean getRevisedRepeat() {
        return revisedRepeat;
    }

    public void setRevisedRepeat(Boolean revisedRepeat) {
        this.revisedRepeat = revisedRepeat;
    }

    public boolean isUpcoming() {
        return getTransmissionEndTime().isAfter(DateTime.now(DateTimeZone.UTC));
    }

    @Override
    public int hashCode() {
        // Currently publishers either have ids for all broadcasts or all broadcasts don't have ids
        // (there are no mixes of broadcasts with and without ids) so this hashCode is safe
        if (sourceId != null) {
            return sourceId.hashCode();
        }
        return transmissionInterval.hashCode();
    }

    @Override
    public boolean equals(Object that) {
        if (this == that) {
            return true;
        }
        if (that instanceof Broadcast) {
            Broadcast other = (Broadcast) that;
            if (sourceId != null && other.sourceId != null) {
                return sourceId.equals(other.sourceId);
            }
            return channelId.equals(other.channelId)
                    && transmissionInterval.equals(other.getTransmissionInterval());
        }
        return false;
    }

    @Override
    public String toString() {
        return Objects.toStringHelper(getClass())
                .omitNullValues()
                .addValue(sourceId)
                .add("channel", channelId)
                .add("interval", transmissionInterval)
                .toString();
    }

    public Broadcast withId(String id) {
        this.sourceId = id;
        return this;
    }

    public BroadcastRef toRef() {
        return new BroadcastRef(sourceId, channelId, getTransmissionInterval());
    }

    public Broadcast copy() {
        return copyWithNewInterval(transmissionInterval);
    }

    public Broadcast copyWithNewInterval(Interval transmissionInterval) {
        Broadcast copy = new Broadcast(channelId, transmissionInterval);
        Identified.copyTo(this, copy);
        copy.activelyPublished = activelyPublished;
        copy.sourceId = sourceId;
        copy.scheduleDate = scheduleDate;
        copy.repeat = repeat;
        copy.subtitled = subtitled;
        copy.signed = signed;
        copy.audioDescribed = audioDescribed;
        copy.highDefinition = highDefinition;
        copy.widescreen = widescreen;
        copy.newSeries = newSeries;
        copy.newEpisode = newEpisode;
        copy.newOneOff = newOneOff;
        copy.premiere = premiere;
        copy.continuation = continuation;
        copy.live = live;
        copy.versionId = versionId;
        copy.revisedRepeat = revisedRepeat;
        return copy;
    }

    private static final class BroadcastChannelFilter implements Predicate<Broadcast> {

        private final Channel channel;

        private BroadcastChannelFilter(Channel channel) {
            this.channel = checkNotNull(channel);
        }

        @Override
        public boolean apply(Broadcast input) {
            return input.getChannelId().longValue() == channel.getId().longValue();
        }

        @Override
        public String toString() {
            return "broadcasts on " + channel;
        }
    }

    private static final class BroadcastIntervalFilter implements Predicate<Broadcast> {

        private final Predicate<Interval> scheduleFilter;

        private BroadcastIntervalFilter(Interval interval) {
            scheduleFilter = ScheduleBroadcastFilter.valueOf(interval);
        }

        @Override
        public boolean apply(Broadcast input) {
            return scheduleFilter.apply(input.getTransmissionInterval());
        }

        @Override
        public String toString() {
            return "broadcasts with " + scheduleFilter;
        }
    }

    private static final class BroadcastStartTimeOrdering extends Ordering<Broadcast> {

        @Override
        public int compare(Broadcast left, Broadcast right) {
            return IntervalOrdering.byStartShortestFirst()
                    .compare(left.transmissionInterval, right.transmissionInterval);
        }
    }
}
