/* Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import static com.google.common.base.Preconditions.checkNotNull;

import com.google.common.base.Optional;
import org.atlasapi.channel.Channel;
import org.atlasapi.entity.Id;
import org.atlasapi.meta.annotations.FieldName;
import org.atlasapi.schedule.ScheduleBroadcastFilter;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.base.Predicate;
import com.google.common.collect.Ordering;
import com.metabroadcast.common.time.IntervalOrdering;

/**
 * A time and channel at which a Version is/was receivable.
 * 
 * @author Robert Chatley (robert@metabroadcast.com)
 */
public class Broadcast extends Identified {
    
    private final Id channelId;
    private final Interval transmissionInterval;
    private final Duration broadcastDuration;
    
    private LocalDate scheduleDate;
    private Boolean activelyPublished;
    
    //Should probably be called sourceAlias.
    private String sourceId;  

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
    private Boolean premiere;
    private Boolean is3d;
    private Optional<BlackoutRestriction> blackoutRestriction = Optional.absent();
    
    public Broadcast(Id channelId, DateTime start, DateTime end, Boolean activelyPublished) {
		this(channelId, new Interval(start, end), activelyPublished);
	}
    
    public Broadcast(Id channelId, DateTime start, DateTime end) {
        this(channelId, start, end, true);
    }
    
    public Broadcast(Id channelId, DateTime start, Duration duration) {
        this(channelId, start, duration, true);
    }

    public Broadcast(Id channelId, Interval interval) {
        this(channelId, interval, true);
    }
    
    public Broadcast(Id channelId, DateTime start, Duration duration, Boolean activelyPublished) {
		this(channelId, new Interval(start, start.plus(duration)), activelyPublished);
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
    
    public Broadcast(Channel channel, DateTime start, Duration duration, Boolean activelyPublished) {
        this(channel.getId(), new Interval(start, start.plus(duration)), activelyPublished);
    }
    
    public Broadcast(Id channelId, Interval interval, Boolean activelyPublished) {
        this.channelId = checkNotNull(channelId);
        this.transmissionInterval = checkNotNull(interval);
        this.broadcastDuration = transmissionInterval.toDuration();
        this.activelyPublished = activelyPublished;
    }

    @FieldName("transmission_time")
    public DateTime getTransmissionTime() {
        return transmissionInterval.getStart();
    }

    @FieldName("transmission_end_time")
    public DateTime getTransmissionEndTime() {
		return transmissionInterval.getEnd();
	}
    
    @FieldName("transmission_interval")
    public Interval getTransmissionInterval() {
        return transmissionInterval;
    }

    @FieldName("broadcast_duration")
    public Duration getBroadcastDuration() {
        return this.broadcastDuration;
    }

    @FieldName("channel_id")
    public Id getChannelId() {
        return channelId;
    }

    @FieldName("schedule_date")
    public LocalDate getScheduleDate() {
        return scheduleDate;
    }
    
    @FieldName("source_id")
    public String getSourceId() {
        return sourceId;
    }

    public void setScheduleDate(LocalDate scheduleDate) {
        this.scheduleDate = scheduleDate;
    }
    
    public Broadcast withId(String id) {
        this.sourceId = id;
        return this;
    }
    
    @FieldName("actively_published")
    public Boolean isActivelyPublished() {
        return activelyPublished;
    }
    
    public void setIsActivelyPublished(Boolean activelyPublished) {
        this.activelyPublished = activelyPublished;
    }
    
    @FieldName("repeat")
    public Boolean getRepeat() {
        return repeat;
    }
    
    public void setRepeat(Boolean repeat) {
        this.repeat = repeat;
    }
    
    public void setSubtitled(Boolean subtitled) {
        this.subtitled = subtitled;
    }

    @FieldName("subtitled")
    public Boolean getSubtitled() {
        return subtitled;
    }

    public void setSigned(Boolean signed) {
        this.signed = signed;
    }

    @FieldName("signed")
    public Boolean getSigned() {
        return signed;
    }

    public void setAudioDescribed(Boolean audioDescribed) {
        this.audioDescribed = audioDescribed;
    }

    @FieldName("audio_described")
    public Boolean getAudioDescribed() {
        return audioDescribed;
    }

    public void setHighDefinition(Boolean highDefinition) {
        this.highDefinition = highDefinition;
    }

    @FieldName("high_definition")
    public Boolean getHighDefinition() {
        return highDefinition;
    }

    public void setWidescreen(Boolean widescreen) {
        this.widescreen = widescreen;
    }

    @FieldName("widescreen")
    public Boolean getWidescreen() {
        return widescreen;
    }
    
    public void setSurround(Boolean surround) {
        this.surround = surround;
    }

    @FieldName("surround")
    public Boolean getSurround() {
        return surround;
    }

    public void setLive(Boolean live) {
        this.live = live;
    }

    @FieldName("live")
    public Boolean getLive() {
        return live;
    }
    
    public void setPremiere(Boolean premiere) {
        this.premiere = premiere;
    }

    @FieldName("premiere")
    public Boolean getPremiere() {
        return premiere;
    }
    
    public void setNewSeries(Boolean newSeries) {
        this.newSeries = newSeries;
    }
    
    @FieldName("new_series")
    public Boolean getNewSeries() {
        return newSeries;
    }
    
    public void setNewEpisode(Boolean newEpisode) {
        this.newEpisode = newEpisode;
    }
    
    @FieldName("new_episode")
    public Boolean getNewEpisode() {
        return newEpisode;
    }
    
    @FieldName("three_d")
    public Boolean is3d() {
        return is3d;
    }
    
    public void set3d(Boolean is3d) {
        this.is3d = is3d;
    }
    
    @FieldName("version_id")
    public String getVersionId() {
        return versionId;
    }
    
    public void setVersionId(String versionId) {
        this.versionId = versionId;
    }
    
    public BroadcastRef toRef() {
        return new BroadcastRef(sourceId, channelId, getTransmissionInterval());
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
    public int hashCode() {
        // Currently publishers either have ids for all broadcasts or all broadcasts don't have ids 
        // (there are no mixes of broadcasts with and without ids) so this hashCode is safe
        if (sourceId != null) {
            return sourceId.hashCode();
        }
        return transmissionInterval.hashCode();
    }
    
    public Broadcast copy() {
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
        copy.premiere = premiere;
        copy.live = live;
        copy.versionId = versionId;
        return copy;
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
    
    public final static Function<Broadcast, Broadcast> COPY = new Function<Broadcast, Broadcast>() {
        @Override
        public Broadcast apply(Broadcast input) {
            return input.copy();
        }
    };

    public static final Predicate<Broadcast> IS_REPEAT = new Predicate<Broadcast>() {
        @Override
        public boolean apply(Broadcast input) {
            return input.repeat != null && input.repeat;
        }
    };
    
    public static final Function<Broadcast, DateTime> TO_TRANSMISSION_TIME = new Function<Broadcast, DateTime>() {
        @Override
        public DateTime apply(Broadcast input) {
            return input.getTransmissionTime();
        }
    };

    public static final Predicate<Broadcast> ACTIVELY_PUBLISHED = new Predicate<Broadcast>() {
        @Override
        public boolean apply(Broadcast input) {
            return input.isActivelyPublished();
        }
    };
    
    public static final Predicate<Broadcast> channelFilter(final Channel channel) {
        return new BroadcastChannelFilter(channel);
    }

    public void setBlackoutRestriction(BlackoutRestriction blackoutRestriction) {
        this.blackoutRestriction = Optional.fromNullable(blackoutRestriction);
    }

    public Optional<BlackoutRestriction> getBlackoutRestriction() {
        return blackoutRestriction;
    }

    private static final class BroadcastChannelFilter implements Predicate<Broadcast> {

        private final Channel channel;

        private BroadcastChannelFilter(Channel channel) {
            this.channel = checkNotNull(channel);
        }

        @Override
        public boolean apply(Broadcast input) {
            return input.getChannelId() != null 
                && input.getChannelId().longValue() == channel.getId().longValue();
        }
        
        @Override
        public String toString() {
            return "broadcasts on " + channel;
        }
    }

    public static final Predicate<Broadcast> intervalFilter(final Interval interval) {
        return new BroadcastIntervalFilter(interval);
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
    
    private static final Ordering<Broadcast> START_TIME_ORDERING = new BroadcastStartTimeOrdering();

    public static final Ordering<Broadcast> startTimeOrdering() {
        return START_TIME_ORDERING;
    }
}
