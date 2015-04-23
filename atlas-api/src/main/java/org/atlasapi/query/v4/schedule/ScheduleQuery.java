package org.atlasapi.query.v4.schedule;

import static com.google.common.base.Preconditions.checkNotNull;

import java.util.List;

import com.google.common.base.Optional;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.QueryContext;
import org.joda.time.DateTime;
import org.joda.time.Interval;

import com.google.common.collect.ImmutableSet;

public abstract class ScheduleQuery {
    
    public static final ScheduleQuery single(Publisher source, DateTime start, DateTime end, QueryContext context, Id channelId) {
        return new SingleScheduleQuery(source, start, end, context, channelId);
    }

    public static final ScheduleQuery single(Publisher source, DateTime start, Integer count, QueryContext context, Id channelId) {
        return new SingleScheduleQuery(source, start, count, context, channelId);
    }
    
    public static final ScheduleQuery multi(Publisher source, DateTime start, DateTime end, QueryContext context, List<Id> channelIds) {
        return new MultiScheduleQuery(source, start, end, context, channelIds);
    }

    public static final ScheduleQuery multi(Publisher source, DateTime start, Integer count, QueryContext context, List<Id> channelIds) {
        return new MultiScheduleQuery(source, start, count, context, channelIds);
    }
    
    private final Publisher source;
    private final DateTime start;
    private final Optional<DateTime> end;
    private final Optional<Integer> count;
    private final QueryContext context;

    public ScheduleQuery(Publisher source, DateTime start, DateTime end, QueryContext context) {
        this.source = checkNotNull(source);
        this.start = checkNotNull(start);
        this.end = Optional.of(end);
        this.count = Optional.absent();
        this.context = checkNotNull(context);
    }

    public ScheduleQuery(Publisher source, DateTime start, Integer count, QueryContext context) {
        this.source = checkNotNull(source);
        this.start = checkNotNull(start);
        this.end = Optional.absent();
        this.count = Optional.of(count);
        this.context = checkNotNull(context);
    }
    
    public abstract boolean isMultiChannel();
    
    public abstract Id getChannelId();
    
    public abstract ImmutableSet<Id> getChannelIds();
    
    public Publisher getSource() {
        return source;
    }
        
    public QueryContext getContext() {
        return this.context;
    }

    public DateTime getStart() {
        return start;
    }

    public Optional<DateTime> getEnd() {
        return end;
    }

    public Optional<Integer> getCount() {
        return count;
    }

    private static final class SingleScheduleQuery extends ScheduleQuery {

        private final Id channelId;

        public SingleScheduleQuery(Publisher source, DateTime start, DateTime end, QueryContext context, Id channelId) {
            super(source, start, end, context);
            this.channelId = channelId;
        }

        public SingleScheduleQuery(Publisher source, DateTime start, Integer count, QueryContext context, Id channelId) {
            super(source, start, count, context);
            this.channelId = channelId;
        }
        
        @Override
        public boolean isMultiChannel() {
            return false;
        }

        @Override
        public Id getChannelId() {
            return channelId;
        }

        @Override
        public ImmutableSet<Id> getChannelIds() {
            throw new IllegalStateException("Can't call ScheduleQuery.getChannelIds() on single query");
        }
        
    }

    private static final class MultiScheduleQuery extends ScheduleQuery {

        private final ImmutableSet<Id> channelIds;

        public MultiScheduleQuery(Publisher source, DateTime start, DateTime end, QueryContext context, List<Id> ids) {
            super(source, start, end, context);
            this.channelIds = ImmutableSet.copyOf(ids);
        }

        public MultiScheduleQuery(Publisher source, DateTime start, Integer count, QueryContext context, List<Id> ids) {
            super(source, start, count, context);
            this.channelIds = ImmutableSet.copyOf(ids);
        }

        @Override
        public boolean isMultiChannel() {
            return true;
        }

        @Override
        public Id getChannelId() {
            throw new IllegalStateException("Can't call ScheduleQuery.getChannelId() on multi query");
        }

        @Override
        public ImmutableSet<Id> getChannelIds() {
            return channelIds;
        }
        
    }
    
}
