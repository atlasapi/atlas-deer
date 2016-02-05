package org.atlasapi.query.v4.schedule;

import java.util.List;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.QueryContext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkNotNull;

public abstract class ScheduleQuery {

    public static ScheduleQuery single(
            Publisher source, Publisher override,
            DateTime start, DateTime end,
            QueryContext context, Id channelId
    ) {
        return new SingleScheduleQuery(source, override, start, end, context, channelId);
    }

    public static ScheduleQuery single(
            Publisher source, Publisher override,
            DateTime start, Integer count,
            QueryContext context, Id channelId
    ) {
        return new SingleScheduleQuery(source, override, start, count, context, channelId);
    }
    
    public static ScheduleQuery multi(
            Publisher source, Publisher override, DateTime start,
            DateTime end, QueryContext context,
            List<Id> channelIds
    ) {
        return new MultiScheduleQuery(source, override, start, end, context, channelIds);
    }

    public static ScheduleQuery multi(
            Publisher source, Publisher override, DateTime start,
            Integer count, QueryContext context,
            List<Id> channelIds
    ) {
        return new MultiScheduleQuery(source, override, start, count, context, channelIds);
    }
    
    private final Publisher source;
    private final DateTime start;
    private final Optional<DateTime> end;
    private final Optional<Integer> count;
    private final QueryContext context;
    private final Optional<Publisher> override;

    public ScheduleQuery(
            Publisher source, Publisher override,
            DateTime start, DateTime end, QueryContext context) {
        this(
                checkNotNull(source),
                Optional.fromNullable(override),
                checkNotNull(start),
                Optional.of(end),
                Optional.absent(),
                checkNotNull(context)
        );
    }

    public ScheduleQuery(
            Publisher source, Publisher override,
            DateTime start, Integer count,
            QueryContext context
    ) {
        this(
                checkNotNull(source),
                Optional.fromNullable(override),
                checkNotNull(start),
                Optional.absent(),
                Optional.of(count),
                checkNotNull(context)
        );
    }

    private ScheduleQuery(
            Publisher source,
            Optional<Publisher> override,
            DateTime start,
            Optional<DateTime> end,
            Optional<Integer> count,
            QueryContext context
    ) {
        this.source = source;
        this.override = override;
        this.start = start;
        this.end = end;
        this.count = count;
        this.context = context;
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

    public Optional<Publisher> getOverride() {
        return override;
    }

    private static final class SingleScheduleQuery extends ScheduleQuery {

        private final Id channelId;

        public SingleScheduleQuery(
                Publisher source, Publisher override,
                DateTime start, DateTime end,
                QueryContext context, Id channelId
        ) {
            super(source, override, start, end, context);
            this.channelId = channelId;
        }

        public SingleScheduleQuery(
                Publisher source, Publisher override,
                DateTime start, Integer count,
                QueryContext context, Id channelId
        ) {
            super(source, override, start, count, context);
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

        public MultiScheduleQuery(
                Publisher source, Publisher override, DateTime start, DateTime end,
                QueryContext context, Iterable<Id> ids
        ) {
            super(source, override, start, end, context);
            this.channelIds = ImmutableSet.copyOf(ids);
        }

        public MultiScheduleQuery(
                Publisher source, Publisher override, DateTime start, Integer count,
                QueryContext context, Iterable<Id> ids
        ) {
            super(source, override, start, count, context);
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
