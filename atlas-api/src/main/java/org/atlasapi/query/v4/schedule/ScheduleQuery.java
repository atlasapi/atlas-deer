package org.atlasapi.query.v4.schedule;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.query.common.QueryContext;

import com.google.common.base.Optional;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.joda.time.DateTime;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ScheduleQuery {

    public static Builder builder() {
        return new Builder();
    }

    private final Publisher source;
    private final DateTime start;
    private final Optional<DateTime> end;
    private final Optional<Integer> count;
    private final QueryContext context;
    private final Optional<Publisher> override;
    private final ImmutableSet<Id> ids;

    private ScheduleQuery(
            Publisher source,
            DateTime start,
            Optional<DateTime> end,
            Optional<Integer> count,
            QueryContext context,
            Optional<Publisher> override,
            Iterable<Id> ids
    ) {
        this.source = checkNotNull(source);
        this.start = checkNotNull(start);
        this.end = checkNotNull(end);
        this.count = checkNotNull(count);
        this.context = checkNotNull(context);
        this.override = checkNotNull(override);
        checkArgument(!Iterables.isEmpty(ids), "IDs must not be empty");
        this.ids = ImmutableSet.copyOf(ids);
    }

    public boolean isMultiChannel() {
        return ids.size() > 1;
    }

    public Id getChannelId() {
        if (isMultiChannel()) {
            throw new IllegalStateException(
                    "Can't call ScheduleQuery.getChannelId() on multi query");
        }

        return Iterables.getOnlyElement(ids);
    }
    
    public ImmutableSet<Id> getChannelIds() {
        if (!isMultiChannel()) {
            throw new IllegalStateException(
                    "Can't call ScheduleQuery.getChannelIds() on single query");
        }

        return ids;
    }
    
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

    public static class Builder {
        private Publisher source;
        private DateTime start;
        private Optional<DateTime> end = Optional.absent();
        private Optional<Integer> count = Optional.absent();
        private QueryContext context;
        private Optional<Publisher> override = Optional.absent();
        private Iterable<Id> ids;

        public Builder withSource(Publisher source) {
            this.source = source;
            return this;
        }

        public Builder withStart(DateTime start) {
            this.start = start;
            return this;
        }

        public Builder withEnd(DateTime end) {
            this.end = Optional.fromNullable(end);
            return this;
        }

        public Builder withCount(Integer count) {
            this.count = Optional.fromNullable(count);
            return this;
        }

        public Builder withContext(QueryContext context) {
            this.context = context;
            return this;
        }

        public Builder withOverride(Publisher override) {
            this.override = Optional.fromNullable(override);
            return this;
        }

        public Builder withId(Id id) {
            this.ids = ImmutableSet.of(id);
            return this;
        }

        public Builder withIds(Iterable<Id> ids) {
            this.ids = ids;
            return this;
        }

        public ScheduleQuery build() {
            return new ScheduleQuery(source, start, end, count, context, override, ids);
        }
    }
}
