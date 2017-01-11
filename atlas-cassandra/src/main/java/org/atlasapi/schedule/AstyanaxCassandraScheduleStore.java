package org.atlasapi.schedule;

import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.util.ResolveException;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.CassandraUtil;

import com.metabroadcast.common.base.MorePredicates;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.DateTimeZones;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.base.Joiner;
import com.google.common.base.Predicate;
import com.google.common.base.Splitter;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.ColumnListMutation;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.RowSliceQuery;
import com.netflix.astyanax.serializers.StringSerializer;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import static com.google.common.base.Preconditions.checkNotNull;

/**
 * Cassandra-based implementation of ScheduleStore.
 * <p>
 * Schedules are stored in day-long rows. Each row has a column containing all the currently valid
 * broadcast IDs for that day. The entries for a row are stored one per column, identified by
 * broadcast ID.
 * <p>
 * To update a row the "ids" column is updated and all entries are serialized into their own column.
 * To read a row the "ids" column is de-serialized and used to lookup the relevant columns from the
 * rest of the row. It follows that stale schedule entries remain in their rows but are not read.
 */
public class AstyanaxCassandraScheduleStore extends AbstractScheduleStore {

    private static final String UPDATED_COL = "updated";
    private static final String IDS_COL = "ids";

    public static final Builder builder(AstyanaxContext<Keyspace> context, String name,
            ContentStore contentStore, MessageSender<ScheduleUpdateMessage> messageSender) {
        return new Builder(context, name, contentStore, messageSender);
    }

    public static final class Builder {

        private final AstyanaxContext<Keyspace> context;
        private final String name;
        private final ContentStore contentStore;
        private final MessageSender<ScheduleUpdateMessage> messageSender;

        private ConsistencyLevel readCl = ConsistencyLevel.CL_QUORUM;
        private ConsistencyLevel writeCl = ConsistencyLevel.CL_QUORUM;
        private Clock clock = new SystemClock();
        private MetricRegistry metricRegistry;
        private String metricPrefix;

        public Builder(AstyanaxContext<Keyspace> context, String name, ContentStore contentStore,
                MessageSender<ScheduleUpdateMessage> messageSender) {
            this.context = checkNotNull(context);
            this.name = checkNotNull(name);
            this.contentStore = checkNotNull(contentStore);
            this.messageSender = checkNotNull(messageSender);
        }

        public Builder withReadConsistency(ConsistencyLevel readLevel) {
            this.readCl = checkNotNull(readLevel);
            return this;
        }

        public Builder withWriteConsistency(ConsistencyLevel writeLevel) {
            this.writeCl = checkNotNull(writeLevel);
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = checkNotNull(clock);
            return this;
        }

        public Builder withMetricRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = checkNotNull(metricRegistry);
            return this;
        }

        public Builder withMetricPrefix(String metricPrefix) {
            this.metricPrefix = checkNotNull(metricPrefix);
            return this;
        }

        public AstyanaxCassandraScheduleStore build() {
            return new AstyanaxCassandraScheduleStore(
                    context,
                    name,
                    contentStore,
                    messageSender,
                    clock,
                    readCl,
                    writeCl,
                    metricRegistry,
                    metricPrefix
            );
        }

    }

    private final Keyspace keyspace;
    private final ColumnFamily<String, String> cf;
    private final Clock clock;
    private final ConsistencyLevel readCl;
    private final ConsistencyLevel writeCl;

    private final ItemAndBroadcastSerializer serializer;
    private final Function<ItemAndBroadcast, String> toBroadacstId = Functions.compose(
            new Function<Broadcast, String>() {

                @Override
                public String apply(Broadcast input) {
                    return input.getSourceId();
                }
            }, ItemAndBroadcast.toBroadcast());

    private AstyanaxCassandraScheduleStore(
            AstyanaxContext<Keyspace> context,
            String name,
            ContentStore contentStore,
            MessageSender<ScheduleUpdateMessage> messageSender,
            Clock clock,
            ConsistencyLevel readCl,
            ConsistencyLevel writeCl,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(contentStore, messageSender, metricRegistry, metricPrefix);
        this.serializer = new ItemAndBroadcastSerializer(new ContentSerializer(new ContentSerializationVisitor()));
        this.keyspace = context.getClient();
        this.cf = ColumnFamily.newColumnFamily(
                name,
                StringSerializer.get(),
                StringSerializer.get()
        );
        this.clock = clock;
        this.readCl = readCl;
        this.writeCl = writeCl;
    }

    @Override
    public ListenableFuture<Schedule> resolve(Iterable<Channel> channels,
            Interval interval, Publisher source) {
        List<Channel> chans = ImmutableList.copyOf(channels);
        checkNotNull(interval);
        checkNotNull(source);
        return resolveSchedule(source, chans, interval);
    }

    /*
     * To resolve we create a (multi-)mapping of Channel to row keys for the
     * given interval and source. Each of the keys is retrieved and de-serialized
     * before being sorted in to ChannelSchedules.
     */
    private ListenableFuture<Schedule> resolveSchedule(Publisher source, List<Channel> chans,
            Interval interval) {
        Multimap<Channel, String> channelKeys = keys(chans, interval, source);
        return Futures.transform(
                scheduleRows(channelKeys.values()),
                toChannelSchedules(channelKeys, interval)
        );
    }

    //Expect a maximum of 2 keys per channel as each row is a day and the max interval is one day.
    private Multimap<Channel, String> keys(List<Channel> channels, Interval interval,
            Publisher source) {
        HashMultimap<Channel, String> keys = HashMultimap.create(channels.size(), 2);
        for (final Channel channel : channels) {
            keys.putAll(channel, rowKeys(channel, interval, source));
        }
        return keys;
    }

    private Function<Rows<String, String>, Schedule> toChannelSchedules(
            final Multimap<Channel, String> channelKeys, final Interval interval) {
        return new Function<Rows<String, String>, Schedule>() {

            @Override
            public Schedule apply(Rows<String, String> input) {
                return transformRowsToChannelSchedules(channelKeys, interval, input);
            }
        };
    }

    // Transform the rows retrieved to channel schedules using the keys in the
    // multimap. Some keys may not have an associated row. The rows may have
    // superfluous entries before and/or after the requested interval so they
    // have to be filtered.
    private Schedule transformRowsToChannelSchedules(Multimap<Channel, String> channelKeys,
            final Interval interval, final Rows<String, String> rows) {
        ImmutableList.Builder<ChannelSchedule> schedules = ImmutableList.builder();
        Predicate<Broadcast> filter = Broadcast.intervalFilter(interval);
        for (Entry<Channel, Collection<String>> channelAndKeys : channelKeys.asMap().entrySet()) {
            Channel channel = channelAndKeys.getKey();
            Iterable<ItemAndBroadcast> entries = entries(rows, channelAndKeys.getValue());
            schedules.add(new ChannelSchedule(
                    channel,
                    interval,
                    ImmutableSet.copyOf(trim(filter, entries))
            ));
        }
        return new Schedule(schedules.build(), interval);
    }

    private Iterable<ItemAndBroadcast> trim(final Predicate<Broadcast> filter,
            Iterable<ItemAndBroadcast> entries) {
        return Iterables.filter(
                entries,
                MorePredicates.transformingPredicate(ItemAndBroadcast.toBroadcast(), filter)
        );
    }

    private Iterable<ItemAndBroadcast> entries(final Rows<String, String> rows,
            Collection<String> keys) {
        return Iterables.concat(Iterables.transform(
                keys,
                new Function<String, Iterable<ItemAndBroadcast>>() {

                    @Override
                    public Iterable<ItemAndBroadcast> apply(String key) {
                        return deserialize(rows.getRow(key));
                    }
                }
        ));
    }

    private Iterable<ItemAndBroadcast> deserialize(Row<String, String> row) {
        if (row == null) {
            return ImmutableList.of();
        }
        return deserialize(row.getColumns());
    }

    private Iterable<ItemAndBroadcast> deserialize(ColumnList<String> columns) {
        Column<String> ids = columns.getColumnByName(IDS_COL);
        if (ids == null) {
            return ImmutableList.of();
        }
        ArrayList<ItemAndBroadcast> iabs = Lists.newArrayListWithCapacity(columns.size());
        for (String id : Splitter.on(',').omitEmptyStrings().split(ids.getStringValue())) {
            Column<String> column = columns.getColumnByName(id);
            iabs.add(serializer.deserialize(column.getByteArrayValue()));
        }
        return iabs;
    }

    private ListenableFuture<Rows<String, String>> scheduleRows(Iterable<String> keys) {
        try {
            RowSliceQuery<String, String> query = keyspace
                    .prepareQuery(cf)
                    .setConsistencyLevel(readCl)
                    .getKeySlice(keys);
            return Futures.transform(
                    query.executeAsync(),
                    CassandraUtil.<Rows<String, String>>toResult()
            );
        } catch (ConnectionException ce) {
            return Futures.immediateFailedCheckedFuture(new ResolveException(ce));
        }
    }

    @Override
    protected void doWrite(Publisher source, List<ChannelSchedule> blocks) throws WriteException {
        try {
            MutationBatch batch = keyspace.prepareMutationBatch().withConsistencyLevel(writeCl);
            for (ChannelSchedule block : blocks) {
                ColumnListMutation<String> rowMutation = batch.withRow(cf, key(source, block));
                for (ItemAndBroadcast entry : block.getEntries()) {
                    rowMutation.putColumn(
                            entry.getBroadcast().getSourceId(),
                            serializer.serialize(entry)
                    );
                }
                rowMutation.putColumn(IDS_COL, Joiner.on(',').join(broadcastIds(block)));
                rowMutation.putColumn(UPDATED_COL, clock.now().toDate());
            }
            try {
                batch.execute();
            } catch (ConnectionException ce) {
                throw new WriteException(ce);
            }
        } catch (WriteException | RuntimeException e) {
            throw e;
        }
    }

    private Iterable<String> broadcastIds(ChannelSchedule block) {
        return Iterables.transform(block.getEntries(), toBroadacstId);
    }

    @Override
    protected List<ChannelSchedule> resolveCurrentScheduleBlocks(Publisher source, Channel channel,
            Interval interval) throws WriteException {
        Rows<String, String> rows = fetchRows(source, channel, interval);
        List<ChannelSchedule> channelSchedules = Lists.newArrayList();
        for (LocalDate date : new ScheduleIntervalDates(interval)) {
            DateTime start = date.toDateTimeAtStartOfDay(DateTimeZones.UTC);
            Interval dayInterval = new Interval(start, start.plusDays(1));
            channelSchedules.add(schedule(
                    channel,
                    dayInterval,
                    rows.getRow(keyFor(source, channel.getId().longValue(), date))
            ));
        }
        return channelSchedules;
    }

    @Override
    protected List<ChannelSchedule> resolveStaleScheduleBlocks(Publisher source, Channel channel,
            Interval interval) throws WriteException {
        Rows<String, String> rows = fetchRows(source, channel, interval);
        List<ChannelSchedule> channelSchedules = Lists.newArrayList();
        for (LocalDate date : new ScheduleIntervalDates(interval)) {
            channelSchedules.add(
                    new ChannelSchedule(
                            channel,
                            interval,
                            pastSchedule(
                                    rows.getRow(
                                            keyFor(source, channel.getId().longValue(), date)
                                    )
                            )
                    )
            );
        }
        return channelSchedules;
    }

    private Iterable<ItemAndBroadcast> pastSchedule(Row<String, String> row) {
        return deserializePastBlocks(row);

    }

    private Iterable<ItemAndBroadcast> deserializePastBlocks(Row<String, String> row) {
        if (row == null) {
            return ImmutableList.of();
        }
        ColumnList<String> columns = row.getColumns();
        Column<String> idColumn = columns.getColumnByName(IDS_COL);
        Set<String> ids;
        if (idColumn == null) {
            ids = ImmutableSet.of();
        } else {
            ids = ImmutableSet.copyOf(Splitter.on(',')
                    .omitEmptyStrings()
                    .split(idColumn.getStringValue()));
        }

        ArrayList<ItemAndBroadcast> iabs = Lists.newArrayListWithCapacity(columns.size());
        for (Column<String> column : columns) {
            if (IDS_COL.equals(column.getName())
                    || UPDATED_COL.equals(column.getName())
                    || ids.contains(column.getName())) {
                continue;
            }
            iabs.add(serializer.deserialize(column.getByteArrayValue()));
        }

        return iabs;
    }

    private Rows<String, String> fetchRows(Publisher source, Channel channel, Interval interval)
            throws WriteException {
        Iterable<String> keys = rowKeys(channel, interval, source);
        int timeout = 1;
        TimeUnit units = TimeUnit.MINUTES;
        try {
            return Futures.get(scheduleRows(keys), timeout, units, WriteException.class);
        } catch (WriteException we) {
            String msg = String.format("failed to read %s in %s %s", keys, timeout, units);
            throw new WriteException(msg, we.getCause());
        }
    }

    private ChannelSchedule schedule(Channel channel, Interval interval, Row<String, String> row) {
        return new ChannelSchedule(channel, interval, deserialize(row));
    }

    private String key(Publisher source, ChannelSchedule block) {
        return keyFor(
                source,
                block.getChannel().getId().longValue(),
                block.getInterval().getStart().toLocalDate()
        );
    }

    private String keyFor(Publisher source, Long channelId, LocalDate day) {
        return String.format("%s-%s-%s", source.key(), channelId, day.toString());
    }

    private Iterable<String> rowKeys(final Channel channel, Interval interval,
            final Publisher source) {
        return Iterables.transform(
                new ScheduleIntervalDates(interval),
                new Function<LocalDate, String>() {

                    @Override
                    public String apply(LocalDate input) {
                        return keyFor(source, channel.getId().longValue(), input);
                    }
                }
        );
    }

}
