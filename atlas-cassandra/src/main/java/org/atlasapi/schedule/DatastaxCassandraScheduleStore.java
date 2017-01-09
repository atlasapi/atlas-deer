package org.atlasapi.schedule;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ItemAndBroadcast;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;

import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Predicate;
import com.google.common.base.Throwables;
import com.google.common.collect.ArrayListMultimap;
import com.google.common.collect.HashMultimap;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.joda.time.DateTimeZone;
import org.joda.time.Interval;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.putAll;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatastaxCassandraScheduleStore extends AbstractScheduleStore {

    private static final String SOURCE_COLUMN = "source";
    private static final String CHANNEL_COLUMN = "channel";
    private static final String DAY_COLUMN = "day";
    private static final String BROADCAST_IDS_COLUMN = "broadcast_ids";
    private static final String BROADCASTS_COLUMN = "broadcasts";
    private static final String UPDATED_COLUMN = "updated";

    private final String table;
    private final Clock clock;
    private final ConsistencyLevel readCl;
    private final ConsistencyLevel writeCl;
    private final Session session;
    private final Integer timeoutSeconds;

    private final ItemAndBroadcastSerializer serializer;
    private final PreparedStatement scheduleUpdate;
    private final PreparedStatement scheduleSelect;

    public DatastaxCassandraScheduleStore(
            String table,
            ContentStore contentStore,
            MessageSender<ScheduleUpdateMessage> sender,
            Clock clock,
            ConsistencyLevel readCl,
            ConsistencyLevel writeCl,
            Session session,
            ItemAndBroadcastSerializer serializer,
            Integer timeoutSeconds,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(contentStore, sender, metricRegistry, metricPrefix);
        this.table = checkNotNull(table);
        this.clock = checkNotNull(clock);
        this.readCl = checkNotNull(readCl);
        this.writeCl = checkNotNull(writeCl);
        this.session = checkNotNull(session);
        this.serializer = checkNotNull(serializer);
        this.timeoutSeconds = checkNotNull(timeoutSeconds);

        this.scheduleUpdate = session.prepare(update(table)
                .where(eq(SOURCE_COLUMN, bindMarker("source")))
                .and(eq(CHANNEL_COLUMN, bindMarker("channel")))
                .and(eq(DAY_COLUMN, bindMarker("day")))
                .with(putAll(BROADCASTS_COLUMN, bindMarker("broadcastsData")))
                .and(set(BROADCAST_IDS_COLUMN, bindMarker("broadcastsIdsData")))
                .and(set(UPDATED_COLUMN, bindMarker("updatedData"))));
        this.scheduleUpdate.setConsistencyLevel(writeCl);

        this.scheduleSelect = session.prepare(select(
                SOURCE_COLUMN,
                CHANNEL_COLUMN,
                DAY_COLUMN,
                BROADCAST_IDS_COLUMN,
                BROADCASTS_COLUMN,
                UPDATED_COLUMN
        )
                .from(table)
                .where(eq(SOURCE_COLUMN, bindMarker("source")))
                .and(eq(CHANNEL_COLUMN, bindMarker("channel")))
                .and(eq(DAY_COLUMN, bindMarker("day"))));
        this.scheduleSelect.setConsistencyLevel(readCl);
    }

    @Override
    protected List<ChannelSchedule> resolveCurrentScheduleBlocks(Publisher source, Channel channel,
            Interval interval) throws WriteException {
        return Futures.get(
                resolveAndProcess(
                        ImmutableList.of(channel),
                        interval,
                        source,
                        (List<Optional<Row>> input) -> rowsToCurrentScheduleBlocks(
                                input,
                                channel,
                                interval
                        )
                ), timeoutSeconds, TimeUnit.SECONDS,
                WriteException.class
        );
    }

    @Override
    protected List<ChannelSchedule> resolveStaleScheduleBlocks(Publisher source, Channel channel,
            Interval interval) throws WriteException {
        return Futures.get(
                resolveAndProcess(
                        ImmutableList.of(channel),
                        interval,
                        source,
                        (List<Optional<Row>> input) -> rowsToPastScheduleBlocks(
                                input,
                                channel,
                                interval
                        )
                ), timeoutSeconds, TimeUnit.SECONDS,
                WriteException.class
        );
    }

    @Override
    protected void doWrite(Publisher source, List<ChannelSchedule> blocks) {
        try {
            if (blocks.isEmpty()) {
                return;
            }
            BatchStatement batch = new BatchStatement();
            batch.setConsistencyLevel(writeCl);
            for (ChannelSchedule block : blocks) {
                Long channelId = block.getChannel().getId().longValue();
                Map<String, ByteBuffer> broadcasts = block.getEntries().stream()
                        .collect(
                                Collectors.toMap(
                                        key -> key.getBroadcast().getSourceId(),
                                        value -> ByteBuffer.wrap(serializer.serialize(value))
                                )
                        );

                batch.add(scheduleUpdate.bind()
                        .setString("source", source.key())
                        .setLong("channel", channelId)
                        .setTimestamp(
                                "day",
                                block.getInterval().getStart().toDate()
                        )
                        .setMap("broadcastsData", broadcasts)
                        .setSet("broadcastsIdsData", broadcasts.keySet())
                        .setTimestamp("updatedData", clock.now().toDate()));
            }
            session.execute(batch);
        } catch (Exception e) {
            Throwables.propagate(e);
        }
    }

    @Override
    public ListenableFuture<Schedule> resolve(Iterable<Channel> channels, Interval interval,
            Publisher source) {
        return resolveAndProcess(
                channels,
                interval,
                source,
                (List<Optional<Row>> input) -> rowsToSchedule(input, channels, interval)
        );
    }

    private <T> ListenableFuture<T> resolveAndProcess(
            Iterable<Channel> channels,
            Interval interval,
            Publisher source,
            Function<List<Optional<Row>>, T> transformation
    ) {
        Set<Date> datesToResolve = ImmutableSet.copyOf(
                Iterables.transform(
                        new ScheduleIntervalDates(interval),
                        ld -> ld.toDateTimeAtStartOfDay(DateTimeZone.UTC).toDate()

                )
        );
        ImmutableList.Builder<Statement> selects = ImmutableList.builder();
        for (Channel channel : channels) {
            for (Date date : datesToResolve) {
                selects.add(scheduleSelect.bind()
                        .setString("source", source.key())
                        .setLong("channel", channel.getId().longValue())
                        .setTimestamp("day", date));
            }
        }

        ListenableFuture<List<Optional<Row>>> resultFutures = Futures.allAsList(selects.build()
                .stream()
                .map(
                        s -> Futures.transform(
                                session.executeAsync(s),
                                (ResultSet rs) -> Optional.ofNullable(rs.one())
                        )
                )
                .collect(Collectors.toList())
        );
        return Futures.transform(
                resultFutures,
                transformation
        );
    }

    private Schedule rowsToSchedule(List<Optional<Row>> rows, Iterable<Channel> channels,
            Interval interval) {
        Multimap<Channel, ItemAndBroadcast> multimap = HashMultimap.create();
        Map<Id, Channel> channelsMap = StreamSupport.stream(channels.spliterator(), false)
                .collect(Collectors.toMap(Identified::getId, ch -> ch));
        Predicate<Broadcast> filter = Broadcast.intervalFilter(interval);
        List<Row> existingRows = rows.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        for (Row row : existingRows) {
            Id channelId = Id.valueOf(row.getLong(CHANNEL_COLUMN));
            Channel channel = channelsMap.get(channelId);
            Set<String> broadcastIds = row.getSet(BROADCAST_IDS_COLUMN, String.class);
            Map<String, ByteBuffer> broadcasts = row.getMap(
                    BROADCASTS_COLUMN,
                    String.class,
                    ByteBuffer.class
            );
            for (String broadcastId : broadcastIds) {
                ItemAndBroadcast itemAndBroadcast = deserialize(broadcasts.get(broadcastId));
                if (filter.apply(itemAndBroadcast.getBroadcast())) {
                    multimap.put(channel, itemAndBroadcast);
                }
            }
        }
        HashMap<Channel, Collection<ItemAndBroadcast>> channelSchedules = Maps.newHashMap(multimap.asMap());
        for (Channel channel : channels) {
            if (!channelSchedules.containsKey(channel)) {
                channelSchedules.put(channel, ImmutableList.of());
            }
        }

        return Schedule.fromChannelMap(channelSchedules, interval);
    }

    private List<ChannelSchedule> rowsToCurrentScheduleBlocks(List<Optional<Row>> rows,
            Channel channel, Interval interval) {

        Predicate<Broadcast> filter = Broadcast.intervalFilter(interval);
        List<Row> existingRows = rows.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        Multimap<Date, ItemAndBroadcast> scheduleBroadcasts = ArrayListMultimap.create();
        for (Row row : existingRows) {
            Set<String> broadcastIds = row.getSet(BROADCAST_IDS_COLUMN, String.class);
            Map<String, ByteBuffer> broadcasts = row.getMap(
                    BROADCASTS_COLUMN,
                    String.class,
                    ByteBuffer.class
            );
            Date scheduleDate = row.getTimestamp(DAY_COLUMN);
            for (String broadcastId : broadcastIds) {
                ItemAndBroadcast itemAndBroadcast = deserialize(broadcasts.get(broadcastId));
                if (filter.apply(itemAndBroadcast.getBroadcast())) {
                    scheduleBroadcasts.put(scheduleDate, itemAndBroadcast);
                }
            }
        }
        return StreamSupport.stream(new ScheduleIntervalDates(interval).spliterator(), false)
                .map(date -> new ChannelSchedule(
                        channel,
                        new Interval(
                                date.toDateTimeAtStartOfDay(DateTimeZone.UTC),
                                date.plusDays(1).toDateTimeAtStartOfDay(DateTimeZone.UTC)
                        ),
                        scheduleBroadcasts.get(date.toDateTimeAtStartOfDay(DateTimeZone.UTC)
                                .toDate())
                ))
                .collect(Collectors.toList());
    }

    private List<ChannelSchedule> rowsToPastScheduleBlocks(List<Optional<Row>> rows,
            Channel channel, Interval interval) {

        Predicate<Broadcast> filter = Broadcast.intervalFilter(interval);
        List<Row> existingRows = rows.stream()
                .filter(Optional::isPresent)
                .map(Optional::get)
                .collect(Collectors.toList());
        Multimap<Date, ItemAndBroadcast> scheduleBroadcasts = ArrayListMultimap.create();
        for (Row row : existingRows) {
            Set<String> broadcastIds = row.getSet(BROADCAST_IDS_COLUMN, String.class);
            Map<String, ByteBuffer> broadcasts = row.getMap(
                    BROADCASTS_COLUMN,
                    String.class,
                    ByteBuffer.class
            );
            Date scheduleDate = row.getTimestamp(DAY_COLUMN);
            for (Map.Entry<String, ByteBuffer> broadcastsEntry : broadcasts.entrySet()) {
                if (broadcastIds.contains(broadcastsEntry.getKey())) {
                    continue;
                }
                ItemAndBroadcast itemAndBroadcast = deserialize(broadcastsEntry.getValue());
                if (filter.apply(itemAndBroadcast.getBroadcast())) {
                    scheduleBroadcasts.put(scheduleDate, itemAndBroadcast);
                }
            }
        }
        return StreamSupport.stream(new ScheduleIntervalDates(interval).spliterator(), false)
                .map(date -> new ChannelSchedule(
                        channel,
                        new Interval(
                                date.toDateTimeAtStartOfDay(DateTimeZone.UTC),
                                date.plusDays(1).toDateTimeAtStartOfDay(DateTimeZone.UTC)
                        ),
                        scheduleBroadcasts.get(date.toDateTimeAtStartOfDay(DateTimeZone.UTC)
                                .toDate())
                ))
                .filter(cs -> !cs.getEntries().isEmpty())
                .collect(Collectors.toList());
    }

    private ItemAndBroadcast deserialize(ByteBuffer byteBuffer) {
        byte[] bytes = new byte[byteBuffer.remaining()];
        byteBuffer.get(bytes);
        return serializer.deserialize(bytes);
    }

}
