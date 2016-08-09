package org.atlasapi.schedule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.Stream;
import java.util.stream.StreamSupport;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.channel.Channel;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.BroadcastSerializer;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentSerializationVisitor;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceRef;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.util.Column;
import org.atlasapi.util.GroupLock;

import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.Clock;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.joda.time.DateTime;
import org.joda.time.Duration;
import org.joda.time.Interval;
import org.joda.time.LocalDate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.util.Column.bigIntColumn;
import static org.atlasapi.util.Column.bytesColumn;
import static org.atlasapi.util.Column.dateColumn;
import static org.atlasapi.util.Column.textColumn;

public final class CassandraEquivalentScheduleStore extends AbstractEquivalentScheduleStore {

    private static final Logger log = LoggerFactory.getLogger(CassandraEquivalentScheduleStore.class);
    private static final Duration MAX_SCHEDULE_LENGTH = Duration.standardHours(24);
    private static final int CONTENT_UPDATE_TIMEOUT = 10;

    private static final String EQUIVALENT_SCHEDULE_TABLE = "equivalent_schedule";

    private static final Column<String> SOURCE = textColumn("source");
    private static final Column<Long> CHANNEL = bigIntColumn("channel");
    private static final Column<Date> DAY = dateColumn("day");
    private static final Column<String> BROADCAST_ID = textColumn("broadcast_id");
    private static final Column<Date> BROADCAST_START = dateColumn("broadcast_start");
    private static final Column<ByteBuffer> BROADCAST = bytesColumn("broadcast");
    private static final Column<ByteBuffer> GRAPH = bytesColumn("graph");
    private static final Column<Long> CONTENT_COUNT = bigIntColumn("content_count");
    private static final Column<ByteBuffer> CONTENT = bytesColumn("content");
    private static final Column<Date> SCHEDULE_UPDATE = dateColumn("schedule_update");
    private static final Column<Date> EQUIV_UPDATE = dateColumn("equiv_update");

    private final Session session;
    private final ConsistencyLevel read;
    private final ConsistencyLevel write;
    private final Clock clock;

    private final ContentSerializer contentSerializer;
    private final EquivalenceGraphSerializer graphSerializer = new EquivalenceGraphSerializer();
    private final BroadcastSerializer broadcastSerializer = new BroadcastSerializer();

    private final PreparedStatement scheduleSelect;
    private final PreparedStatement broadcastDelete;
    private final PreparedStatement contentUpdate;
    private final PreparedStatement broadcastEquivUpdate;
    private final PreparedStatement broadcastScheduleUpdate;
    private final PreparedStatement broadcastSelect;

    private final GroupLock<String> lock = GroupLock.natural();

    public CassandraEquivalentScheduleStore(
            EquivalenceGraphStore graphStore,
            ContentResolver contentStore,
            Session session,
            ConsistencyLevel read,
            ConsistencyLevel write,
            Clock clock
    ) {
        super(graphStore, contentStore);

        this.contentSerializer = new ContentSerializer(
                new ContentSerializationVisitor(contentStore)
        );
        this.session = checkNotNull(session);
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
        this.clock = checkNotNull(clock);

        this.broadcastSelect = session.prepare(
                select(
                        CHANNEL.name(),
                        SOURCE.name(),
                        DAY.name(),
                        BROADCAST_ID.name(),
                        BROADCAST.name()
                )
                        .from(EQUIVALENT_SCHEDULE_TABLE)
                        .where(eq(SOURCE.name(), bindMarker("source")))
                        .and(eq(CHANNEL.name(), bindMarker("channel")))
                        .and(eq(DAY.name(), bindMarker("day"))));

        this.scheduleSelect = session.prepare(select().all()
                .from(EQUIVALENT_SCHEDULE_TABLE)
                .where(eq(SOURCE.name(), bindMarker("source")))
                .and(eq(CHANNEL.name(), bindMarker("channel")))
                .and(eq(DAY.name(), bindMarker("day"))));

        this.broadcastDelete = session.prepare(
                QueryBuilder.delete().all().from(EQUIVALENT_SCHEDULE_TABLE)
                        .where(eq(SOURCE.name(), bindMarker("source")))
                        .and(eq(CHANNEL.name(), bindMarker("channel")))
                        .and(eq(DAY.name(), bindMarker("day")))
                        .and(eq(BROADCAST_ID.name(), bindMarker("broadcast"))));

        this.contentUpdate = session.prepare(update(EQUIVALENT_SCHEDULE_TABLE)
                .where(eq(SOURCE.name(), bindMarker("source")))
                .and(eq(CHANNEL.name(), bindMarker("channel")))
                .and(eq(DAY.name(), bindMarker("day")))
                .and(eq(BROADCAST_ID.name(), bindMarker("broadcast")))
                .with(set(CONTENT_COUNT.name(), bindMarker("contentCount")))
                .and(set(CONTENT.name(), bindMarker("data"))));

        this.broadcastEquivUpdate = session.prepare(update(EQUIVALENT_SCHEDULE_TABLE)
                .where(eq(SOURCE.name(), bindMarker("source")))
                .and(eq(CHANNEL.name(), bindMarker("channel")))
                .and(eq(DAY.name(), bindMarker("day")))
                .and(eq(BROADCAST_ID.name(), bindMarker("broadcast")))
                .with(set(BROADCAST.name(), bindMarker("broadcastData")))
                .and(set(BROADCAST_START.name(), bindMarker("broadcastStartData")))
                .and(set(GRAPH.name(), bindMarker("graphData")))
                .and(set(CONTENT_COUNT.name(), bindMarker("contentCountData")))
                .and(set(CONTENT.name(), bindMarker("contentData")))
                .and(set(EQUIV_UPDATE.name(), bindMarker("now"))));

        this.broadcastScheduleUpdate = session.prepare(update(EQUIVALENT_SCHEDULE_TABLE)
                .where(eq(SOURCE.name(), bindMarker("source")))
                .and(eq(CHANNEL.name(), bindMarker("channel")))
                .and(eq(DAY.name(), bindMarker("day")))
                .and(eq(BROADCAST_ID.name(), bindMarker("broadcast")))
                .with(set(BROADCAST.name(), bindMarker("broadcastData")))
                .and(set(BROADCAST_START.name(), bindMarker("broadcastStartData")))
                .and(set(GRAPH.name(), bindMarker("graphData")))
                .and(set(CONTENT_COUNT.name(), bindMarker("contentCountData")))
                .and(set(CONTENT.name(), bindMarker("contentData")))
                .and(set(SCHEDULE_UPDATE.name(), bindMarker("now"))));
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            Interval interval,
            Publisher source,
            Set<Publisher> selectedSources
    ) {
        ImmutableList<ResultSetFuture> resultFutures = selectStatements(source, channels, interval)
                .stream()
                .map(statement -> statement.setConsistencyLevel(read))
                .map(session::executeAsync)
                .collect(MoreCollectors.toImmutableList());

        return Futures.transform(
                Futures.allAsList(resultFutures),
                new ToEquivalentSchedule(ImmutableSet.copyOf(channels), interval, selectedSources)
        );
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            DateTime start,
            Integer count,
            Publisher source,
            Set<Publisher> selectedSources
    ) {
        Interval interval = new Interval(start, start.plus(MAX_SCHEDULE_LENGTH));
        return Futures.transform(
                resolveSchedules(channels, interval, source, selectedSources),
                (Function<EquivalentSchedule, EquivalentSchedule>) input ->
                        input.withLimitedBroadcasts(count)
        );
    }

    @Override
    protected void writeSchedule(
            ScheduleUpdate update,
            Map<ScheduleRef.Entry, EquivalentScheduleEntry> content
    ) throws WriteException {
        List<Date> daysInSchedule = daysIn(update.getSchedule().getInterval());
        ImmutableList<Date> staleBroadcastDays = update.getStaleBroadcasts()
                .stream()
                .map(broadcastRef -> broadcastRef.getTransmissionInterval()
                        .getStart()
                        .toLocalDate()
                        .toDate())
                .collect(MoreCollectors.toImmutableList());

        ImmutableSet<String> lockKeys = getLockKeys(
                update.getSchedule().getChannel(),
                update.getSource(),
                ImmutableSet.<Date>builder()
                        .addAll(daysInSchedule)
                        .addAll(staleBroadcastDays)
                        .build()
        );

        try {
            lock.lock(lockKeys);

            DateTime now = clock.now();

            ImmutableSet<BroadcastRef> updateBroadcastRefs = update.getSchedule()
                    .getScheduleEntries()
                    .stream()
                    .map(ScheduleRef.Entry::getBroadcast)
                    .collect(MoreCollectors.toImmutableSet());

            Set<String> updateBroadcastIds = updateBroadcastRefs
                    .stream()
                    .map(BroadcastRef::getSourceId)
                    .collect(MoreCollectors.toImmutableSet());

            Set<BroadcastRef> currentBroadcastRefs = resolveBroadcasts(
                    update.getSource(),
                    update.getSchedule().getChannel(),
                    update.getSchedule().getInterval()
            );

            Set<BroadcastRef> staleBroadcasts = currentBroadcastRefs.stream()
                    .filter(broadcast -> !updateBroadcastIds.contains(broadcast.getSourceId()))
                    .collect(MoreCollectors.toImmutableSet());

            List<Statement> deletes = deleteStatements(
                    update.getSource(),
                    Sets.union(staleBroadcasts, update.getStaleBroadcasts())
            );

            log.info(
                    "Processing equivalent schedule update for {} {} {}: currentEntries:{}, "
                            + "update:{}, stale broadcasts from update: {}, stale broadcasts "
                            + "from store: {}",
                    update.getSource(),
                    update.getSchedule().getChannel().longValue(),
                    update.getSchedule().getInterval(),
                    updateLog(currentBroadcastRefs),
                    updateLog(updateBroadcastRefs),
                    updateLog(update.getStaleBroadcasts()),
                    updateLog(staleBroadcasts)
            );

            List<Statement> updates = updateStatements(
                    update.getSource(),
                    update.getSchedule(),
                    content,
                    now
            );

            if (updates.isEmpty() && deletes.isEmpty()) {
                return;
            }

            BatchStatement updateBatch = new BatchStatement();
            updateBatch.addAll(Iterables.concat(updates, deletes));

            try {
                session.execute(updateBatch.setConsistencyLevel(write));
                log.info(
                        "Processed equivalent schedule update for {} {} {}, updates: {}, "
                                + "deletes: {}",
                        update.getSource(),
                        update.getSchedule().getChannel().longValue(),
                        update.getSchedule().getInterval(),
                        updates.size(),
                        deletes.size()
                );
            } catch (NoHostAvailableException | QueryExecutionException e) {
                throw new WriteException(e);
            }
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock(lockKeys);
        }
    }

    @Override
    protected void updateEquivalentContent(
            Publisher publisher,
            Broadcast broadcast,
            EquivalenceGraph graph,
            ImmutableSet<Item> content
    ) throws WriteException {
        List<Date> daysInInterval = daysIn(broadcast.getTransmissionInterval());

        ImmutableSet<String> lockKeys = getLockKeys(
                broadcast.getChannelId(), publisher, daysInInterval
        );

        try {
            lock.lock(lockKeys);

            Date broadcastStart = broadcast.getTransmissionTime().toDate();
            ByteBuffer broadcastBytes = serialize(broadcast);
            ByteBuffer graphBytes = graphSerializer.serialize(graph);
            ByteBuffer contentBytes = serialize(content);

            daysInInterval
                    .stream()
                    .map(day -> broadcastEquivUpdate.bind()
                            .setString("source", publisher.key())
                            .setLong("channel", broadcast.getChannelId().longValue())
                            .setDate("day", day)
                            .setString("broadcast", broadcast.getSourceId())
                            .setBytes("broadcastData", broadcastBytes)
                            .setDate("broadcastStartData", broadcastStart)
                            .setBytes("graphData", graphBytes)
                            .setLong("contentCountData", content.size())
                            .setBytes("contentData", contentBytes)
                            .setDate("now", clock.now().toDate()))
                    .map(statement -> statement.setConsistencyLevel(write))
                    .forEach(session::execute);
        } catch (InterruptedException e) {
            throw Throwables.propagate(e);
        } finally {
            lock.unlock(lockKeys);
        }
    }

    @Override
    public void updateContent(Iterable<Item> content) throws WriteException {
        ByteBuffer serializedContent = serialize(content);
        int contentCount = Iterables.size(content);

        ImmutableList.Builder<Statement> statements = ImmutableList.builder();
        for (Item item : content) {
            ImmutableList<Broadcast> activelyPublishedBroadcasts = StreamSupport
                    .stream(item.getBroadcasts().spliterator(), false)
                    .filter(Broadcast::isActivelyPublished)
                    .collect(MoreCollectors.toImmutableList());

            BatchStatement batchStatement = new BatchStatement();

            batchStatement.addAll(activelyPublishedBroadcasts.stream()
                    .flatMap(broadcast -> contentUpdateStatements(
                            item.getSource(), broadcast, serializedContent, contentCount
                    ))
                    .collect(Collectors.toList()))
                    .setConsistencyLevel(write);

            statements.add(batchStatement);
        }
        try {
            ListenableFuture<List<ResultSet>> insertsFuture = Futures.allAsList(statements.build()
                    .stream()
                    .map(session::executeAsync)
                    .collect(Collectors.toList()));
            insertsFuture.get(CONTENT_UPDATE_TIMEOUT, TimeUnit.SECONDS);
        } catch (NoHostAvailableException
                | QueryExecutionException
                | InterruptedException
                | ExecutionException
                | TimeoutException
                e
                ) {
            throw new WriteException(e);
        }
    }

    private Stream<Statement> contentUpdateStatements(
            Publisher src,
            Broadcast broadcast,
            ByteBuffer serializedContent,
            int contentCount
    ) {
        return daysIn(broadcast.getTransmissionInterval())
                .stream()
                .map(day -> contentUpdate.bind()
                        .setString("source", src.key())
                        .setLong("channel", broadcast.getChannelId().longValue())
                        .setDate("day", day)
                        .setString("broadcast", broadcast.getSourceId())
                        .setLong("contentCount", contentCount)
                        .setBytes("data", serializedContent));
    }

    private List<Statement> selectStatements(Publisher src, Iterable<Channel> channels,
            Interval interval) {
        List<Date> days = daysIn(interval);
        ImmutableList.Builder<Statement> selects = ImmutableList.builder();

        for (Channel channel : channels) {
            for (Date day : days) {
                selects.add(scheduleSelect.bind()
                        .setString("source", src.key())
                        .setLong("channel", channel.getId().longValue())
                        .setDate("day", day));
            }
        }
        return selects.build();
    }

    private List<Date> daysIn(Interval interval) {
        return StreamSupport.stream(new ScheduleIntervalDates(interval).spliterator(), false)
                .map(LocalDate::toDate)
                .collect(Collectors.toList());
    }

    private String updateLog(Set<BroadcastRef> staleBroadcasts) {
        StringBuilder update = new StringBuilder();
        for (BroadcastRef broadcastRef : staleBroadcasts) {
            update.append(
                    String.format(
                            " %s -> (%s -> %s)",
                            broadcastRef.getSourceId(),
                            broadcastRef.getTransmissionInterval().getStart(),
                            broadcastRef.getTransmissionInterval().getEnd()
                    )
            );
        }

        return update.toString();
    }

    private List<Statement> updateStatements(Publisher source, ScheduleRef scheduleRef,
            Map<ScheduleRef.Entry, EquivalentScheduleEntry> content, DateTime now)
            throws WriteException {
        ImmutableList.Builder<Statement> statements = ImmutableList.builder();

        for (ScheduleRef.Entry entry : scheduleRef.getScheduleEntries()) {
            EquivalentScheduleEntry entryItems = content.get(entry);
            if (entryItems != null) {
                statements.addAll(statementsForEntry(source, entry, entryItems, now));
            } else {
                log.warn("No content provided for entry " + entry);
            }
        }
        return statements.build();
    }

    private List<Statement> statementsForEntry(
            Publisher source,
            ScheduleRef.Entry entry,
            EquivalentScheduleEntry content,
            DateTime now
    ) throws WriteException {
        Equivalent<Item> items = content.getItems();
        int contentCount = items.getResources().size();

        ByteBuffer serializedContent = serialize(items.getResources());
        ByteBuffer graph = graphSerializer.serialize(items.getGraph());
        ByteBuffer broadcast = serialize(content.getBroadcast());

        return daysIn(entry.getBroadcast().getTransmissionInterval())
                .stream()
                .map(day -> broadcastScheduleUpdate.bind()
                        .setString("source", source.key())
                        .setLong("channel", entry.getBroadcast().getChannelId().longValue())
                        .setDate("day", day)
                        .setString("broadcast", entry.getBroadcast().getSourceId())
                        .setBytes("broadcastData", broadcast)
                        .setDate(
                                "broadcastStartData",
                                entry.getBroadcast().getTransmissionInterval().getStart().toDate()
                        )
                        .setBytes("graphData", graph)
                        .setLong("contentCountData", contentCount)
                        .setBytes("contentData", serializedContent)
                        .setDate("now", now.toDate()))
                .collect(MoreCollectors.toImmutableList());
    }

    private ByteBuffer serialize(Iterable<Item> resources) throws WriteException {
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for (Content content : resources) {
            try {
                contentSerializer.serialize(content).writeDelimitedTo(outputStream);
            } catch (IOException e) {
                throw new WriteException("failed to serialize " + content, e);
            }
        }
        return ByteBuffer.wrap(outputStream.toByteArray());
    }

    private ByteBuffer serialize(Broadcast broadcast) {
        return ByteBuffer.wrap(broadcastSerializer.serialize(broadcast).build().toByteArray());
    }

    /**
     * Delete stale broadcasts from the update interval. We need to delete the broadcasts from the
     * update interval, rather than broadcast interval because we need to avoid incorrectly avoid
     * broadcasts which were moved out of update interval
     */
    private List<Statement> deleteStatements(Publisher src, Set<BroadcastRef> staleBroadcasts) {
        return staleBroadcasts.stream()
                .map(broadcastRef -> broadcastDelete.bind()
                        .setString("source", src.key())
                        .setLong("channel", broadcastRef.getChannelId().longValue())
                        .setDate("day", broadcastRef.getTransmissionInterval()
                                .getStart()
                                .toLocalDate()
                                .toDate())
                        .setString("broadcast", broadcastRef.getSourceId()))
                .collect(MoreCollectors.toImmutableList());
    }

    private Set<BroadcastRef> resolveBroadcasts(Publisher publisher, Id channelId,
            Interval interval) throws WriteException {
        ImmutableList.Builder<ListenableFuture<ResultSet>> broadcastFutures = ImmutableList.builder();
        for (Date day : daysIn(interval)) {
            broadcastFutures.add(session.executeAsync(
                    broadcastSelect.bind()
                            .setString("source", publisher.key())
                            .setLong("channel", channelId.longValue())
                            .setDate("day", day)));
        }

        ImmutableList<Row> rows = Futures.get(
                Futures.allAsList(broadcastFutures.build()),
                WriteException.class
        ).stream()
                .flatMap(rs -> StreamSupport.stream(rs.spliterator(), false))
                .collect(MoreCollectors.toImmutableList());

        ImmutableSet.Builder<BroadcastRef> broadcasts = ImmutableSet.builder();
        for (Row row : rows) {
            if (row.isNull(BROADCAST.name())) {
                log.warn(
                        "null broadcast for day: {}, channel: {}, source {}, broadcast {}",
                        DAY.valueFrom(row),
                        CHANNEL.valueFrom(row),
                        SOURCE.valueFrom(row),
                        BROADCAST_ID.valueFrom(row)

                );
                continue;
            }
            try {
                Broadcast broadcast = broadcastSerializer.deserialize(
                        ContentProtos.Broadcast.parseFrom(
                                ByteString.copyFrom(BROADCAST.valueFrom(row))
                        )
                );
                if (broadcast.getTransmissionInterval().overlaps(interval)) {
                    broadcasts.add(broadcast.toRef());
                }
            } catch (InvalidProtocolBufferException e) {
                Throwables.propagate(e);
            }
        }
        return broadcasts.build();
    }

    private ImmutableSet<String> getLockKeys(Id channelId, Publisher source,
            Iterable<Date> dates) {
        return StreamSupport.stream(dates.spliterator(), false)
                .map(date -> getLockKey(channelId, source, date))
                .collect(MoreCollectors.toImmutableSet());
    }

    private String getLockKey(Id channelId, Publisher source, Date date) {
        return channelId.longValue() + "|" + source.key() + "|" + date.toInstant().toEpochMilli();
    }

    private final class ToEquivalentSchedule implements Function<List<ResultSet>,
            EquivalentSchedule> {

        private final Interval interval;
        private final Set<Publisher> selectedSources;
        private final Set<Channel> channels;

        private ToEquivalentSchedule(
                Set<Channel> channels,
                Interval interval,
                Set<Publisher> selectedSources
        ) {
            this.interval = interval;
            this.selectedSources = selectedSources;
            this.channels = channels;
        }

        @Override
        public EquivalentSchedule apply(List<ResultSet> input) {
            return new EquivalentSchedule(toChannelSchedules(input, channels, interval, Annotation.all()), interval);
        }

        public EquivalentSchedule apply(List<ResultSet> input, Set<Annotation> annotations) {
            return new EquivalentSchedule(toChannelSchedules(input, channels, interval, annotations), interval);
        }

        private List<EquivalentChannelSchedule> toChannelSchedules(List<ResultSet> input,
                Iterable<Channel> channels, Interval interval, Set<Annotation> annotations) {
            SetMultimap<Id, EquivalentScheduleEntry> entriesByChannel = transformToEntries(
                    input, interval, annotations
            );

            ImmutableList.Builder<EquivalentChannelSchedule> channelSchedules =
                    ImmutableList.builder();
            for (Channel channel : channels) {
                Set<EquivalentScheduleEntry> entries = entriesByChannel.get(channel.getId());
                channelSchedules.add(new EquivalentChannelSchedule(channel, interval, entries));
            }
            return channelSchedules.build();
        }

        private SetMultimap<Id, EquivalentScheduleEntry> transformToEntries(List<ResultSet> input,
                Interval interval, Set<Annotation> annotations) {
            ScheduleBroadcastFilter broadcastFilter = ScheduleBroadcastFilter.valueOf(interval);
            ImmutableSetMultimap.Builder<Id, EquivalentScheduleEntry> channelEntries =
                    ImmutableSetMultimap.builder();
            for (Row row : Iterables.concat(input)) {
                if (row.isNull(BROADCAST.name())) {
                    log.warn(
                            "null broadcast for day: {}, channel: {}, source {}, broadcast {}",
                            DAY.valueFrom(row),
                            CHANNEL.valueFrom(row),
                            SOURCE.valueFrom(row),
                            BROADCAST_ID.valueFrom(row)

                    );
                    continue;
                }
                deserializeRow(channelEntries, row, broadcastFilter, annotations);
            }
            return channelEntries.build();
        }

        private void deserializeRow(
                ImmutableSetMultimap.Builder<Id, EquivalentScheduleEntry> channelEntries,
                Row row,
                ScheduleBroadcastFilter broadcastFilter,
                Set<Annotation> annotations
        ) {
            try {
                Broadcast broadcast = deserialize(BROADCAST.valueFrom(row));
                if (broadcastFilter.apply(broadcast.getTransmissionInterval())) {
                    Equivalent<Item> equivItems = deserialize(row, annotations);
                    channelEntries.put(
                            Id.valueOf(CHANNEL.valueFrom(row)),
                            new EquivalentScheduleEntry(broadcast, equivItems)
                    );
                }
            } catch (IOException e) {
                throw new RuntimeException("Error reading " + row, e);
            }
        }

        private Equivalent<Item> deserialize(Row row, Set<Annotation> annotations) throws IOException {
            EquivalenceGraph graph = graphSerializer.deserialize(GRAPH.valueFrom(row));
            Long itemCount = CONTENT_COUNT.valueFrom(row);
            ByteBuffer itemsBytes = CONTENT.valueFrom(row);

            ByteString itemBytesCopy = ByteString.copyFrom(itemsBytes);
            InputStream itemsStream = itemBytesCopy.newInput();

            ImmutableSet.Builder<Item> itemsBuilder = ImmutableSet.builder();
            for (int i = 0; i < itemCount; i++) {
                ContentProtos.Content msg = ContentProtos.Content.parseDelimitedFrom(itemsStream);
                Item item = (Item) contentSerializer.deserialize(msg, annotations);
                if (selectedSources.contains(item.getSource())) {
                    itemsBuilder.add(item);
                }
            }

            ImmutableSet<Item> items = getItemsWithEquivalents(itemsBuilder.build());

            return new Equivalent<>(graph, items);
        }

        private ImmutableSet<Item> getItemsWithEquivalents(ImmutableSet<Item> items) {
            // Ensure all items in the set have the correct equivalents

            ImmutableSet<EquivalenceRef> equivalenceRefs = items.stream()
                    .map(EquivalenceRef::valueOf)
                    .collect(MoreCollectors.toImmutableSet());

            return items.stream()
                    .map(item -> (Item) item.copyWithEquivalentTo(
                            equivalenceRefs.stream()
                                    .filter(ref -> !ref.getId().equals(item.getId()))
                                    .collect(MoreCollectors.toImmutableSet())
                    ))
                    .collect(MoreCollectors.toImmutableSet());
        }

        private Broadcast deserialize(ByteBuffer broadcastBytes)
                throws InvalidProtocolBufferException {
            return broadcastSerializer.deserialize(ContentProtos.Broadcast.parseFrom(
                    ByteString.copyFrom(broadcastBytes)
            ));
        }
    }
}
