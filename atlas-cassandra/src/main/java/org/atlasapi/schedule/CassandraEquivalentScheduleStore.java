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
import java.util.stream.StreamSupport;

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
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.util.Column;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.time.Clock;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
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
import com.google.common.collect.Lists;
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

    private static final Logger log =
            LoggerFactory.getLogger(CassandraEquivalentScheduleStore.class);
    private static final Duration MAX_SCHEDULE_LENGTH = Duration.standardHours(24);
    private static final int CONTENT_UPDATE_TIMEOUT = 10;

    private final class ToEquivalentSchedule implements Function<List<ResultSet>, EquivalentSchedule> {

        private final Interval interval;
        private final Set<Publisher> selectedSources;
        private final Set<Channel> chans;

        private ToEquivalentSchedule(Set<Channel> chans, Interval interval,
                Set<Publisher> selectedSources) {
            this.interval = interval;
            this.selectedSources = selectedSources;
            this.chans = chans;
        }

        @Override
        public EquivalentSchedule apply(List<ResultSet> input) {
            return new EquivalentSchedule(toChannelSchedules(input, chans, interval), interval);
        }

        private List<EquivalentChannelSchedule> toChannelSchedules(List<ResultSet> input,
                Iterable<Channel> channels, final Interval interval) {
            SetMultimap<Id, EquivalentScheduleEntry> entriesByChannel = transformToEntries(input, interval);
            ImmutableList.Builder<EquivalentChannelSchedule> channelSchedules = ImmutableList.builder();
            for (Channel channel : channels) {
                Set<EquivalentScheduleEntry> entries = entriesByChannel.get(channel.getId());
                channelSchedules.add(new EquivalentChannelSchedule(channel, interval, entries));
            }
            return channelSchedules.build();
        }

        private SetMultimap<Id, EquivalentScheduleEntry> transformToEntries(
                List<ResultSet> input, Interval interval) {
            ScheduleBroadcastFilter broadcastFilter = ScheduleBroadcastFilter.valueOf(interval);
            ImmutableSetMultimap.Builder<Id, EquivalentScheduleEntry> channelEntries = ImmutableSetMultimap.builder();
            for (Row row : Iterables.concat(input)) {
                if(row.isNull(BROADCAST.name())) {
                    log.warn(
                            "null broadcast for day: {}, channel: {}, source {}, broadcast {}",
                            DAY.valueFrom(row),
                            CHANNEL.valueFrom(row),
                            SOURCE.valueFrom(row),
                            BROADCAST_ID.valueFrom(row)

                    );
                    continue;
                }
                deserializeRow(channelEntries, row, broadcastFilter);
            }
            return channelEntries.build();
        }

        private void deserializeRow(
                ImmutableSetMultimap.Builder<Id, EquivalentScheduleEntry> channelEntries,
                Row row, ScheduleBroadcastFilter broadcastFilter) {
            try {
                Broadcast broadcast = deserialize(BROADCAST.valueFrom(row));
                if (broadcastFilter.apply(broadcast.getTransmissionInterval())) {
                    Equivalent<Item> equivItems = deserialize(row);
                    channelEntries.put(Id.valueOf(CHANNEL.valueFrom(row)),
                            new EquivalentScheduleEntry(broadcast, equivItems));
                }
            } catch (IOException e) {
                // has to be unchecked. is there a better type? 
                // does it matter since we're in a future?
                throw new RuntimeException("error reading "+row, e);
            }
        }

        private Equivalent<Item> deserialize(Row row) throws IOException {
            EquivalenceGraph graph = graphSerializer.deserialize(GRAPH.valueFrom(row));
            Long itemCount = CONTENT_COUNT.valueFrom(row);
            ByteBuffer itemsBytes = CONTENT.valueFrom(row);
            ByteString sytes = ByteString.copyFrom(itemsBytes);
            InputStream itemsStream = sytes.newInput();
            ImmutableSet.Builder<Item> items = ImmutableSet.builder();
            for (int i = 0; i < itemCount; i++) {
                ContentProtos.Content msg =
                    ContentProtos.Content.parseDelimitedFrom(itemsStream);
                Item item = (Item)contentSerializer.deserialize(msg);
                if (selectedSources.contains(item.getSource())) {
                    items.add(item);
                }
            }
            return new Equivalent<>(graph, items.build());
        }

        private Broadcast deserialize(ByteBuffer bcastBytes) throws InvalidProtocolBufferException {
            return broadcastSerializer.deserialize(ContentProtos.Broadcast.parseFrom(ByteString.copyFrom(bcastBytes)));
        }
    }

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

    public CassandraEquivalentScheduleStore(EquivalenceGraphStore graphStore,
            ContentResolver contentStore, Session session, ConsistencyLevel read,
            ConsistencyLevel write, Clock clock) {
        super(graphStore, contentStore);
        this.contentSerializer = new ContentSerializer(new ContentSerializationVisitor(contentStore));
        this.session = checkNotNull(session);
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
        this.clock = checkNotNull(clock);

        this.broadcastSelect = session.prepare(
                select(CHANNEL.name(), SOURCE.name(), DAY.name(), BROADCAST_ID.name(), BROADCAST.name())
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
    public void updateContent(Iterable<Item> content) throws WriteException {
        ByteBuffer serializedContent = serialize(content);
        int contentCount = Iterables.size(content);

        ImmutableList.Builder<Statement> statements = ImmutableList.builder();
        for (Item item : content) {
            Publisher src = item.getSource();
            ImmutableList<Broadcast> activelyPublishedBroadcasts = StreamSupport.stream(
                    item.getBroadcasts().spliterator(), false)
                    .filter(b -> Broadcast.ACTIVELY_PUBLISHED.apply(b))
                    .collect(ImmutableCollectors.toList());

            BatchStatement batchStatement = new BatchStatement();
            batchStatement.addAll(activelyPublishedBroadcasts.stream()
                    .flatMap(broadcast -> StreamSupport.stream(
                            contentUpdateStatements(src, broadcast, serializedContent, contentCount).spliterator(), false))
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
        } catch(NoHostAvailableException
                | QueryExecutionException
                | InterruptedException
                | ExecutionException
                | TimeoutException
                e
        ) {
            throw new WriteException(e);
        }
    }

    private Iterable<Statement> contentUpdateStatements(
            Publisher src,
            Broadcast broadcast,
            ByteBuffer serializedContent,
            int contentCount
    ) {
        ImmutableList.Builder<Statement> statements = ImmutableList.builder();
        for (Date day : daysIn(broadcast.getTransmissionInterval())) {
            statements.add(
                    contentUpdate.bind()
                        .setString("source", src.key())
                        .setLong("channel", broadcast.getChannelId().longValue())
                        .setDate("day", day)
                        .setString("broadcast", broadcast.getSourceId())
                        .setLong("contentCount", contentCount)
                        .setBytes("data", serializedContent)
            );
        }

        return statements.build();
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            final Interval interval,
            Publisher source,
            final Set<Publisher> selectedSources
    ) {
        final Set<Channel> chans = ImmutableSet.copyOf(channels);
        List<Statement> selects = selectStatements(source, channels, interval);
        ListenableFuture<List<ResultSet>> results = Futures.allAsList(Lists.transform(selects,
                (Function<Statement, ListenableFuture<ResultSet>>) input ->
                        session.executeAsync(input.setConsistencyLevel(read))
        ));

        return Futures.transform(
                results,
                new ToEquivalentSchedule(chans, interval, selectedSources)
        );
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(
            Iterable<Channel> channels,
            DateTime start,
            final Integer count,
            Publisher source,
            Set<Publisher> selectedSources
    ) {
        Interval interval = new Interval(start, start.plus(MAX_SCHEDULE_LENGTH));
        return Futures.transform(
                resolveSchedules(channels, interval, source, selectedSources),
                (EquivalentSchedule input) -> input.withLimitedBroadcasts(count)
        );
    }

    private List<Statement> selectStatements(
            Publisher src,
            Iterable<Channel> channels,
            Interval interval
    ) {
        ImmutableList.Builder<Statement> selects = ImmutableList.builder();
        List<Date> days = daysIn(interval);
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

    @Override
    protected synchronized void writeSchedule(
            ScheduleUpdate update,
            Map<ScheduleRef.Entry, EquivalentScheduleEntry> content
    ) throws WriteException {
        DateTime now = clock.now();

        ImmutableSet<BroadcastRef> updateBroadcastRefs = update.getSchedule().getScheduleEntries()
                .stream()
                .map(ScheduleRef.Entry::getBroadcast)
                .collect(ImmutableCollectors.toSet());

        Set<String> updateBroadcastIds = updateBroadcastRefs
                .stream()
                .map(BroadcastRef::getSourceId)
                .collect(ImmutableCollectors.toSet());

        Set<BroadcastRef> currentBroadcastRefs = resolveBroadcasts(
                update.getSource(),
                update.getSchedule().getChannel(),
                update.getSchedule().getInterval()
        );

        Set<BroadcastRef> staleBroadcasts = currentBroadcastRefs.stream()
                .filter(broadcastRef -> !updateBroadcastIds.contains(broadcastRef.getSourceId()))
                .collect(ImmutableCollectors.toSet());

        List<Statement> deletes = deleteStatements(
                update.getSource(),
                Sets.union(staleBroadcasts, update.getStaleBroadcasts()),
                update.getSchedule().getInterval()
        );
        log.info(
                "Processing equivalent schedule update for {} {} {}: currentEntries:{}, update:{}, stale broadcasts from update: {}, stale broadcasts from store: {}",
                update.getSource(),
                update.getSchedule().getChannel().longValue(),
                update.getSchedule().getInterval(),
                updateLog(currentBroadcastRefs),
                updateLog(updateBroadcastRefs),
                updateLog(update.getStaleBroadcasts()),
                updateLog(staleBroadcasts)
        );
        List<Statement> updates = updateStatements(update.getSource(),
                update.getSchedule(),
                content,
                now);
        if (updates.isEmpty() && deletes.isEmpty()) {
            return;
        }

        BatchStatement updateBatch = new BatchStatement();
        updateBatch.addAll(Iterables.concat(updates, deletes));

        try {
            session.execute(updateBatch.setConsistencyLevel(write));
            log.info(
                    "Processed equivalent schedule update for {} {} {}, updates: {}, deletes: {}",
                    update.getSource(),
                    update.getSchedule().getChannel().longValue(),
                    update.getSchedule().getInterval(),
                    updates.size(),
                    deletes.size()

            );
        } catch(NoHostAvailableException | QueryExecutionException e) {
            throw new WriteException(e);
        }
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

    private List<Statement> updateStatements(
            Publisher source,
            ScheduleRef scheduleRef,
            Map<ScheduleRef.Entry, EquivalentScheduleEntry> content,
            DateTime now
    ) throws WriteException {
        ImmutableList.Builder<Statement> stmts = ImmutableList.builder();
        for (ScheduleRef.Entry entry : scheduleRef.getScheduleEntries()) {
            EquivalentScheduleEntry entryItems = content.get(entry);
            if (entryItems != null) {
                statementsForEntry(stmts, source, entry, entryItems, now);
            } else {
                log.warn("No content provided for entry " + entry);
            }
        }
        return stmts.build();
    }
    
    private void statementsForEntry(
            ImmutableList.Builder<Statement> stmts,
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
        
        for (Date day : daysIn(entry.getBroadcast().getTransmissionInterval())) {
            stmts.add(updateStatement(source, day, entry, contentCount, serializedContent,
                    graph, broadcast, now));
        }
    }

    private Statement updateStatement(
            Publisher source,
            Date day,
            ScheduleRef.Entry entry,
            int contentCount,
            ByteBuffer content,
            ByteBuffer graph,
            ByteBuffer broadcast,
            DateTime now
    ) {
        Date broadcastStart = entry.getBroadcast().getTransmissionInterval().getStart().toDate();
        return scheduleUpdateStatement(source, entry.getBroadcast().getChannelId(),
            day, entry.getBroadcast().getSourceId(), broadcastStart, broadcast, graph, contentCount, content, now
        );
    }

    private ByteBuffer serialize(Iterable<Item> resources) throws WriteException {
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        for (Content content : resources) {
            try {
                contentSerializer.serialize(content).writeDelimitedTo(baos);
            } catch (IOException ioe) {
                throw new WriteException("failed to serialize " + content, ioe);
            }
        }
        return ByteBuffer.wrap(baos.toByteArray());
    }

    private ByteBuffer serialize(Broadcast broadcast) {
        return ByteBuffer.wrap(broadcastSerializer.serialize(broadcast).build().toByteArray());
    }

    /**
     * Delete stale broadcasts from the update interval.
     * We need to delete the broadcasts from the update interval, rather than broadcast interval
     * because we need to avoid incorrectly avoid broadcasts which were moved out of update interval
     * @param src
     * @param staleBroadcasts
     * @param interval
     * @return
     */
    private List<Statement> deleteStatements(
            Publisher src,
            Set<BroadcastRef> staleBroadcasts,
            Interval interval
    ) {
        ImmutableList.Builder<Statement> stmts = ImmutableList.builder();
        for (BroadcastRef ref : staleBroadcasts) {
            for (Date day : daysIn(interval)) {
                stmts.add(delete(ref, src, day));
            }
        }
        return stmts.build();
    }

    private Statement delete(BroadcastRef ref, Publisher src, Date date) {
        return broadcastDelete.bind()
                .setString("source", src.key())
                .setLong("channel", ref.getChannelId().longValue())
                .setDate("day", date)
                .setString("broadcast", ref.getSourceId());
    }

    @Override
    protected synchronized void updateEquivalentContent(
            Publisher publisher,
            Broadcast bcast,
            EquivalenceGraph graph,
            ImmutableSet<Item> content
    ) throws WriteException {

        Date bcastStart = bcast.getTransmissionTime().toDate();
        ByteBuffer bcastBytes = serialize(bcast);
        ByteBuffer graphBytes = graphSerializer.serialize(graph);
        ByteBuffer contentBytes = serialize(content);
        
        ImmutableList.Builder<Statement> stmts = ImmutableList.builder();
        for (Date day : daysIn(bcast.getTransmissionInterval())) {
            stmts.add(equivUpdateStatement(publisher, bcast.getChannelId(), day,
                    bcast.getSourceId(), bcastStart, bcastBytes, graphBytes, content.size(), contentBytes));
        }
        for (Statement statement : stmts.build()) {
            session.execute(statement.setConsistencyLevel(write));
        }
    }

    private Statement equivUpdateStatement(
            Publisher publisher,
            Id channelId,
            Date day,
            String bcastId,
            Date bcastStart,
            ByteBuffer bcastBytes,
            ByteBuffer graphBytes,
            int contentCount,
            ByteBuffer contentBytes
    ) {
        return broadcastEquivUpdate.bind()
                .setString("source", publisher.key())
                .setLong("channel", channelId.longValue())
                .setDate("day", day)
                .setString("broadcast", bcastId)
                .setBytes("broadcastData", bcastBytes)
                .setDate("broadcastStartData", bcastStart)
                .setBytes("graphData", graphBytes)
                .setLong("contentCountData", contentCount)
                .setBytes("contentData", contentBytes)
                .setDate("now", clock.now().toDate());
    }

    private Statement scheduleUpdateStatement(
            Publisher publisher,
            Id channelId,
            Date day,
            String bcastId,
            Date bcastStart,
            ByteBuffer bcastBytes,
            ByteBuffer graphBytes,
            int contentCount,
            ByteBuffer contentBytes,
            DateTime now
    ) {
        return broadcastScheduleUpdate.bind()
                .setString("source", publisher.key())
                .setLong("channel", channelId.longValue())
                .setDate("day", day)
                .setString("broadcast", bcastId)
                .setBytes("broadcastData", bcastBytes)
                .setDate("broadcastStartData", bcastStart)
                .setBytes("graphData", graphBytes)
                .setLong("contentCountData", contentCount)
                .setBytes("contentData", contentBytes)
                .setDate("now", now.toDate());
    }

    private Set<BroadcastRef> resolveBroadcasts(
            Publisher publisher,
            Id channelId,
            Interval interval
    ) throws WriteException {
        ImmutableList.Builder<ListenableFuture<ResultSet>> broadcastFutures = ImmutableList.builder();
        for (Date day : daysIn(interval)) {
            broadcastFutures.add(session.executeAsync(
                    broadcastSelect.bind()
                            .setString("source", publisher.key())
                            .setLong("channel", channelId.longValue())
                            .setDate("day", day)));
        }
        ImmutableList<Row> rows = Futures.get(
                Futures.allAsList(
                        broadcastFutures.build()
                ),
                WriteException.class
        ).stream()
                .flatMap(rs -> StreamSupport.stream(rs.spliterator(), false))
                .collect(ImmutableCollectors.toList());

        ImmutableSet.Builder<BroadcastRef> broadcasts = ImmutableSet.builder();
        for (Row row : rows) {
            if(row.isNull(BROADCAST.name())) {
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
                Broadcast broadcast  = broadcastSerializer.deserialize(
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
}
