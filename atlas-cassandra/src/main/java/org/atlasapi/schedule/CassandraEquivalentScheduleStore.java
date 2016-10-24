package org.atlasapi.schedule;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Objects;
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
import com.google.common.collect.ImmutableCollection;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMultimap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.SetMultimap;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
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
        DateTime now = clock.now();

        ImmutableSet<BroadcastRef> updateBroadcastRefs = update.getSchedule()
                .getScheduleEntries()
                .stream()
                .map(ScheduleRef.Entry::getBroadcast)
                .collect(MoreCollectors.toImmutableSet());

        ImmutableMultimap<LocalDate, BroadcastRow> currentBroadcastRows = resolveBroadcasts(
                update.getSource(),
                update.getSchedule().getChannel(),
                update.getSchedule().getInterval()
        );

        Set<BroadcastRow> staleBroadcasts = getStaleBroadcasts(
                updateBroadcastRefs, currentBroadcastRows
        );

        List<Statement> deletes = deleteStatements(
                update.getSource(),
                update.getSchedule().getChannel(),
                staleBroadcasts
        );

        log.info(
                "Processing equivalent schedule update for {} {} {}: currentEntries:{}, "
                        + "update:{}, stale broadcasts from update:{}, stale broadcasts:{}",
                update.getSource(),
                update.getSchedule().getChannel().longValue(),
                update.getSchedule().getInterval(),
                currentBroadcastRows.values()
                        .stream()
                        .map(BroadcastRow::toString)
                        .collect(Collectors.joining(",")),
                updateLog(updateBroadcastRefs),
                updateLog(update.getStaleBroadcasts()),
                staleBroadcasts.stream()
                        .map(BroadcastRow::getBroadcastSourceId)
                        .collect(Collectors.joining(","))
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
    }

    @Override
    protected void updateEquivalentContent(
            Publisher publisher,
            Broadcast broadcast,
            EquivalenceGraph graph,
            ImmutableSet<Item> content,
            List<LocalDate> daysInInterval
    ) throws WriteException {
        Date broadcastStart = broadcast.getTransmissionTime().toDate();
        ByteBuffer broadcastBytes = serialize(broadcast);
        ByteBuffer graphBytes = graphSerializer.serialize(graph);
        ByteBuffer contentBytes = serialize(content);

        daysInInterval
                .stream()
                .map(day -> broadcastEquivUpdate.bind()
                        .setString("source", publisher.key())
                        .setLong("channel", broadcast.getChannelId().longValue())
                        .setTimestamp("day", toJavaUtilDate(day))
                        .setString("broadcast", broadcast.getSourceId())
                        .setBytes("broadcastData", broadcastBytes)
                        .setTimestamp("broadcastStartData", broadcastStart)
                        .setBytes("graphData", graphBytes)
                        .setLong("contentCountData", content.size())
                        .setBytes("contentData", contentBytes)
                        .setTimestamp("now", clock.now().toDate()))
                .map(statement -> statement.setConsistencyLevel(write))
                .forEach(session::execute);
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

    private Set<BroadcastRow> getStaleBroadcasts(
            Set<BroadcastRef> updateBroadcastRefs,
            ImmutableMultimap<LocalDate, BroadcastRow> currentBroadcastRows
    ) {
        ImmutableMultimap<LocalDate, BroadcastRef> updateBroadcastsByDay = getBroadcastsByDay(
                updateBroadcastRefs
        );

        ImmutableSet.Builder<BroadcastRow> staleBroadcasts = ImmutableSet.builder();

        for (LocalDate day : currentBroadcastRows.keySet()) {
            ImmutableCollection<BroadcastRow> currentBroadcastsInDay = currentBroadcastRows
                    .get(day);

            ImmutableSet<String> updateBroadcastIdsInDay = updateBroadcastsByDay
                    .get(day)
                    .stream()
                    .map(BroadcastRef::getSourceId)
                    .collect(MoreCollectors.toImmutableSet());

            ImmutableSet<BroadcastRow> staleBroadcastsInDay = currentBroadcastsInDay
                    .stream()
                    .filter(broadcast ->
                            !updateBroadcastIdsInDay.contains(broadcast.getBroadcastSourceId()))
                    .collect(MoreCollectors.toImmutableSet());

            staleBroadcasts.addAll(staleBroadcastsInDay);
        }

        return staleBroadcasts.build();
    }

    private ImmutableMultimap<LocalDate, BroadcastRef> getBroadcastsByDay(
            Set<BroadcastRef> broadcasts
    ) {
        ImmutableMultimap.Builder<LocalDate, BroadcastRef> broadcastsPerDay =
                ImmutableMultimap.builder();

        for (BroadcastRef broadcast : broadcasts) {
            for (LocalDate day : daysIn(broadcast.getTransmissionInterval())) {
                broadcastsPerDay.put(day, broadcast);
            }
        }

        return broadcastsPerDay.build();
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
                        .setTimestamp("day", toJavaUtilDate(day))
                        .setString("broadcast", broadcast.getSourceId())
                        .setLong("contentCount", contentCount)
                        .setBytes("data", serializedContent));
    }

    private List<Statement> selectStatements(Publisher src, Iterable<Channel> channels,
            Interval interval) {
        List<LocalDate> days = daysIn(interval);
        ImmutableList.Builder<Statement> selects = ImmutableList.builder();

        for (Channel channel : channels) {
            for (LocalDate day : days) {
                selects.add(scheduleSelect.bind()
                        .setString("source", src.key())
                        .setLong("channel", channel.getId().longValue())
                        .setTimestamp("day", toJavaUtilDate(day)));
            }
        }
        return selects.build();
    }

    private Date toJavaUtilDate(LocalDate localDate) {
        return localDate.toDateTimeAtStartOfDay(DateTimeZone.UTC).toDate();
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
            EquivalentScheduleEntry scheduleEntry = content.get(entry);
            if (scheduleEntry != null) {
                statements.addAll(statementsForEntry(source, scheduleEntry, now));
            } else {
                log.warn("No content provided for entry " + entry);
            }
        }
        return statements.build();
    }

    private List<Statement> statementsForEntry(
            Publisher source,
            EquivalentScheduleEntry content,
            DateTime now
    ) throws WriteException {
        Equivalent<Item> items = content.getItems();
        int contentCount = items.getResources().size();

        Broadcast broadcast = content.getBroadcast();

        ByteBuffer serializedContent = serialize(items.getResources());
        ByteBuffer serializedGraph = graphSerializer.serialize(items.getGraph());
        ByteBuffer serializedBroadcast = serialize(broadcast);

        return daysIn(broadcast.getTransmissionInterval())
                .stream()
                .map(day -> broadcastScheduleUpdate.bind()
                        .setString("source", source.key())
                        .setLong("channel", broadcast.getChannelId().longValue())
                        .setTimestamp("day", toJavaUtilDate(day))
                        .setString("broadcast", broadcast.getSourceId())
                        .setBytes("broadcastData", serializedBroadcast)
                        .setTimestamp(
                                "broadcastStartData",
                                broadcast.getTransmissionInterval().getStart().toDate()
                        )
                        .setBytes("graphData", serializedGraph)
                        .setLong("contentCountData", contentCount)
                        .setBytes("contentData", serializedContent)
                        .setTimestamp("now", now.toDate()))
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

    private List<Statement> deleteStatements(
            Publisher src, Id channelId, Set<BroadcastRow> staleBroadcasts
    ) {
        return staleBroadcasts.stream()
                .map(staleBroadcast -> broadcastDelete.bind()
                        .setString(
                                "source",
                                src.key()
                        )
                        .setLong(
                                "channel",
                                channelId.longValue()
                        )
                        .setTimestamp(
                                "day",
                                toJavaUtilDate(staleBroadcast.getDay())
                        )
                        .setString(
                                "broadcast",
                                staleBroadcast.getBroadcastSourceId()
                        ))
                .collect(MoreCollectors.toImmutableList());
    }

    private ImmutableMultimap<LocalDate, BroadcastRow> resolveBroadcasts(
            Publisher publisher,
            Id channelId,
            Interval interval
    ) throws WriteException {
        ImmutableMultimap<LocalDate, ResultSetFuture> futuresByDay = daysIn(interval)
                .stream()
                .collect(MoreCollectors.toImmutableListMultiMap(
                        day -> day,
                        day -> session.executeAsync(
                                broadcastSelect.bind()
                                        .setString("source", publisher.key())
                                        .setLong("channel", channelId.longValue())
                                        .setTimestamp("day", toJavaUtilDate(day))
                        )
                ));

        ImmutableMultimap.Builder<LocalDate, BroadcastRow> broadcastRows =
                ImmutableMultimap.builder();

        // We are getting the broadcastId from its column instead of the broadcast itself because
        // we have seen situations where the broadcasts have been written under the wrong ID due
        // to bugs. Using the broadcastId in the column guarantees we will either mark this row
        // as stale or update it as appropriate
        // We are also including rows with null broadcasts to ensure we give downstream code the
        // chance to update or delete them
        for (LocalDate day : futuresByDay.keySet()) {
            Futures.get(
                    Futures.allAsList(futuresByDay.get(day)),
                    WriteException.class
            )
                    .stream()
                    .flatMap(resultSet -> StreamSupport.stream(resultSet.spliterator(), false))
                    .filter(row -> row.isNull(BROADCAST.name()) ||
                            deserializeBroadcast(row).getTransmissionInterval().overlaps(interval))
                    .map(row -> BroadcastRow.create(
                            day,
                            BROADCAST_ID.valueFrom(row)
                    ))
                    .forEach(broadcastRow -> broadcastRows.put(
                            day, broadcastRow
                    ));
        }

        return broadcastRows.build();
    }

    private Broadcast deserializeBroadcast(Row row) {
        try {
            return broadcastSerializer.deserialize(
                    ContentProtos.Broadcast.parseFrom(
                            ByteString.copyFrom(BROADCAST.valueFrom(row))
                    )
            );
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
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

    private static class BroadcastRow {

        private final LocalDate day;
        private final String broadcastSourceId;

        private BroadcastRow(LocalDate day, String broadcastSourceId) {
            this.day = checkNotNull(day);
            this.broadcastSourceId = checkNotNull(broadcastSourceId);
        }

        public static BroadcastRow create(LocalDate day, String broadcastSourceId) {
            return new BroadcastRow(day, broadcastSourceId);
        }

        public LocalDate getDay() {
            return day;
        }

        public String getBroadcastSourceId() {
            return broadcastSourceId;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            BroadcastRow that = (BroadcastRow) o;
            return Objects.equals(day, that.day) &&
                    Objects.equals(broadcastSourceId, that.broadcastSourceId);
        }

        @Override
        public int hashCode() {
            return Objects.hash(day, broadcastSourceId);
        }

        @Override
        public String toString() {
            return "[" + day + "|" +broadcastSourceId + "]";
        }
    }
}
