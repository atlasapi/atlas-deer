package org.atlasapi.schedule;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;
import static org.atlasapi.util.Column.bigIntColumn;
import static org.atlasapi.util.Column.bytesColumn;
import static org.atlasapi.util.Column.dateColumn;
import static org.atlasapi.util.Column.textColumn;

import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.BroadcastRef;
import org.atlasapi.content.BroadcastSerializer;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.ContentSerializer;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.Equivalent;
import org.atlasapi.media.channel.Channel;
import org.atlasapi.media.entity.Identified;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.util.Column;
import org.joda.time.DateTime;
import org.joda.time.Interval;
import org.joda.time.LocalDate;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Query;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.datastax.driver.core.querybuilder.Update.Assignments;
import com.google.common.base.Function;
import com.google.common.base.Objects;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableList.Builder;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;
import com.metabroadcast.common.time.Clock;

public final class CassandraEquivalentScheduleStore extends AbstractEquivalentScheduleStore {

    private static final String EQUIVALENT_SCHEDULE_TABLE = "equivalent_schedule";
    
    private static final Column<String> SOURCE = textColumn("source");
    private static final Column<Long> CHANNEL = bigIntColumn("channel");
    private static final Column<Date> DAY = dateColumn("day");
    private static final Column<String> BROADCAST_ID = textColumn("broadcast_id");
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
    
    private final ContentSerializer contentSerializer = new ContentSerializer();
    private final EquivalenceGraphSerializer graphSerializer = new EquivalenceGraphSerializer();
    private final BroadcastSerializer broadcastSerializer = new BroadcastSerializer();
    
    public CassandraEquivalentScheduleStore(EquivalenceGraphStore graphStore,
            ContentResolver contentStore, Session session, ConsistencyLevel read,
            ConsistencyLevel write, Clock clock) {
        super(graphStore, contentStore);
        this.session = checkNotNull(session);
        this.read = checkNotNull(read);
        this.write = checkNotNull(write);
        this.clock = checkNotNull(clock);
    }

    @Override
    public ListenableFuture<EquivalentSchedule> resolveSchedules(Iterable<Channel> channels,
            final Interval interval, Publisher source, final Set<Publisher> selectedSources) {
        final Set<Channel> chans = ImmutableSet.copyOf(channels);
        List<Query> selects = selectStatements(source, channels, interval);
        ListenableFuture<List<ResultSet>> results = Futures.allAsList(Lists.transform(selects, new Function<Query, ListenableFuture<ResultSet>>(){
            @Override
            public ListenableFuture<ResultSet> apply(Query input) {
                return session.executeAsync(input.setConsistencyLevel(read));
            }
        }));
        return Futures.transform(results, new Function<List<ResultSet>, EquivalentSchedule>(){
            @Override
            public EquivalentSchedule apply(List<ResultSet> input) {
                return new EquivalentSchedule(toChannelSchedules(input, chans, interval), interval);
            }

            private List<EquivalentChannelSchedule> toChannelSchedules(List<ResultSet> input,
                    Iterable<Channel> channels, final Interval interval) {
                ImmutableMap<Long, List<EquivalentScheduleEntry>> entriesByChannel = transformToEntries(input);
                ImmutableList.Builder<EquivalentChannelSchedule> channelSchedules = ImmutableList.builder();
                for (Channel channel : channels) {
                    List<EquivalentScheduleEntry> entries = channelEntries(entriesByChannel, channel);
                    channelSchedules.add(new EquivalentChannelSchedule(channel, interval, entries));
                }
                return channelSchedules.build();
            }

            private List<EquivalentScheduleEntry> channelEntries(
                    ImmutableMap<Long, List<EquivalentScheduleEntry>> channelEntryIndex,
                    Channel channel) {
                return Objects.firstNonNull(channelEntryIndex.get(channel.getId()), ImmutableList.<EquivalentScheduleEntry>of());
            }

            private ImmutableMap<Long, List<EquivalentScheduleEntry>> transformToEntries(
                    List<ResultSet> input) {
                ImmutableMap.Builder<Long, List<EquivalentScheduleEntry>> channelEntries = ImmutableMap.builder();
                for (ResultSet resultSet : input) {
                    if (!resultSet.isExhausted()) {
                        channelEntries.put(entriesFor(resultSet));
                    }
                }
                return channelEntries.build();
            }

            private Entry<Long, ? extends List<EquivalentScheduleEntry>> entriesFor(ResultSet input) {
                Long channel = null;
                ImmutableList.Builder<EquivalentScheduleEntry> entries = ImmutableList.builder();
                for (Row row : input) {
                    if (channel == null) {
                        channel = CHANNEL.valueFrom(row);
                    }
                    try {
                        Broadcast broadcast = deserialize(BROADCAST.valueFrom(row));
                        Equivalent<Item> equivItems = equivItems(row);
                        entries.add(new EquivalentScheduleEntry(broadcast, equivItems));
                    } catch (IOException e) {
                        // has to be unchecked. is there a better type? 
                        // does it matter since we're in a future?
                        throw new RuntimeException("error reading "+row, e);
                    }
                }
                return Maps.immutableEntry(channel, entries.build());
            }

            private Equivalent<Item> equivItems(Row row) throws IOException {
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
                    if (selectedSources.contains(item.getPublisher())) {
                        items.add(item);
                    }
                }
                return new Equivalent<Item>(graph, items.build());
            }
            
            private Broadcast deserialize(ByteBuffer bcastBytes) throws InvalidProtocolBufferException {
                return broadcastSerializer.deserialize(ContentProtos.Broadcast.parseFrom(ByteString.copyFrom(bcastBytes)));
            }
        });
    }

    private List<Query> selectStatements(Publisher src, Iterable<Channel> channels, Interval interval) {
        ImmutableList.Builder<Query> selects = ImmutableList.builder();
        Object[] days = daysIn(interval); 
        for (Channel channel : channels) {
            selects.add(QueryBuilder.select().all()
                .from(EQUIVALENT_SCHEDULE_TABLE)
                .where(eq(SOURCE.name(), src.key()))
                    .and(eq(CHANNEL.name(), channel.getId()))
                    .and(in(DAY.name(), days)));
        }
        return selects.build();
    }

    private Date[] daysIn(Interval interval) {
        return Iterables.toArray(Iterables.transform(new IntervalDates(interval), 
            new Function<LocalDate, Date>() {
                @Override
                public Date apply(LocalDate input) {
                    return input.toDate();
                }
            }
        ), Date.class);
    }

    @Override
    protected synchronized void writeSchedule(ScheduleUpdate update, Map<ScheduleRef.Entry, EquivalentScheduleEntry> content)
            throws WriteException {
        DateTime now = clock.now();
        Query updateBatch = batch(Iterables.toArray(Iterables.concat(
            updateStatements(update.getSource(), update.getSchedule(), content, now),
            deleteStatements(update.getSource(), update.getStaleBroadcasts())
        ), Statement.class)).setConsistencyLevel(write);
        session.execute(updateBatch);
    }

    private Iterable<Statement> updateStatements(Publisher source, ScheduleRef scheduleRef, Map<ScheduleRef.Entry, EquivalentScheduleEntry> content, DateTime now)
            throws WriteException {
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
    
    private void statementsForEntry(ImmutableList.Builder<Statement> stmts, Publisher source, ScheduleRef.Entry entry,
            EquivalentScheduleEntry content, DateTime now) throws WriteException {
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

    private Statement updateStatement(Publisher source, Date day, ScheduleRef.Entry entry,
            int contentCount, ByteBuffer content, ByteBuffer graph, ByteBuffer broadcast, DateTime now) {
        return updateStatement(source, entry.getBroadcast().getChannelId(),
            day, entry.getBroadcast().getSourceId(), broadcast, graph, contentCount, content
        ).and(set(SCHEDULE_UPDATE.name(), now.toDate()));
    }

    private ByteBuffer serialize(ImmutableSet<? extends Content> resources) throws WriteException {
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

    private Iterable<Statement> deleteStatements(Publisher src, ImmutableSet<BroadcastRef> staleBroadcasts) {
        ImmutableList.Builder<Statement> stmts = ImmutableList.builder();
        for (BroadcastRef ref : staleBroadcasts) {
            for (Date day : daysIn(ref.getTransmissionInterval())) {
                stmts.add(delete(ref, src, day));
            }
        }
        return stmts.build();
    }

    private Statement delete(BroadcastRef ref, Publisher src, Date date) {
        return QueryBuilder.delete().all().from(EQUIVALENT_SCHEDULE_TABLE)
            .where(eq(SOURCE.name(), src.key()))
                .and(eq(CHANNEL.name(), ref.getChannelId().longValue()))
                .and(eq(DAY.name(), date))
                .and(eq(BROADCAST_ID.name(), ref.getSourceId()));
    }

    @Override
    protected synchronized void updateEquivalentContent(Publisher publisher, Broadcast bcast, 
            EquivalenceGraph graph, ImmutableSet<Item> content) throws WriteException {
        DateTime now = clock.now();
        
        ByteBuffer bcastBytes = serialize(bcast);
        ByteBuffer graphBytes = graphSerializer.serialize(graph);
        ByteBuffer contentBytes = serialize(content);
        
        ImmutableList.Builder<Statement> stmts = ImmutableList.builder();
        for (Date day : daysIn(bcast.getTransmissionInterval())) {
            stmts.add(updateStatement(publisher, bcast.getChannelId(), day, 
                    bcast.getSourceId(), bcastBytes, graphBytes, content.size(), contentBytes)
                    .and(set(EQUIV_UPDATE.name(),now.toDate())));
        }
        
        session.execute(batch(Iterables.toArray(stmts.build(), Statement.class))
                .setConsistencyLevel(write));
    }

    private Assignments updateStatement(Publisher publisher, Id channelId, Date day, String bcastId,
            ByteBuffer bcastBytes, ByteBuffer graphBytes, int contentCount, ByteBuffer contentBytes) {
        return update(EQUIVALENT_SCHEDULE_TABLE)
            .where(eq(SOURCE.name(), publisher.key()))
                .and(eq(CHANNEL.name(), channelId.longValue()))
                .and(eq(DAY.name(), day))
                .and(eq(BROADCAST_ID.name(), bcastId))
            .with(set(BROADCAST.name(), bcastBytes))
                .and(set(GRAPH.name(), graphBytes))
                .and(set(CONTENT_COUNT.name(), contentCount))
                .and(set(CONTENT.name(), contentBytes));
    }


}
