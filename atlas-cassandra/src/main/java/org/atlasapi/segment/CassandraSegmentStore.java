package org.atlasapi.segment;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.CassandraUtil;

import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraSegmentStore extends AbstractSegmentStore {

    private static final String SEGMENT = "column1";
    public static final int SELECT_TIMEOUT = 10;

    private final Session session;
    private final AliasIndex<Segment> aliasIndex;
    private final String keyspace;
    private final String tableName;
    private final SegmentSerializer segmentSerializer = new SegmentSerializer();
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PreparedStatement segmentInsert;
    private final PreparedStatement segmentSelect;

    private final Meter calledMeter;
    private final Meter failureMeter;

    private CassandraSegmentStore(
            Session session,
            String keyspace,
            String tableName,
            AliasIndex<Segment> aliasIndex,
            IdGenerator idGenerator,
            Equivalence<? super Segment> equivalence,
            MessageSender<ResourceUpdatedMessage> sender,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(idGenerator, equivalence, sender, new SystemClock());
        this.session = checkNotNull(session);
        this.aliasIndex = checkNotNull(aliasIndex);
        this.keyspace = checkNotNull(keyspace);
        this.tableName = checkNotNull(tableName);

        this.segmentInsert = session.prepare(insertInto(keyspace, tableName)
                .value(CassandraUtil.KEY, bindMarker("id"))
                .value(SEGMENT, "segment")
                .value(CassandraUtil.VALUE, bindMarker("data"))
                .setForceNoValues(true));

        this.segmentSelect = session.prepare(select().from(keyspace, tableName)
                .where(eq(CassandraUtil.KEY, bindMarker()))
                .setForceNoValues(true));

        calledMeter = metricRegistry.meter(metricPrefix + "meter.called");
        failureMeter = metricRegistry.meter(metricPrefix + "meter.failure");
    }

    @Override
    protected void doWrite(Segment segment, Segment previous) {
        calledMeter.mark();
        checkArgument(
                previous == null || segment.getSource().equals(previous.getSource()),
                "Cannot change the Source of a Segment!"
        );
        try {
            log.trace("Writing Segment {}", segment.getId());
            long id = segment.getId().longValue();
            session.execute(segmentInsert.bind()
                    .setLong("id", id)
                    .setBytes("data", ByteBuffer.wrap(segmentSerializer.serialize(segment))));
            /* TODO Write CQL implementation of AliasIndex so that these may be batched together */
            aliasIndex.mutateAliases(segment, previous).execute();
        } catch (ConnectionException e) {
            failureMeter.mark();
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Optional<Segment> resolvePrevious(@Nullable Id id, Publisher source,
            Set<Alias> aliases) {

        Segment previous = null;
        if (id != null) {
            previous = Iterables.getOnlyElement(resolveSegments(ImmutableList.of(id)), null);
            if (previous != null) {
                return Optional.of(previous);
            }
        }

        return Optional.fromNullable(resolveByAlias(source, aliases));
    }

    private Segment resolveByAlias(Publisher source, Set<Alias> aliases) {
        try {
            Set<Long> aliasIds = aliasIndex.readAliases(source, aliases);
            Long aliasId = Iterables.getFirst(aliasIds, null);
            if (aliasId == null) {
                return null;
            }
            return Iterables.getOnlyElement(
                    resolveSegments(ImmutableList.of(Id.valueOf(aliasId))),
                    null
            );
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Iterable<Segment> resolveSegments(Iterable<Id> ids) {
        return transformResultSet(executeReadQuery(ids));
    }

    private Iterable<Row> executeReadQuery(Iterable<Id> ids) {
        ListenableFuture<List<Row>> rowFuture = Futures.transform(
                Futures.allAsList(StreamSupport.stream(ids.spliterator(), false)
                        .map(Id.toLongValue()::apply)
                        .map(segmentSelect::bind)
                        .map(session::executeAsync)
                        .map(rsFuture -> Futures.transform(
                                rsFuture,
                                (Function<ResultSet, List<Row>>) input ->
                                        input != null ? input.all() : Lists.newArrayList()
                        ))
                        .collect(Collectors.toList())),
                (Function<List<List<Row>>, List<Row>>) input -> input.stream()
                        .flatMap(Collection::stream)
                        .collect(MoreCollectors.toImmutableList())
        );
        try {
            return rowFuture.get(SELECT_TIMEOUT, TimeUnit.SECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private Iterable<Segment> transformResultSet(Iterable<Row> rows) {
        ImmutableList.Builder<Segment> list = ImmutableList.builder();
        for (Row row : rows) {
            ByteBuffer buffer = row.getBytes(CassandraUtil.VALUE).slice();
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes, 0, buffer.limit());
            list.add(segmentSerializer.deserialize(bytes));
        }
        return list.build();
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private Session cassandra;
        private String keyspace;
        private String tableName;
        private AliasIndex<Segment> segmentIndex;
        private IdGenerator idGenerator;
        private Equivalence<Segment> equivalence;
        private MessageSender<ResourceUpdatedMessage> sender;
        private MetricRegistry metricRegistry;
        private String metricPrefix;

        public Builder() {

        }

        public Builder withCassandraSession(Session cassandra) {
            this.cassandra = cassandra;
            return this;
        }

        public Builder withKeyspace(String keyspace) {
            this.keyspace = keyspace;
            return this;
        }

        public Builder withTableName(String tableName) {
            this.tableName = tableName;
            return this;
        }

        public Builder withAliasIndex(AliasIndex<Segment> segmentIndex) {
            this.segmentIndex = segmentIndex;
            return this;
        }

        public Builder withIdGenerator(IdGenerator idGenerator) {
            this.idGenerator = idGenerator;
            return this;
        }

        public Builder withEquivalence(Equivalence<Segment> equivalence) {
            this.equivalence = equivalence;
            return this;
        }

        public Builder withMessageSender(MessageSender<ResourceUpdatedMessage> sender) {
            this.sender = sender;
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

        public CassandraSegmentStore build() {
            return new CassandraSegmentStore(
                    cassandra,
                    keyspace,
                    tableName,
                    segmentIndex,
                    idGenerator,
                    equivalence,
                    sender,
                    metricRegistry,
                    metricPrefix
            );
        }
    }
}
