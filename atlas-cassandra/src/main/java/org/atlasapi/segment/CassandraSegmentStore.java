package org.atlasapi.segment;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import javax.annotation.Nullable;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.CassandraUtil;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.google.common.base.Equivalence;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.SystemClock;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class CassandraSegmentStore extends AbstractSegmentStore {

   private static final String SEGMENT = "column1";

    private final Session session;
    private final AliasIndex<Segment> aliasIndex;
    private final String keyspace;
    private final String tableName;
    private final SegmentSerializer segmentSerializer = new SegmentSerializer();
    private final Logger log = LoggerFactory.getLogger(getClass());

    private final PreparedStatement segmentInsert;
    private final PreparedStatement segmentsSelect;

    private CassandraSegmentStore(Session session, String keyspace, String tableName,
                                 AliasIndex<Segment> aliasIndex, IdGenerator idGenerator,
                                 Equivalence<? super Segment> equivalence,
                                 MessageSender<ResourceUpdatedMessage> sender) {
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

        this.segmentsSelect = session.prepare(select().from(keyspace, tableName)
                .where(in(CassandraUtil.KEY, bindMarker()))
                .setForceNoValues(true));
    }

    @Override
    protected void doWrite(Segment segment, Segment previous) {
        checkArgument(previous == null || segment.getSource().equals(previous.getSource()),
                "Cannot change the Source of a Segment!");
        try {
            log.trace("Writing Segment {}", segment.getId());
            long id = segment.getId().longValue();
            session.execute(segmentInsert.bind()
                    .setLong("id", id)
                    .setBytes("data", ByteBuffer.wrap(segmentSerializer.serialize(segment))));
            /* TODO Write CQL implementation of AliasIndex so that these may be batched together */
            aliasIndex.mutateAliases(segment, previous).execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected Optional<Segment> resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases) {

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
            return Iterables.getOnlyElement(resolveSegments(ImmutableList.of(Id.valueOf(aliasId))), null);
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Iterable<Segment> resolveSegments(Iterable<Id> ids) {
        ResultSet rows = executeReadQuery(ids);

        if (rows == null || rows.isExhausted()) {
            return ImmutableList.of();
        }

        return transformResultSet(rows);
    }

    private ResultSet executeReadQuery(Iterable<Id> ids) {
        return session.execute(segmentsSelect.bind(idsToList(ids)));
    }

    private Iterable<Segment> transformResultSet(ResultSet rows) {
        ImmutableList.Builder<Segment> list = ImmutableList.builder();
        while (!rows.isExhausted()) {
            ByteBuffer buffer = rows.one().getBytes(CassandraUtil.VALUE).slice();
            byte[] bytes = new byte[buffer.limit()];
            buffer.get(bytes, 0, buffer.limit());
            list.add(segmentSerializer.deserialize(bytes));
        }
        return list.build();
    }

    private List<Long> idsToList(Iterable<Id> ids) {
        return StreamSupport.stream(ids.spliterator(), false)
                .map(Id::longValue)
                .collect(Collectors.toList());
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

        public CassandraSegmentStore build() {
            return new CassandraSegmentStore(
                    cassandra,
                    keyspace,
                    tableName,
                    segmentIndex,
                    idGenerator,
                    equivalence,
                    sender
            );
        }
    }
}
