package org.atlasapi.segment;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

import java.nio.ByteBuffer;
import java.util.Set;

import javax.annotation.Nullable;

import org.apache.cassandra.utils.Hex;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.CassandraUtil;

import com.datastax.driver.core.Row;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Equivalence;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.Iterables;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.persistence.cassandra.CassandraDataStaxClient;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class CassandraSegmentStore extends AbstractSegmentStore {

    private static final String SEGMENT = "column1";

    private final CassandraDataStaxClient cassandra;
    private final AliasIndex<Segment> aliasIndex;
    private final String keyspace;
    private final String tableName;
    private final SegmentSerializer segmentSerializer = new SegmentSerializer();

    public CassandraSegmentStore(CassandraDataStaxClient cassandra, String keyspace, String tableName,
                                 AliasIndex<Segment> aliasIndex, IdGenerator idGenerator,
                                 Equivalence<? super Segment> equivalence,
                                 MessageSender<ResourceUpdatedMessage> sender) {
        super(idGenerator, equivalence, sender, new SystemClock());
        this.cassandra = checkNotNull(cassandra);
        this.aliasIndex = checkNotNull(aliasIndex);
        this.keyspace = checkNotNull(keyspace);
        this.tableName = checkNotNull(tableName);
    }

    @Override
    protected void doWrite(Segment segment, Segment previous) {
        checkArgument(previous == null || segment.getPublisher().equals(previous.getPublisher()));
        try {
            long id = segment.getId().longValue();
            String query = QueryBuilder.insertInto(keyspace, tableName)
                    .value(CassandraUtil.KEY, id)
                    .value(SEGMENT, "segment")
                    .value(CassandraUtil.VALUE, ByteBuffer.wrap(segmentSerializer.serialize(segment)))
                    .setForceNoValues(true)
                    .getQueryString();
            cassandra.executeWriteQuery(query);
            /* TODO Write CQL implementation of AliasIndex so that these may be batched together */
            aliasIndex.mutateAliases(segment, previous).execute();
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }
    }

    @Nullable
    @Override
    protected Segment resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases) {

        Segment previous = null;
        if (id != null) {
            previous = resolveSegment(id).orNull();
            if (previous != null) {
                return previous;
            }
        }

        try {
            Set<Long> aliasIds = aliasIndex.readAliases(source, aliases);
            Long aliasId = Iterables.getFirst(aliasIds, null);
            if (aliasId != null) {
                return resolveSegment(Id.valueOf(aliasId)).orNull();
            }
        } catch (ConnectionException e) {
            throw Throwables.propagate(e);
        }

        return null;
    }

    @Override
    public Optional<Segment> resolveSegment(Id id) {
        String query = select().from(keyspace, tableName)
                .where(eq(CassandraUtil.KEY, id.longValue()))
                .getQueryString();
        Row result = cassandra.executeReadQuery(query).one();

        if (result == null) {
            return Optional.absent();
        }

        ByteBuffer buffer = result.getBytes(CassandraUtil.VALUE).slice();
        byte[] bytes = new byte[buffer.limit()];
        buffer.get(bytes, 0, buffer.limit());
        return Optional.of(segmentSerializer.deserialize(bytes));
    }

    public static Builder builder() {
        return new Builder();
    }

    public static class Builder {

        private CassandraDataStaxClient cassandra;
        private String keyspace;
        private String tableName;
        private AliasIndex<Segment> segmentIndex;
        private IdGenerator idGenerator;
        private Equivalence<Segment> equivalence;
        private MessageSender<ResourceUpdatedMessage> sender;

        public Builder() {

        }

        public Builder withDataStaxClient(CassandraDataStaxClient cassandra) {
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
