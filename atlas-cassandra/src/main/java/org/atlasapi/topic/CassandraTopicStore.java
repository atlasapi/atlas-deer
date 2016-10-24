package org.atlasapi.topic;

import java.util.Set;
import java.util.concurrent.TimeUnit;

import javax.annotation.Nullable;

import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.CassandraPersistenceException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.ResourceUpdatedMessage;
import org.atlasapi.util.CassandraUtil;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.ids.IdGenerator;
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.time.Clock;
import com.metabroadcast.common.time.SystemClock;

import com.codahale.metrics.Meter;
import com.codahale.metrics.MetricRegistry;
import com.google.common.base.Equivalence;
import com.google.common.base.Function;
import com.google.common.base.Joiner;
import com.google.common.base.Optional;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.AstyanaxContext;
import com.netflix.astyanax.Keyspace;
import com.netflix.astyanax.MutationBatch;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;
import com.netflix.astyanax.connectionpool.exceptions.NotFoundException;
import com.netflix.astyanax.model.Column;
import com.netflix.astyanax.model.ColumnFamily;
import com.netflix.astyanax.model.ColumnList;
import com.netflix.astyanax.model.ConsistencyLevel;
import com.netflix.astyanax.model.Row;
import com.netflix.astyanax.model.Rows;
import com.netflix.astyanax.query.ColumnQuery;
import com.netflix.astyanax.serializers.LongSerializer;
import com.netflix.astyanax.serializers.StringSerializer;

import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraTopicStore extends AbstractTopicStore {

    public static final Builder builder(AstyanaxContext<Keyspace> context,
            String name, Equivalence<? super Topic> equivalence,
            MessageSender<ResourceUpdatedMessage> sender, IdGenerator idGenerator) {
        return new Builder(context, name, equivalence, sender, idGenerator);
    }

    public static final class Builder {

        private final AstyanaxContext<Keyspace> context;
        private final String name;
        private final Equivalence<? super Topic> equivalence;
        private final IdGenerator idGenerator;
        private final MessageSender<ResourceUpdatedMessage> sender;

        private ConsistencyLevel readCl = ConsistencyLevel.CL_QUORUM;
        private ConsistencyLevel writeCl = ConsistencyLevel.CL_QUORUM;
        private Clock clock = new SystemClock();
        private MetricRegistry metricRegistry;
        private String metricPrefix;

        public Builder(AstyanaxContext<Keyspace> context, String name,
                Equivalence<? super Topic> equivalence,
                MessageSender<ResourceUpdatedMessage> sender, IdGenerator idGenerator) {
            this.context = context;
            this.name = name;
            this.equivalence = equivalence;
            this.sender = sender;
            this.idGenerator = idGenerator;
        }

        public Builder withReadConsistency(ConsistencyLevel readCl) {
            this.readCl = readCl;
            return this;
        }

        public Builder withWriteConsistency(ConsistencyLevel writeCl) {
            this.writeCl = writeCl;
            return this;
        }

        public Builder withClock(Clock clock) {
            this.clock = clock;
            return this;
        }

        public Builder withMetricRegistry(MetricRegistry metricRegistry) {
            this.metricRegistry = metricRegistry;
            return this;
        }

        public Builder withMetricPrefix(String metricPrefix) {
            this.metricPrefix = metricPrefix;
            return this;
        }

        public CassandraTopicStore build() {
            return new CassandraTopicStore(context, name, readCl, writeCl,
                    equivalence, idGenerator, sender, clock, metricRegistry, metricPrefix
            );
        }

    }

    private final Keyspace keyspace;
    private final ConsistencyLevel readConsistency;
    private final ConsistencyLevel writeConsistency;
    private final ColumnFamily<Long, String> mainCf;
    private final AliasIndex<Topic> aliasIndex;

    private final Meter calledMeter;
    private final Meter failureMeter;

    private final String valueColumn = "topic";
    private final TopicSerializer topicSerializer = new TopicSerializer();
    private final Function<Row<Long, String>, Topic> rowToTopic =
            input -> {
                ColumnList<String> cols = input.getColumns();
                Column<String> col = cols.getColumnByName(valueColumn);
                if (col == null) {
                    return null;
                }
                return topicSerializer.deserialize(col.getByteArrayValue());
            };
    private final Function<Rows<Long, String>, Resolved<Topic>> toResolved =
            rows -> Resolved.valueOf(FluentIterable.from(rows)
                    .transform(rowToTopic)
                    .filter(Predicates.notNull()));

    public CassandraTopicStore(
            AstyanaxContext<Keyspace> context,
            String cfName,
            ConsistencyLevel readCl,
            ConsistencyLevel writeCl,
            Equivalence<? super Topic> equivalence,
            IdGenerator idGenerator,
            MessageSender<ResourceUpdatedMessage> sender,
            Clock clock,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(idGenerator, equivalence, sender, clock, metricRegistry, metricPrefix);
        this.keyspace = checkNotNull(context.getClient());
        this.readConsistency = checkNotNull(readCl);
        this.writeConsistency = checkNotNull(writeCl);
        this.mainCf = ColumnFamily.newColumnFamily(checkNotNull(cfName),
                LongSerializer.get(), StringSerializer.get()
        );
        this.aliasIndex = AliasIndex.create(keyspace, cfName + "_aliases");

        this.calledMeter = checkNotNull(metricRegistry).meter(checkNotNull(metricPrefix) + "meter.called");
        this.failureMeter = checkNotNull(metricRegistry).meter(checkNotNull(metricPrefix) + "meter.failure");

    }

    @Override
    public ListenableFuture<Resolved<Topic>> resolveIds(Iterable<Id> ids) {
        try {
            Iterable<Long> longIds = Iterables.transform(ids, Id.toLongValue());
            return Futures.transform(resolveLongs(longIds), toResolved);
        } catch (Exception e) {
            throw new CassandraPersistenceException(Joiner.on(", ").join(ids), e);
        }
    }

    private ListenableFuture<Rows<Long, String>> resolveLongs(Iterable<Long> longIds)
            throws ConnectionException {
        return Futures.transform(keyspace
                .prepareQuery(mainCf)
                .setConsistencyLevel(readConsistency)
                .getKeySlice(longIds)
                .executeAsync(), CassandraUtil.<Rows<Long, String>>toResult());
    }

    @Override
    public OptionalMap<Alias, Topic> resolveAliases(Iterable<Alias> aliases, Publisher source) {
        try {
            Set<Alias> uniqueAliases = ImmutableSet.copyOf(aliases);
            Set<Long> ids = aliasIndex.readAliases(source, uniqueAliases);
            // TODO: move timeout to config
            Rows<Long, String> resolved = resolveLongs(ids).get(1, TimeUnit.MINUTES);
            Iterable<Topic> topics = Iterables.transform(resolved, rowToTopic);
            ImmutableMap.Builder<Alias, Optional<Topic>> aliasMap = ImmutableMap.builder();
            for (Topic topic : topics) {
                topic.getAliases()
                        .stream()
                        .filter(uniqueAliases::contains)
                        .forEach(alias -> aliasMap.put(alias, Optional.of(topic)));
            }
            return ImmutableOptionalMap.copyOf(aliasMap.build());
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    protected void doWrite(Topic topic, @Nullable Topic previous) {
        //Allow migration of owl content that has changed source
        //checkArgument(previous == null || topic.getSource().equals(previous.getSource()));
        calledMeter.mark();
        try {
            long id = topic.getId().longValue();
            MutationBatch batch = keyspace.prepareMutationBatch();
            batch.setConsistencyLevel(writeConsistency);
            batch.withRow(mainCf, id)
                    .putColumn(valueColumn, topicSerializer.serialize(topic));
            batch.mergeShallow(aliasIndex.mutateAliases(topic, previous));
            batch.execute();
        } catch (Exception e) {
            failureMeter.mark();
            throw new CassandraPersistenceException(topic.toString(), e);
        }
    }

    @Override
    @Nullable
    protected Topic resolvePrevious(@Nullable Id id, Publisher source, Set<Alias> aliases) {
        Topic previous = null;
        if (id != null) {
            previous = resolve(id.longValue());
        }

        if (previous == null) {
            try {
                Set<Long> ids = aliasIndex.readAliases(source, aliases);
                Long aliasId = Iterables.getFirst(ids, null);
                if (aliasId != null) {
                    previous = resolve(aliasId);
                }
            } catch (ConnectionException e) {
                throw Throwables.propagate(e);
            }
        }
        return previous;
    }

    private Topic resolve(long longId) {
        try {
            ColumnQuery<String> query = keyspace.prepareQuery(mainCf)
                    .getKey(longId).getColumn(valueColumn);
            Column<String> col = query.execute().getResult();

            return topicSerializer.deserialize(col.getByteArrayValue());
        } catch (NotFoundException e) {
            return null;
        } catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

}
