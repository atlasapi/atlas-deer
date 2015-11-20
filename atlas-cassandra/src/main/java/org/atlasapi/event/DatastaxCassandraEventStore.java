package org.atlasapi.event;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.content.CorruptContentException;
import org.atlasapi.entity.Alias;
import org.atlasapi.entity.AliasIndex;
import org.atlasapi.entity.CassandraPersistenceException;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.media.entity.Publisher;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Session;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.netflix.astyanax.connectionpool.exceptions.ConnectionException;

public class DatastaxCassandraEventStore implements EventPersistenceStore {

    private static final String EVENT_TABLE = "event";
    private static final String PRIMARY_KEY_COLUMN = "event_id";
    private static final int TIMEOUT_IN_MINUTES = 1;

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final AliasIndex<Event> aliasIndex;
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;
    private final DatastaxProtobufEventMarshaller marshaller;

    private final PreparedStatement idsSelect;

    protected DatastaxCassandraEventStore(AliasIndex<Event> aliasIndex, Session session,
            ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        this.aliasIndex = checkNotNull(aliasIndex);
        this.session = checkNotNull(session);
        this.writeConsistency = checkNotNull(writeConsistency);
        this.readConsistency = checkNotNull(readConsistency);
        this.marshaller = new DatastaxProtobufEventMarshaller(new EventSerializer(), session);

        this.idsSelect = session.prepare(select().all()
                .from(EVENT_TABLE)
                .where(in(PRIMARY_KEY_COLUMN, bindMarker())));
        this.idsSelect.setConsistencyLevel(readConsistency);
    }

    @Override
    public ListenableFuture<Resolved<Event>> resolveIds(Iterable<Id> ids) {
        ResultSetFuture result = session.executeAsync(idsSelect.bind(
                StreamSupport.stream(ids.spliterator(), false)
                    .map(Id::longValue)
                    .collect(Collectors.toList())));

        return Futures.transform(
                result,
                (ResultSet input) -> {
                    return Resolved.valueOf(
                            StreamSupport.stream(input.spliterator(), false)
                                    .map(marshaller::unmarshall)
                                    .collect(Collectors.toList())
                    );
                }
        );
    }

    @Override
    public Optional<Event> resolvePrevious(Optional<Id> id, Publisher source,
            Iterable<Alias> aliases) {
        try {
            if (id.isPresent()) {
                return Futures.get(
                        resolveIds(ImmutableSet.of(id.get())),
                        TIMEOUT_IN_MINUTES, TimeUnit.MINUTES,
                        IOException.class
                ).getResources().first();
            } else {
                Set<Long> ids = aliasIndex.readAliases(source, aliases);
                Long aliasId = Iterables.getFirst(ids, null);
                if (aliasId != null) {
                    return Futures.get(
                            resolveIds(ImmutableSet.of(Id.valueOf(aliasId))),
                            TIMEOUT_IN_MINUTES, TimeUnit.MINUTES,
                            IOException.class
                    ).getResources().first();
                }
                return Optional.absent();
            }
        } catch (ConnectionException | IOException e) {
            throw Throwables.propagate(e);
        } catch (CorruptContentException e) {
            log.error("Previously written content is corrupt", e);
            return Optional.absent();
        }
    }

    @Override
    public void write(Event event, Event previous) {
        try {
            BatchStatement batch = new BatchStatement();
            batch.setConsistencyLevel(writeConsistency);
            marshaller.marshallInto(event.getId(), batch, event);
            aliasIndex.mutateAliasesAndExecute(event, previous);
            session.execute(batch);
        } catch (Exception e) {
            throw new CassandraPersistenceException(event.toString(), e);
        }
    }

    public static AliasIndexStep builder() {
        return new Builder();
    }

    public interface AliasIndexStep {
        SessionStep withAliasIndex(AliasIndex<Event> aliasIndex);
    }

    public interface SessionStep {
        WriteConsistencyStep withSession(Session session);
    }

    public interface WriteConsistencyStep {
        ReadConsistencyStep withWriteConsistency(ConsistencyLevel writeConsistency);
    }

    public interface ReadConsistencyStep {
        BuildStep withReadConsistency(ConsistencyLevel readConsistency);
    }

    public interface BuildStep {
        DatastaxCassandraEventStore build();
    }

    private static class Builder implements AliasIndexStep, SessionStep, WriteConsistencyStep,
            ReadConsistencyStep, BuildStep {

        private AliasIndex<Event> aliasIndex;
        private Session session;
        private ConsistencyLevel writeConsistency;
        private ConsistencyLevel readConsistency;

        private Builder() {}

        @Override
        public SessionStep withAliasIndex(AliasIndex<Event> aliasIndex) {
            this.aliasIndex = aliasIndex;
            return this;
        }

        @Override
        public WriteConsistencyStep withSession(Session session) {
            this.session = session;
            return this;
        }

        @Override
        public ReadConsistencyStep withWriteConsistency(ConsistencyLevel writeConsistency) {
            this.writeConsistency = writeConsistency;
            return this;
        }

        @Override
        public BuildStep withReadConsistency(ConsistencyLevel readConsistency) {
            this.readConsistency = readConsistency;
            return this;
        }

        @Override
        public DatastaxCassandraEventStore build() {
            return new DatastaxCassandraEventStore(
                    this.aliasIndex,
                    this.session,
                    this.writeConsistency,
                    this.readConsistency
            );
        }
    }
}
