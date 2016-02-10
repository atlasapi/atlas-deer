package org.atlasapi.organisation;

import java.nio.ByteBuffer;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.serialization.protobuf.CommonProtos;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Throwables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.protobuf.InvalidProtocolBufferException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class DatastaxCassandraOrganisationStore implements OrganisationStore {

    private static final String ORGANISATION_TABLE = "organisation";
    private static final String PRIMARY_KEY_COLUMN = "organisation_id";
    private static final String DATA_COLUMN = "data";

    private final String KEYS = "keys";
    private final String ORGANISATION_ID = "organisationId";
    private final String DATA = "data";

    private final Logger log = LoggerFactory.getLogger(getClass());

    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;
    private final OrganisationSerializer serializer;
    private final PreparedStatement rowUpdate;
    private final PreparedStatement selectStatement;

    protected DatastaxCassandraOrganisationStore(Session session,
            ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        this.session = checkNotNull(session);
        this.readConsistency = checkNotNull(readConsistency);
        this.serializer = new OrganisationSerializer();
        this.writeConsistency = checkNotNull(writeConsistency);

        RegularStatement statement = select().all()
                .from(ORGANISATION_TABLE)
                .where(in(PRIMARY_KEY_COLUMN, bindMarker(KEYS)));
        statement.setFetchSize(Integer.MAX_VALUE);
        selectStatement = session.prepare(statement);

        rowUpdate = session.prepare(update(ORGANISATION_TABLE)
                .where(eq(PRIMARY_KEY_COLUMN, bindMarker(ORGANISATION_ID)))
                .with(set(DATA_COLUMN, bindMarker(DATA))))
                .setConsistencyLevel(writeConsistency);
    }

    @Override
    public ListenableFuture<Resolved<Organisation>> resolveIds(Iterable<Id> ids) {
        Statement select = selectStatement.bind().setList(
                KEYS,
                StreamSupport.stream(ids.spliterator(), false)
                        .map(Id::longValue)
                        .collect(Collectors.toList())
        )
                .setConsistencyLevel(readConsistency);

        ResultSetFuture result = session.executeAsync(select);

        return Futures.transform(
                result,
                (ResultSet input) -> {
                    return Resolved.valueOf(
                            StreamSupport.stream(input.spliterator(), false)
                                    .map(this::extractOrganisation)
                                    .collect(Collectors.toList())
                    );
                }
        );
    }

    @Override
    public void write(Organisation organisation) {
        ByteBuffer serializedOrganisation = ByteBuffer.wrap(serializer.serialize(organisation)
                .toByteArray());

        session.execute(rowUpdate.bind()
                .setLong(ORGANISATION_ID, organisation.getId().longValue())
                .setBytes(DATA, serializedOrganisation));
    }

    protected Organisation extractOrganisation(Row row) {
        ByteBuffer buffer = row.getBytes(DATA_COLUMN);
        byte[] bytes = new byte[buffer.remaining()];
        buffer.get(bytes);
        try {
            return serializer.deserialize(CommonProtos.Organisation.parseFrom(bytes));
        } catch (InvalidProtocolBufferException e) {
            throw Throwables.propagate(e);
        }
    }

    private static class Builder implements SessionStep, WriteConsistencyStep,
            ReadConsistencyStep, BuildStep {

        private Session session;
        private ConsistencyLevel writeConsistency;
        private ConsistencyLevel readConsistency;

        private Builder() {
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
        public DatastaxCassandraOrganisationStore build() {
            return new DatastaxCassandraOrganisationStore(
                    this.session,
                    this.writeConsistency,
                    this.readConsistency
            );
        }
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

        DatastaxCassandraOrganisationStore build();
    }

    public static SessionStep builder() {
        return new Builder();
    }
}