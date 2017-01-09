package org.atlasapi.organisation;

import org.atlasapi.entity.Id;

import com.metabroadcast.common.ids.IdGenerator;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

public class OrganisationUriStore implements OrganisationUriResolver {

    private static final String ORGANISATION_URI_TABLE = "organisation_uri";

    private static final String URI_COLUMN = "uri";
    private static final String SOURCE_COLUMN = "source";
    private static final String ID_COLUMN = "id";

    private final Session session;

    private final PreparedStatement uriSelect;
    private final PreparedStatement rowUpdate;

    protected OrganisationUriStore(Session session,
            ConsistencyLevel writeConsistency, ConsistencyLevel readConsistency) {
        this.session = checkNotNull(session);

        this.uriSelect = session.prepare(select(
                URI_COLUMN,
                SOURCE_COLUMN,
                ID_COLUMN
        )
                .from(ORGANISATION_URI_TABLE)
                .where(eq(URI_COLUMN, bindMarker(URI_COLUMN)))
                .and(eq(SOURCE_COLUMN, bindMarker(SOURCE_COLUMN))))
                .setConsistencyLevel(checkNotNull(readConsistency));

        this.rowUpdate = session.prepare(update(ORGANISATION_URI_TABLE)
                .where(eq(URI_COLUMN, bindMarker(URI_COLUMN)))
                .and(eq(SOURCE_COLUMN, bindMarker(SOURCE_COLUMN)))
                .with(set(ID_COLUMN, bindMarker(ID_COLUMN))))
                .setConsistencyLevel(checkNotNull(writeConsistency));
    }


    public Statement prepareWritingStatement(Organisation organisation) {
        String uri = organisation.getCanonicalUri();
        String source = organisation.getSource().key();
        Long id = organisation.getId().longValue();

        return rowUpdate
                .bind()
                .setString(URI_COLUMN, uri)
                .setString(SOURCE_COLUMN, source)
                .setLong(ID_COLUMN, id);
    }

    public ListenableFuture<Optional<Id>> getExistingId(Organisation organisation) {
        String uri = organisation.getCanonicalUri();
        String source = organisation.getSource().key();
        ResultSetFuture resultSetFuture = session.executeAsync(uriSelect.bind()
                .setString(URI_COLUMN, uri)
                .setString(SOURCE_COLUMN, source));
        return Futures.transform(
                resultSetFuture,
                (ResultSet input) -> {
                    Row row = input.one();
                    if (row != null) {
                        return Optional.of(Id.valueOf(row.getLong(ID_COLUMN)));
                    } else {
                        return Optional.absent();
                    }
                }
        );
    }

    private static class Builder implements SessionStep, WriteConsistencyStep,
            ReadConsistencyStep, BuildStep {

        private Session session;
        private ConsistencyLevel writeConsistency;
        private ConsistencyLevel readConsistency;
        private IdGenerator idGenerator;

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
        public OrganisationUriStore build() {
            return new OrganisationUriStore(
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

        OrganisationUriStore build();
    }

    public static SessionStep builder() {
        return new Builder();
    }

}
