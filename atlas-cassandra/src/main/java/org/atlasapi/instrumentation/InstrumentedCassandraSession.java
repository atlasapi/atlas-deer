package org.atlasapi.instrumentation;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.ListenableFuture;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedCassandraSession implements Session {

    private final Session sut;

    public InstrumentedCassandraSession(Session sut) {
        this.sut = checkNotNull(sut);
    }

    @Override
    public String getLoggedKeyspace() {
        return sut.getLoggedKeyspace();
    }

    @Override
    public Session init() {
        return sut.init();
    }

    @Override
    public ResultSet execute(String query) {
        return sut.execute(query);
    }

    @Override
    public ResultSet execute(String query, Object... values) {
        return execute(query, values);
    }

    @Override
    public ResultSet execute(Statement statement) {
        return sut.execute(statement);
    }

    @Override
    public ResultSetFuture executeAsync(String query) {
        return sut.executeAsync(query);
    }

    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
        return sut.executeAsync(query, values);
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        return sut.executeAsync(statement);
    }

    @Override
    public PreparedStatement prepare(String query) {
        return sut.prepare(query);
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        return sut.prepare(statement);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        return sut.prepareAsync(query);
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
        return sut.prepareAsync(statement);
    }

    @Override
    public CloseFuture closeAsync() {
        return sut.closeAsync();
    }

    @Override
    public void close() {
        sut.close();
    }

    @Override
    public boolean isClosed() {
        return sut.isClosed();
    }

    @Override
    public Cluster getCluster() {
        return sut.getCluster();
    }

    @Override
    public State getState() {
        return sut.getState();
    }
}
