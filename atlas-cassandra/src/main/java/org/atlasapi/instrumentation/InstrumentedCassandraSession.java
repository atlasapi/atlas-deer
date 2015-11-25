package org.atlasapi.instrumentation;

import com.datastax.driver.core.Session;
import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.CloseFuture;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.PreparedStatement;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.concurrent.atomic.AtomicLong;

import static com.google.common.base.Preconditions.checkNotNull;

public class InstrumentedCassandraSession implements Session {
    private static final Logger LOG = LoggerFactory.getLogger(InstrumentedCassandraSession.class);
    private final Session sut;

    private AtomicLong callId = new AtomicLong(0);

    public InstrumentedCassandraSession(Session sut) {
        this.sut = checkNotNull(sut);
        LOG.info("Cassandra Session Instrumentation Installed");
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
        long id = startObserving("executeQ", query);
        ResultSet result = sut.execute(query);
        finishObserving(id);
        return result;
    }

    @Override
    public ResultSet execute(String query, Object... values) {
        long id = startObserving("executeQO", query);
        ResultSet result = sut.execute(query, values);
        finishObserving(id);
        return result;
    }

    @Override
    public ResultSet execute(Statement statement) {
        long id = startObserving("executeS", statement.toString());
        ResultSet result = sut.execute(statement);
        finishObserving(id);
        return result;
    }

    @Override
    public ResultSetFuture executeAsync(String query) {
        long id = startObserving("executeAsyncQ", query);
        ResultSetFuture result = sut.executeAsync(query);
        finishObservingAsync(id, result);
        return result;
    }

    @Override
    public ResultSetFuture executeAsync(String query, Object... values) {
        long id = startObserving("executeAsyncQO", query);
        ResultSetFuture result = sut.executeAsync(query, values);
        finishObservingAsync(id, result);
        return result;
    }

    @Override
    public ResultSetFuture executeAsync(Statement statement) {
        long id = startObserving("executeAsyncS", statement.toString());
        ResultSetFuture result = sut.executeAsync(statement);
        finishObservingAsync(id, result);
        return result;
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


    private long startObserving(String methodName, String details) {
        long id = callId.getAndIncrement();
        LOG.info("--> " + id + " " + methodName + " " + details);
        return id;
    }

    private void finishObservingAsync(long id, ResultSetFuture observee) {

        Futures.addCallback(observee, new FutureCallback<ResultSet>() {
            @Override
            public void onSuccess(ResultSet resultSetFuture) {
                LOG.info("<-- " + id);
            }

            @Override
            public void onFailure(Throwable thrown) {
                LOG.info("<-- " + id + " Error: " + thrown.toString());
            }
        });

    }

    private void finishObserving(long id) {
        LOG.info("<-- " + id);
    }

}
