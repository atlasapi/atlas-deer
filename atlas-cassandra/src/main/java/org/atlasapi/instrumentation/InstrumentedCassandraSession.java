package org.atlasapi.instrumentation;

import com.datastax.driver.core.*;
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

    // we need this id to unique across all instances of Session
    // Atlas-Deer appears to create 5 (but this might be getSession calls)
    private static AtomicLong callId = new AtomicLong(0);

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
        long id = startObserving("executeS", toMessage(statement));
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
        long id = startObserving("executeAsyncS", toMessage(statement));
        ResultSetFuture result = sut.executeAsync(statement);
        finishObservingAsync(id, result);
        return result;
    }

    @Override
    public PreparedStatement prepare(String query) {
        long id = startObserving("prepareQ", query);
        PreparedStatement result = sut.prepare(query);
        finishObserving(id);
        return result;
    }

    @Override
    public PreparedStatement prepare(RegularStatement statement) {
        long id = startObserving("prepareS", statement.toString());
        PreparedStatement result = sut.prepare(statement);
        finishObserving(id);
        return result;
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(String query) {
        long id = startObserving("prepareAsyncQ", query);
        ListenableFuture<PreparedStatement> result = sut.prepareAsync(query);
        finishObservingAsync(id, result);
        return result;
    }

    @Override
    public ListenableFuture<PreparedStatement> prepareAsync(RegularStatement statement) {
        long id = startObserving("prepareAsyncS", statement.toString());
        ListenableFuture<PreparedStatement> result = sut.prepareAsync(statement);
        finishObservingAsync(id, result);
        return result;
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

    private void finishObservingAsync(long id, ListenableFuture<PreparedStatement> observee) {

        Futures.addCallback(observee, new FutureCallback<PreparedStatement>() {
            @Override
            public void onSuccess(PreparedStatement resultSetFuture) {
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


    private String toMessage(Statement statement) {
        String message = statement instanceof BoundStatement
                         ? ((BoundStatement) statement).preparedStatement().getQueryString()
                         : statement.toString();

        return message;
    }
}
