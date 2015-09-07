package org.atlasapi.util;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.Id;

import java.util.List;

public class NoOpSecondaryIndex implements SecondaryIndex {


    @Override
    public Statement insertStatement(Long key, Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public List<Statement> insertStatements(Iterable<Long> keys, Long value) {
        throw new UnsupportedOperationException();
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys) {
        return Futures.immediateFuture(ImmutableMap.of());
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level) {
        return Futures.immediateFuture(ImmutableMap.of());
    }

    @Override
    public ListenableFuture<ImmutableSet<Long>> reverseLookup(Id id) {
        return Futures.immediateFuture(ImmutableSet.of());
    }
}
