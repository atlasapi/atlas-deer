package org.atlasapi.util;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.util.concurrent.ListenableFuture;

import java.util.List;

public interface SecondaryIndex {
    Statement insertStatement(Long key, Long value);

    List<Statement> insertStatements(Iterable<Long> keys, Long value);

    ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys);

    ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level);
}
