package org.atlasapi.util;

import java.util.List;

import org.atlasapi.entity.Id;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.Statement;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.ListenableFuture;

public interface SecondaryIndex {

    Statement insertStatement(Long key, Long value);

    List<Statement> insertStatements(Iterable<Long> keys, Long value);

    ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys);

    ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level);

    ListenableFuture<ImmutableSet<Long>> reverseLookup(Id id);
}
