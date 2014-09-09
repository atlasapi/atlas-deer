package org.atlasapi.util;

import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

import java.util.ArrayList;
import java.util.List;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

/**
 * An SecondaryIndex is a surjective mapping from a set of keys to values,
 * stored in a separate table to the indexed content.
 * 
 */
public class SecondaryIndex {

    private static final String KEY_KEY = "key";
    private static final String VALUE_KEY = "value";

    private final Session session;
    private final String indexTable;
    private final ConsistencyLevel readConsistency;

    private final Function<ResultSet, ImmutableMap<Long, Long>> toMap
        = new Function<ResultSet, ImmutableMap<Long, Long>>() {
            @Override
            public ImmutableMap<Long, Long> apply(ResultSet rows) {
                Builder<Long, Long> index = ImmutableMap.builder();
                for (Row row : rows) {
                    index.put(row.getLong(KEY_KEY), row.getLong(VALUE_KEY));
                }
                return index.build();
            }
        };

    public SecondaryIndex(Session session, String table, ConsistencyLevel read) {
        this.session = checkNotNull(session);
        this.indexTable = checkNotNull(table);
        this.readConsistency = checkNotNull(read);
    }

    public Statement insertStatement(Long key, Long value) {
        return insertInto(indexTable)
                .value(KEY_KEY, key)
                .value(VALUE_KEY, value);
    }

    public List<Statement> insertStatements(Iterable<Long> keys, Long value) {
        ArrayList<Statement> inserts = Lists.newArrayList();
        for (Long key : keys) {
            inserts.add(insertStatement(key, value));
        }
        return inserts;
    }

    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys) {
        return Futures.transform(session.executeAsync(queryFor(keys, readConsistency)), toMap);
    }

    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level) {
        return Futures.transform(session.executeAsync(queryFor(keys, level)), toMap);
    }

    private Statement queryFor(Iterable<Long> keys, ConsistencyLevel level) {
        return select(KEY_KEY, VALUE_KEY)
                .from(indexTable)
                .where(in(KEY_KEY, Iterables.toArray(keys, Object.class)))
                .setConsistencyLevel(level);
    }

}
