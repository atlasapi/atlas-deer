package org.atlasapi.util;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.atlasapi.entity.Id;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

/**
 * An SecondaryIndex is a surjective mapping from a set of keys to values,
 * stored in a separate table to the indexed content.
 * 
 */
public class CassandraSecondaryIndex implements SecondaryIndex {

    private static final String KEY_KEY = "key";
    private static final String VALUE_KEY = "value";
    private static final Function<? super ResultSet, ? extends ImmutableSet<Long>> RESULT_TO_IDS = rows -> {
        ImmutableSet.Builder<Long> builder = ImmutableSet.builder();
        for (Row row : rows) {
            builder.add(row.getLong(KEY_KEY));
        }
        return builder.build();
    };

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

    public CassandraSecondaryIndex(Session session, String table, ConsistencyLevel read) {
        this.session = checkNotNull(session);
        this.indexTable = checkNotNull(table);
        this.readConsistency = checkNotNull(read);
    }

    @Override
    public Statement insertStatement(Long key, Long value) {
        return insertInto(indexTable)
                .value(KEY_KEY, key)
                .value(VALUE_KEY, value);
    }

    @Override
    public List<Statement> insertStatements(Iterable<Long> keys, Long value) {
        ArrayList<Statement> inserts = Lists.newArrayList();
        for (Long key : keys) {
            inserts.add(insertStatement(key, value));
        }
        return inserts;
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys) {
        return Futures.transform(session.executeAsync(queryFor(keys, readConsistency)), toMap);
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level) {
        return Futures.transform(session.executeAsync(queryFor(keys, level)), toMap);
    }

    private Statement queryFor(Iterable<Long> keys, ConsistencyLevel level) {
        return select(KEY_KEY, VALUE_KEY)
                .from(indexTable)
                .where(in(KEY_KEY, Iterables.toArray(keys, Object.class)))
                .setConsistencyLevel(level);
    }

    @Override
    public ListenableFuture<ImmutableSet<Long>> reverseLookup(Id id) {
        try {
            ImmutableMap<Long, Long> idToCanonical =
                    Futures.get(lookup(ImmutableList.of(id.longValue())), IOException.class);

            Long canonical = idToCanonical.get(id.longValue());

            Statement canonicalToSecondaries = select(KEY_KEY, VALUE_KEY)
                    .from(indexTable)
                    .where(eq(VALUE_KEY, canonical))
                    .setConsistencyLevel(readConsistency);

            return Futures.transform(session.executeAsync(canonicalToSecondaries), RESULT_TO_IDS);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}