package org.atlasapi.util;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Predicates;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableMap.Builder;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;

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

    private final Function<Iterable<Row>, ImmutableMap<Long, Long>> toMap
        = rows -> {
            Builder<Long, Long> index = ImmutableMap.builder();
            for (Row row : rows) {
                index.put(row.getLong(KEY_KEY), row.getLong(VALUE_KEY));
            }
            return index.build();
        };

    private final PreparedStatement insert;
    private final PreparedStatement select;
    private final PreparedStatement canonicalToSecondariesSelect;

    public CassandraSecondaryIndex(Session session, String table, ConsistencyLevel read) {
        this.session = checkNotNull(session);
        this.indexTable = checkNotNull(table);
        this.readConsistency = checkNotNull(read);

        this.insert = session.prepare(insertInto(indexTable)
                .value(KEY_KEY, bindMarker("key"))
                .value(VALUE_KEY, bindMarker("value")));

        this.select = session.prepare(select(KEY_KEY, VALUE_KEY)
                .from(indexTable)
                .where(eq(KEY_KEY, bindMarker("key"))));

        this.canonicalToSecondariesSelect = session.prepare(select(KEY_KEY, VALUE_KEY)
                .from(indexTable)
                .where(eq(VALUE_KEY, bindMarker())));
        this.canonicalToSecondariesSelect.setConsistencyLevel(readConsistency);
    }

    @Override
    public Statement insertStatement(Long key, Long value) {
        return insert.bind().setLong("key", key).setLong("value", value);
    }

    @Override
    public List<Statement> insertStatements(Iterable<Long> keys, Long value) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> insertStatement(k, value))
                .collect(ImmutableCollectors.toList());
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys) {
        return lookup(keys, readConsistency);
    }

    @Override
    public ListenableFuture<ImmutableMap<Long, Long>> lookup(Iterable<Long> keys, ConsistencyLevel level) {
        ImmutableList<Long> uniqueKeys = StreamSupport.stream(keys.spliterator(), false)
                .distinct()
                .collect(ImmutableCollectors.toList());

        ListenableFuture<List<Row>> resultsFuture = Futures.transform(Futures.allAsList(
                queriesFor(uniqueKeys, readConsistency).stream()
                        .map(session::executeAsync)
                        .map(rsFuture -> Futures.transform(rsFuture,
                                (Function<ResultSet, Row>) input -> input != null ? input.one() : null))
                        .collect(Collectors.toList())),
                (Function<List<Row>, List<Row>>) input -> input.stream()
                        .filter(Predicates.notNull()::apply).collect(Collectors.toList())
        );
        return Futures.transform(resultsFuture, toMap);
    }

    private List<Statement> queriesFor(Iterable<Long> keys, ConsistencyLevel level) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> select.bind().setLong("key", k).setConsistencyLevel(level))
                .collect(Collectors.toList());
    }

    @Override
    public ListenableFuture<ImmutableSet<Long>> reverseLookup(Id id) {
        try {
            ImmutableMap<Long, Long> idToCanonical =
                    Futures.get(lookup(ImmutableList.of(id.longValue())), IOException.class);

            Long canonical = idToCanonical.get(id.longValue());
            return Futures.transform(session.executeAsync(canonicalToSecondariesSelect.bind(canonical)), RESULT_TO_IDS);
        } catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }
}