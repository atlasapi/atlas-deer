package org.atlasapi.equivalence;

import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Objects;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.util.GroupLock;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.base.Function;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

public final class CassandraEquivalenceGraphStore extends AbstractEquivalenceGraphStore {

    public static final Function<List<Row>, List<Row>> filterNulls =
            (Function<List<Row>, List<Row>>) input -> input.stream()
                    .filter(Objects::nonNull)
                    .collect(ImmutableCollectors.toList());

    private static final String EQUIVALENCE_GRAPHS_TABLE = "equivalence_graph";
    // These are exposed to allow testing that we can cope with stale index entries which
    // exist in the db due to past, now fixed, bugs
    @VisibleForTesting static final String EQUIVALENCE_GRAPH_INDEX_TABLE = "equivalence_graph_index";

    @VisibleForTesting static final String RESOURCE_ID_KEY = "resource_id";
    @VisibleForTesting static final String GRAPH_ID_KEY = "graph_id";
    private static final String GRAPH_KEY = "graph";
    
    private static final GroupLock<Id> lock = GroupLock.natural();
    private static final Logger log = LoggerFactory.getLogger(CassandraEquivalenceGraphStore.class);
    
    private final EquivalenceGraphSerializer serializer = new EquivalenceGraphSerializer();
    
    private final Session session;
    private final ConsistencyLevel read;
    private final ConsistencyLevel write;

    private final PreparedStatement graphRowsSelect;
    private final PreparedStatement graphIdsSelect;
    private final PreparedStatement indexInsert;
    private final PreparedStatement graphInsert;

    public CassandraEquivalenceGraphStore(MessageSender<EquivalenceGraphUpdateMessage> messageSender, Session session, ConsistencyLevel read, ConsistencyLevel write) {
        super(messageSender);
        this.session = checkNotNull(session);
        this.read = read;
        this.write = write;

        this.graphRowsSelect = session.prepare(select().all()
                .from(EQUIVALENCE_GRAPHS_TABLE)
                .where(eq(GRAPH_ID_KEY, bindMarker())));
        this.graphRowsSelect.setConsistencyLevel(read);

        this.graphIdsSelect = session.prepare(select(RESOURCE_ID_KEY, GRAPH_ID_KEY)
                .from(EQUIVALENCE_GRAPH_INDEX_TABLE)
                .where(eq(RESOURCE_ID_KEY, bindMarker())));
        this.graphIdsSelect.setConsistencyLevel(read);

        this.indexInsert = session.prepare(insertInto(EQUIVALENCE_GRAPH_INDEX_TABLE)
                .value(RESOURCE_ID_KEY, bindMarker("resourceId"))
                .value(GRAPH_ID_KEY, bindMarker("graphId")));
        this.indexInsert.setConsistencyLevel(write);

        this.graphInsert = session.prepare(
                insertInto(EQUIVALENCE_GRAPHS_TABLE)
                        .value(GRAPH_ID_KEY, bindMarker("graphId"))
                        .value(GRAPH_KEY, bindMarker("data")));
        this.graphInsert.setConsistencyLevel(write);
    }

    private final Function<Iterable<Row>, Map<Long, EquivalenceGraph>> toGraph = rows -> {
            ImmutableMap.Builder<Long, EquivalenceGraph> idGraph = ImmutableMap.builder();
            for (Row row : rows) {
                Long graphId = row.getLong(GRAPH_ID_KEY);
                idGraph.put(graphId, serializer.deserialize(row.getBytes(GRAPH_KEY)));
            }
            return idGraph.build();
        };

    private final AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>> toGraphs =
            idIndex -> {
        ListenableFuture<List<Row>> rowFutures = Futures.transform(
                Futures.allAsList(idIndex.values()
                    .stream()
                    .unordered()
                    .distinct()
                    .map(this::queryForGraphRow)
                    .map(this::resultOf)
                    .map(resultSetFuture -> Futures.transform(resultSetFuture, ResultSet::one))
                    .collect(Collectors.toList())),
                filterNulls
        );

        return Futures.transform(
                Futures.transform(rowFutures, toGraph),
                (Function<Map<Long,EquivalenceGraph>, OptionalMap<Id,EquivalenceGraph>>)
                        rowGraphIndex -> getRequestedIdToGraphMap(idIndex, rowGraphIndex)
        );
    };

    private OptionalMap<Id, EquivalenceGraph> getRequestedIdToGraphMap(
            Map<Id, Long> idIndex, Map<Long, EquivalenceGraph> rowGraphIndex) {
        return ImmutableOptionalMap.fromMap(
                idIndex.entrySet().stream()
                    .filter(entry -> hasResolvedEquivalenceGraph(
                            entry.getValue(), entry.getKey(), rowGraphIndex
                    ))
                    .collect(ImmutableCollectors.toMap(
                            Entry::getKey,
                            entry -> rowGraphIndex.get(entry.getValue())
                    ))
        );
    }

    private boolean hasResolvedEquivalenceGraph(Long graphId, Id requestedId,
            Map<Long, EquivalenceGraph> rowGraphIndex) {
        EquivalenceGraph graph = rowGraphIndex.get(graphId);

        // Index entries can be stale and point to a graph that the requested ID used to be part
        // of, but no longer is. Here we filter out such stale entries
        return graph != null && graph.getEquivalenceSet().contains(requestedId);
    }

    private Statement queryForGraphRow(Long id) {
        return graphRowsSelect.bind(id);
    }

    private final Function<Iterable<Row>, Map<Id, Long>> toGraphIdIndex
        = rows -> {
            ImmutableMap.Builder<Id, Long> idIndex = ImmutableMap.builder();
            for (Row row : rows) {
                Id resourceId = Id.valueOf(row.getLong(RESOURCE_ID_KEY));
                long graphId = row.getLong(GRAPH_ID_KEY);
                idIndex.put(resourceId, graphId);
            }
            return idIndex.build();
        };
        
    @Override
    public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> resolveIds(Iterable<Id> ids) {
        ListenableFuture<Map<Id, Long>> graphIdIndex = resolveToGraphIds(ids);
        return Futures.transform(graphIdIndex, toGraphs);
    }

    private ListenableFuture<Map<Id,Long>> resolveToGraphIds(Iterable<Id> ids) {
        ListenableFuture<List<Row>> resultsFuture = Futures.transform(Futures.allAsList(
                queriesForGraphIds(ids)
                        .stream()
                        .map(this::resultOf)
                        .map(resultSetFuture -> Futures.transform(resultSetFuture,
                                (Function<ResultSet, Row>) rs -> rs != null ? rs.one() : null))
                        .collect(Collectors.toList())),
                 filterNulls);
        return Futures.transform(resultsFuture, toGraphIdIndex);
    }

    private ResultSetFuture resultOf(Statement query) {
        return session.executeAsync(query);
    }

    private List<Statement> queriesForGraphIds(Iterable<Id> ids) {
        return StreamSupport.stream(ids.spliterator(), false)
                .map(Id.toLongValue()::apply)
                .unordered()
                .distinct()
                .map(graphIdsSelect::bind)
                .collect(ImmutableCollectors.toList());
    }

    @Override
    protected void doStore(ImmutableSet<EquivalenceGraph> graphs) {
        BatchStatement updateBatch = new BatchStatement();
        updateBatch.setConsistencyLevel(write);

        for (EquivalenceGraph graph : graphs) {
            Long graphId = lowestId(graph); 
            ByteBuffer serializedGraph = serializer.serialize(graph);
            updateBatch.add(graphInsert(graphId, serializedGraph));
            for (Entry<Id, Adjacents> adjacency : graph.getAdjacencyList().entrySet()) {
                updateBatch.add(indexInsert(adjacency.getKey().longValue(), graphId));
            }
        }

        session.execute(updateBatch);
    }

    private Statement indexInsert(Long resourceId, Long graphId) {
        return indexInsert.bind().setLong("resourceId", resourceId).setLong("graphId", graphId);
    }

    private Statement graphInsert(Long graphId, ByteBuffer serializedGraph) {
        return graphInsert.bind().setLong("graphId", graphId).setBytes("data", serializedGraph);
    }

    private Long lowestId(EquivalenceGraph graph) {
        return Ordering.natural().min(graph.getAdjacencyList().keySet()).longValue();
    }

    @Override
    protected GroupLock<Id> lock() {
        return lock;
    }

    @Override
    protected Logger log() {
        return log;
    }

}
