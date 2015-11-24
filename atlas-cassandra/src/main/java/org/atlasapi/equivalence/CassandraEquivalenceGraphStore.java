package org.atlasapi.equivalence;

import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Maps;
import com.google.common.collect.Ordering;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.queue.MessageSender;

public final class CassandraEquivalenceGraphStore extends AbstractEquivalenceGraphStore {

    private static final String EQUIVALENCE_GRAPHS_TABLE = "equivalence_graph";
    private static final String EQUIVALENCE_GRAPH_INDEX_TABLE = "equivalence_graph_index";

    private static final String RESOURCE_ID_KEY = "resource_id";
    private static final String GRAPH_ID_KEY = "graph_id";
    private static final String GRAPH_KEY = "graph";
    
    private static final GroupLock<Id> lock = GroupLock.natural();
    private static final Logger log = LoggerFactory.getLogger(CassandraEquivalenceGraphStore.class);
    
    private final EquivalenceGraphSerializer serializer = new EquivalenceGraphSerializer();
    
    private final Session session;
    private final ConsistencyLevel read;
    private final ConsistencyLevel write;

    private final PreparedStatement indexDelete;
    private final PreparedStatement graphDelete;
    private final PreparedStatement graphRowsSelect;
    private final PreparedStatement graphIdsSelect;
    private final PreparedStatement indexInsert;
    private final PreparedStatement graphInsert;

    public CassandraEquivalenceGraphStore(MessageSender<EquivalenceGraphUpdateMessage> messageSender, Session session, ConsistencyLevel read, ConsistencyLevel write) {
        super(messageSender);
        this.session = checkNotNull(session);
        this.read = read;
        this.write = write;

        this.indexDelete = session.prepare(
                delete().all().from(EQUIVALENCE_GRAPH_INDEX_TABLE)
                        .where(eq(RESOURCE_ID_KEY, bindMarker())));
        this.indexDelete.setConsistencyLevel(write);

        this.graphDelete = session.prepare(
                delete().all().from(EQUIVALENCE_GRAPHS_TABLE)
                        .where(eq(GRAPH_ID_KEY, bindMarker())));
        this.graphDelete.setConsistencyLevel(write);

        this.graphRowsSelect = session.prepare(select().all()
                .from(EQUIVALENCE_GRAPHS_TABLE)
                .where(eq(GRAPH_ID_KEY, bindMarker())));
        this.graphRowsSelect.setConsistencyLevel(read);

        this.graphIdsSelect = session.prepare(select(RESOURCE_ID_KEY, GRAPH_ID_KEY)
                .from(EQUIVALENCE_GRAPH_INDEX_TABLE)
                .where(in(RESOURCE_ID_KEY, bindMarker())));
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

    @Override
    protected void cleanGraphAndIndex(Id subjectId) {
        log.warn("Cleaning graph and index for subject {}", subjectId);
        try {
            Long graphId = Futures.get(resolveToGraphIds(ImmutableList.of(subjectId)), IOException.class)
                    .get(subjectId);
            /* Remove index entry */
            log.warn("Cleaning graph index for subject {}", subjectId);
            session.execute(indexDelete.bind(subjectId.longValue()));
            /* Remove graph entry */
            log.warn("Deleting graph {} for subject {}", graphId, subjectId);
            session.execute(graphDelete.bind(graphId));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final Function<Iterable<Row>, Map<Long, EquivalenceGraph>> toGraph = rows -> {
            ImmutableMap.Builder<Long, EquivalenceGraph> idGraph = ImmutableMap.builder();
            for (Row row : rows) {
                Long graphId = row.getLong(GRAPH_ID_KEY);
                idGraph.put(graphId, serializer.deserialize(row.getBytes(GRAPH_KEY)));
            }
            return idGraph.build();
        };

    private final AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>> toGraphs = idIndex -> {
        ListenableFuture<List<Row>> rowFutures = Futures.allAsList(idIndex.values()
                .stream()
                .unordered()
                .distinct()
                .map(this::queryForGraphRow)
                .map(this::resultOf)
                .map(resultSetFuture -> Futures.transform(resultSetFuture, ResultSet::one))
                .collect(Collectors.toList()));

        return Futures.transform(Futures.transform(rowFutures, toGraph), toIdGraphIndex(idIndex));
    };

    private Function<Map<Long, EquivalenceGraph>, OptionalMap<Id, EquivalenceGraph>> toIdGraphIndex(final Map<Id, Long> idIndex) {
        return rowGraphIndex -> ImmutableOptionalMap.fromMap(Maps.transformValues(idIndex, Functions.forMap(rowGraphIndex, null)));
    }
    
    private Statement queryForGraphRow(Long id) {
        return graphRowsSelect.bind(id);
    }

    private final Function<ResultSet, Map<Id, Long>> toGraphIdIndex
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
        return Futures.transform(resultOf(queryForGraphIds(ids)), toGraphIdIndex);
    }

    private ResultSetFuture resultOf(Statement query) {
        return session.executeAsync(query);
    }

    private Statement queryForGraphIds(Iterable<Id> ids) {
        return graphIdsSelect.bind(
                 StreamSupport.stream(ids.spliterator(), false)
                        .map(Id.toLongValue()::apply)
                        .unordered()
                        .distinct()
                        .collect(Collectors.toList()));
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
