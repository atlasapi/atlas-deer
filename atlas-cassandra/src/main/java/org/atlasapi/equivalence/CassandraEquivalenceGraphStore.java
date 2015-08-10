package org.atlasapi.equivalence;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.insertInto;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import com.google.common.collect.ImmutableList;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph.Adjacents;
import org.atlasapi.util.GroupLock;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.Batch;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Functions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
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

    private final Set<Id> cleaned = ImmutableSet.of();

    public CassandraEquivalenceGraphStore(MessageSender<EquivalenceGraphUpdateMessage> messageSender, Session session, ConsistencyLevel read, ConsistencyLevel write) {
        super(messageSender);
        this.session = session;
        this.read = read;
        this.write = write;
    }

    @Override
    protected void cleanGraphAndIndex(Id subjectId) {
        log.warn("Cleaning graph and index for subject {}", subjectId);
        if (cleaned.contains(subjectId)) {
            throw new RuntimeException("Subject {} has already been cleaned once, preventing infinite recursion!");
        }
        cleaned.add(subjectId);
        try {
            Long graphId = Futures.get(resolveToGraphIds(ImmutableList.of(subjectId)), IOException.class)
                    .get(subjectId);
            /* Remove index entry */
            log.warn("Cleaning graph index for subject {}", subjectId);
            session.execute(
                    delete().all().from(EQUIVALENCE_GRAPH_INDEX_TABLE)
                            .where(eq(RESOURCE_ID_KEY, subjectId.longValue()))
            );
            /* Remove graph entry */
            log.warn("Deleting graph {} for subject {}", graphId, subjectId);
            session.execute(
                    delete().all().from(EQUIVALENCE_GRAPHS_TABLE)
                            .where(eq(GRAPH_ID_KEY, graphId))
            );
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
    }

    private final Function<ResultSet, Map<Long, EquivalenceGraph>> toGraph
        = new Function<ResultSet, Map<Long, EquivalenceGraph>>() {
            @Override
            public Map<Long, EquivalenceGraph> apply(ResultSet rows) {
                ImmutableMap.Builder<Long, EquivalenceGraph> idGraph = ImmutableMap.builder();
                for (Row row : rows) {
                    Long graphId = row.getLong(GRAPH_ID_KEY);
                    idGraph.put(graphId, serializer.deserialize(row.getBytes(GRAPH_KEY)));
                }
                return idGraph.build();
            }

        };

    private final AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>> toGraphs
        = new AsyncFunction<Map<Id, Long>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public ListenableFuture<OptionalMap<Id, EquivalenceGraph>> apply(Map<Id, Long> idIndex) {
                ResultSetFuture graphRows = resultOf(queryForGraphRows(idIndex));
                return Futures.transform(Futures.transform(graphRows, toGraph), toIdGraphIndex(idIndex));
            }
        };

    private Function<Map<Long, EquivalenceGraph>, OptionalMap<Id, EquivalenceGraph>> toIdGraphIndex(final Map<Id, Long> idIndex) {
        return new Function<Map<Long, EquivalenceGraph>, OptionalMap<Id, EquivalenceGraph>>() {
            @Override
            public OptionalMap<Id, EquivalenceGraph> apply(Map<Long, EquivalenceGraph> rowGraphIndex){
                return ImmutableOptionalMap.fromMap(Maps.transformValues(idIndex, Functions.forMap(rowGraphIndex, null)));
            }
        };
    }
    
    private Statement queryForGraphRows(final Map<Id, Long> idIndex) {
        return select().all()
                .from(EQUIVALENCE_GRAPHS_TABLE)
                .where(in(GRAPH_ID_KEY, ImmutableSet.copyOf(idIndex.values()).toArray()))
                .setConsistencyLevel(read);
    }

    private final Function<ResultSet, Map<Id, Long>> toGraphIdIndex
        = new Function<ResultSet, Map<Id, Long>>() {
            @Override
            public ImmutableMap<Id, Long> apply(ResultSet rows) {
                ImmutableMap.Builder<Id, Long> idIndex = ImmutableMap.builder();
                for (Row row : rows) {
                    Id resourceId = Id.valueOf(row.getLong(RESOURCE_ID_KEY));
                    long graphId = row.getLong(GRAPH_ID_KEY);
                    idIndex.put(resourceId, graphId);
                }
                return idIndex.build();
            }
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
        ImmutableSet<Long> uniqueIds = ImmutableSet.copyOf(Iterables.transform(ids, Id.toLongValue()));
        Object[] lids = Iterables.toArray(uniqueIds,Long.class);
        return QueryBuilder
            .select(RESOURCE_ID_KEY,GRAPH_ID_KEY)
            .from(EQUIVALENCE_GRAPH_INDEX_TABLE)
            .where(in(RESOURCE_ID_KEY, lids))
            .setConsistencyLevel(read);
    }

    @Override
    protected void doStore(ImmutableSet<EquivalenceGraph> graphs) {
        List<Statement> updates = Lists.newArrayList();
        for (EquivalenceGraph graph : graphs) {
            Long graphId = lowestId(graph); 
            ByteBuffer serializedGraph = serializer.serialize(graph);
            updates.add(graphInsert(graphId, serializedGraph));
            for (Entry<Id, Adjacents> adjacency : graph.getAdjacencyList().entrySet()) {
                updates.add(indexInsert(adjacency.getKey().longValue(), graphId));
            }
        }
        Batch updateBatch = QueryBuilder.batch(updates.toArray(new RegularStatement[updates.size()]));
        session.execute(updateBatch.setConsistencyLevel(write));
    }

    private Statement indexInsert(Long resourceId, Long graphId) {
        return insertInto(EQUIVALENCE_GRAPH_INDEX_TABLE)
                .value(RESOURCE_ID_KEY, resourceId)
                .value(GRAPH_ID_KEY, graphId);
    }

    private Statement graphInsert(Long graphId, ByteBuffer serializedGraph) {
        return insertInto(EQUIVALENCE_GRAPHS_TABLE)
                .value(GRAPH_ID_KEY, graphId)
                .value(GRAPH_KEY, serializedGraph);
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
