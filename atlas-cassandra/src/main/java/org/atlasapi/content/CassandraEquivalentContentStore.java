package org.atlasapi.content;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;

import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.util.SecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.ResultSetFuture;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import com.google.protobuf.InvalidProtocolBufferException;

public class CassandraEquivalentContentStore extends AbstractEquivalentContentStore {

    private static final String EQUIVALENT_CONTENT_INDEX = "equivalent_content_index";
    private static final String EQUIVALENT_CONTENT_TABLE = "equivalent_content";
    
    private static final String SET_ID_KEY = "set_id";
    private static final String GRAPH_KEY = "graph";
    private static final String CONTENT_ID_KEY = "content_id";
    private static final String DATA_KEY = "data";
    
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;

    private final SecondaryIndex index;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final EquivalenceGraphSerializer graphSerializer = new EquivalenceGraphSerializer();
    private final ContentSerializer contentSerializer = new ContentSerializer();
    
    public CassandraEquivalentContentStore(ContentResolver contentResolver,
            EquivalenceGraphStore graphStore, Session session, ConsistencyLevel read,
            ConsistencyLevel write) {
        super(contentResolver, graphStore);
        this.session = session;
        this.readConsistency = read;
        this.writeConsistency = write;
        this.index = new SecondaryIndex(session, EQUIVALENT_CONTENT_INDEX, read);
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIds(Iterable<Id> ids,
            Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {

        log.debug("Resolving IDs {}", Iterables.toString(ids));
        
        final SettableFuture<ResolvedEquivalents<Content>> result = SettableFuture.create();
  
        resolveWithConsistency(result, ids, selectedSources, readConsistency);
        
        return result;
    }

    private void resolveWithConsistency(final SettableFuture<ResolvedEquivalents<Content>> result, 
            final Iterable<Id> ids, final Set<Publisher> selectedSources, final ConsistencyLevel readConsistency) {
        ListenableFuture<ImmutableMap<Long, Long>> setsToResolve = 
                index.lookup(Iterables.transform(ids, Id.toLongValue()), readConsistency);
        
        Futures.addCallback(Futures.transform(setsToResolve, toEquivalentsSets(selectedSources, readConsistency)), 
                new FutureCallback<Optional<ResolvedEquivalents<Content>>>(){
                    @Override
                    public void onSuccess(Optional<ResolvedEquivalents<Content>> resolved) {
                        /* Because QUORUM writes are used, reads may see a set in an inconsistent 
                         * state. If a set is read in an inconsistent state then a second read is 
                         * attempted at QUORUM level; slower being better than incorrect.
                         */
                        if (resolved.isPresent()) {
                            result.set(resolved.get());
                        } else if (readConsistency != ConsistencyLevel.QUORUM) {
                            resolveWithConsistency(result, ids, selectedSources, ConsistencyLevel.QUORUM);
                        } else {
                            result.setException(new IllegalStateException("Failed to resolve " + ids));
                        }
                    }
        
                    @Override
                    public void onFailure(Throwable t) {
                        result.setException(t);
                    }
                });
    }

    private AsyncFunction<Map<Long, Long>, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            final Set<Publisher> selectedSources, final ConsistencyLevel readConsistency) {
        return new AsyncFunction<Map<Long, Long>, Optional<ResolvedEquivalents<Content>>>() {
            @Override
            public ListenableFuture<Optional<ResolvedEquivalents<Content>>> apply(Map<Long, Long> index)
                    throws Exception {
                return Futures.transform(resultOf(selectSetsQuery(index.values()),readConsistency), 
                        toEquivalentsSets(index, selectedSources));
            }
        };
    }

    private Function<ResultSet, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            final Map<Long, Long> index, final Set<Publisher> selectedSources) {
        return new Function<ResultSet, Optional<ResolvedEquivalents<Content>>>() {
            @Override
            public Optional<ResolvedEquivalents<Content>> apply(ResultSet setsRows) {
                Multimap<Long, Content> sets = deserialize(index, setsRows, selectedSources);
                if (sets == null) {
                    return Optional.absent();
                }
                ResolvedEquivalents.Builder<Content> results = ResolvedEquivalents.builder();
                for (Entry<Long, Long> id : index.entrySet()) {
                    Collection<Content> setForId = sets.get(id.getValue());
                    results.putEquivalents(Id.valueOf(id.getKey()), setForId);
                }
                return Optional.of(results.build());
            }
        };
    }

    private Multimap<Long, Content> deserialize(Map<Long, Long> index, ResultSet setsRows, Set<Publisher> selectedSources) {
        ImmutableSetMultimap.Builder<Long, Content> sets = ImmutableSetMultimap.builder();
        Map<Long, EquivalenceGraph> graphs = Maps.newHashMap();
        for (Row row : setsRows) {
            long setId = row.getLong(SET_ID_KEY);
            if (!row.isNull(GRAPH_KEY)) {
                graphs.put(setId, graphSerializer.deserialize(row.getBytes(GRAPH_KEY)));
                Content content = deserialize(row);
                if (content instanceof Item) {
                    Item item = (Item) content;
                    for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
                        segmentEvent.setPublisher(item.getSource());
                    }
                }
                EquivalenceGraph graphForContent = graphs.get(setId);
                if (contentSelected(content, graphForContent, selectedSources)) {
                    sets.put(setId, content);
                }
            }
        }
        return checkIntegrity(index, graphs) ? sets.build() : null;
    }

    private boolean checkIntegrity(Map<Long, Long> index, Map<Long, EquivalenceGraph> graphs) {
        for (Entry<Long, Long> requests : index.entrySet()) {
            EquivalenceGraph requestedGraph = graphs.get(requests.getValue());
            if (requestedGraph == null
                || !requestedGraph.getEquivalenceSet().contains(Id.valueOf(requests.getKey()))) {
                //stale read of index, pointing a graph that doesn't exist.
                return false;
            }
        }
        return true;
    }

    private Content deserialize(Row row) {
        try {
            ByteString bytes = ByteString.copyFrom(row.getBytes(DATA_KEY));
            ContentProtos.Content buffer = ContentProtos.Content.parseFrom(bytes);
            return contentSerializer.deserialize(buffer);
        } catch (InvalidProtocolBufferException e) {
            throw new RuntimeException(row.getLong(SET_ID_KEY)+":"+row.getLong(CONTENT_ID_KEY), e);
        }
    }

    //TODO more complex following of graph.
    private boolean contentSelected(Content content, EquivalenceGraph equivalenceGraph,
            Set<Publisher> selectedSources) {
        return selectedSources.contains(content.getSource())
            && equivalenceGraph.getEquivalenceSet().contains(content.getId());
    }

    private ResultSetFuture resultOf(Statement query, ConsistencyLevel readConsistency) {
        return session.executeAsync(query.setConsistencyLevel(readConsistency));
    }
    
    private Statement selectSetsQuery(Iterable<Long> keys) {
        return select().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(in(SET_ID_KEY, ImmutableSet.copyOf(keys).toArray()))
                .orderBy(asc(CONTENT_ID_KEY))
                .setFetchSize(Integer.MAX_VALUE);
    }
    
    @Override
    protected void updateEquivalences(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent, EquivalenceGraphUpdate update) {
        if (graphsAndContent.isEmpty()) {
            log.warn("Empty content for " + update);
            return;
        }
        updateDataRows(graphsAndContent);
        updateIndexRows(graphsAndContent);
        deleteStaleSets(update.getDeleted());
        deleteStaleRows(update.getUpdated(), update.getCreated());
    }

    private void deleteStaleRows(EquivalenceGraph updated, ImmutableSet<EquivalenceGraph> created) {
        if (created.isEmpty()) {
            return;
        }
        long id = updated.getId().longValue();
        List<Statement> deletes = Lists.newArrayList();
        for (EquivalenceGraph graph : created) {
            for (Id elem : graph.getEquivalenceSet()) {
                deletes.add(delete()
                    .from(EQUIVALENT_CONTENT_TABLE)
                    .where(eq(SET_ID_KEY, id))
                        .and(eq(CONTENT_ID_KEY, elem.longValue())));
            }
        }
        session.execute(batch(deletes.toArray(new RegularStatement[deletes.size()]))
                .setConsistencyLevel(writeConsistency));
    }

    private void deleteStaleSets(Set<Id> deletedGraphs) {
        if (deletedGraphs.isEmpty()) {
            return;
        }
        Object[] ids = Collections2.transform(deletedGraphs, Id.toLongValue()).toArray();
        session.execute(delete().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(in(SET_ID_KEY, ids))
                .setConsistencyLevel(writeConsistency));
    }

    private void updateDataRows(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        List<Statement> updates = Lists.newArrayListWithExpectedSize(graphsAndContent.size());
        for (Entry<EquivalenceGraph, Content> graphAndContent : graphsAndContent.entries()) {
            EquivalenceGraph graph = graphAndContent.getKey();
            Content content = graphAndContent.getValue();
            updates.add(dataRowUpdateFor(graph, content));
        }
        for (EquivalenceGraph graph : graphsAndContent.keySet()) {
            updates.add(update(EQUIVALENT_CONTENT_TABLE)
                    .where(eq(SET_ID_KEY, graph.getId().longValue()))
                    .and(eq(CONTENT_ID_KEY, graph.getId().longValue()))
                    .with(set(GRAPH_KEY, graphSerializer.serialize(graph))));
        }
        session.execute(batch(updates.toArray(new RegularStatement[updates.size()]))
                .setConsistencyLevel(writeConsistency));
    }

    private Statement dataRowUpdateFor(EquivalenceGraph graph, Content content) {
        return update(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, graph.getId().longValue()))
                .and(eq(CONTENT_ID_KEY, content.getId().longValue()))
                .with(set(DATA_KEY, serialize(content)))
                .and(set(GRAPH_KEY, graphSerializer.serialize(graph)));
    }

    private ByteBuffer serialize(Content content) {
        ContentProtos.Content contentBuffer = contentSerializer.serialize(content);
        ByteBuffer buffer = ByteBuffer.wrap(contentBuffer.toByteArray());
        /* Debug logging to investigate null content being written into equiv store */
        if (!buffer.hasRemaining()) {
            log.debug("ByteBuffer for serialised Content {} is empty!", content.getId());
        }
        return buffer;
    }
    
    private void updateIndexRows(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        List<Statement> updates = Lists.newArrayListWithExpectedSize(graphsAndContent.size());
        for (Entry<EquivalenceGraph, Content> graphAndContent : graphsAndContent.entries()) {
            EquivalenceGraph graph = graphAndContent.getKey();
            Content content = graphAndContent.getValue();
            updates.add(index.insertStatement(content.getId().longValue(), graph.getId().longValue()));
        }
        session.execute(batch(updates.toArray(new RegularStatement[updates.size()]))
                .setConsistencyLevel(writeConsistency));
    }

    @Override
    protected void updateInSet(EquivalenceGraph graph, Content content) {
        session.execute(dataRowUpdateFor(graph, content).setConsistencyLevel(writeConsistency));
    }

}
