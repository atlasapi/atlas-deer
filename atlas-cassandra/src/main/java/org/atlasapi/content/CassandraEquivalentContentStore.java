package org.atlasapi.content;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalentContentUpdatedMessage;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.util.CassandraSecondaryIndex;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;

import com.metabroadcast.common.queue.MessageSender;

import com.datastax.driver.core.BatchStatement;
import com.datastax.driver.core.BoundStatement;
import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.PreparedStatement;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Function;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Multimap;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.datastax.driver.core.querybuilder.QueryBuilder.asc;
import static com.datastax.driver.core.querybuilder.QueryBuilder.bindMarker;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.google.common.base.Preconditions.checkNotNull;

public class CassandraEquivalentContentStore extends AbstractEquivalentContentStore {

    public static final String EQUIVALENT_CONTENT_INDEX = "equivalent_content_index";
    public static final String EQUIVALENT_CONTENT_TABLE = "equivalent_content";

    public static final String SET_ID_KEY = "set_id";
    public static final String CONTENT_ID_KEY = "content_id";
    public static final String DATA_KEY = "data";
    public static final String GRAPH_KEY = "graph";

    private static final String SET_ID_BIND = "set_id";
    private static final String CONTENT_ID_BIND = "content_id";
    private static final String DATA_BIND = "data";
    private static final String GRAPH_BIND = "graph";

    private final LegacyContentResolver legacyContentResolver;
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;

    private final SecondaryIndex index;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentSerializer contentSerializer;
    private final EquivalenceGraphSerializer graphSerializer;

    private final PreparedStatement setsSelect;
    private final PreparedStatement rowDelete;
    private final PreparedStatement setDelete;
    private final PreparedStatement dataRowUpdate;
    private final PreparedStatement graphUpdate;
    private final PreparedStatement equivSetSelect;

    public CassandraEquivalentContentStore(
            ContentResolver contentResolver,
            LegacyContentResolver legacyContentResolver,
            EquivalenceGraphStore graphStore,
            MessageSender<EquivalentContentUpdatedMessage> equivalentContentUpdatedMessageSender,
            Session session,
            ConsistencyLevel read,
            ConsistencyLevel write
    ) {
        super(contentResolver, graphStore, equivalentContentUpdatedMessageSender);
        this.legacyContentResolver = checkNotNull(legacyContentResolver);
        this.contentSerializer = new ContentSerializer(new ContentSerializationVisitor(contentResolver));
        this.graphSerializer = new EquivalenceGraphSerializer();
        this.session = session;
        this.readConsistency = read;
        this.writeConsistency = write;
        this.index = new CassandraSecondaryIndex(session, EQUIVALENT_CONTENT_INDEX, read);

        RegularStatement statement = select().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .orderBy(asc(CONTENT_ID_KEY));
        statement.setFetchSize(Integer.MAX_VALUE);
        statement.setConsistencyLevel(read);
        this.setsSelect = session.prepare(statement);

        this.rowDelete = session.prepare(delete()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .and(eq(CONTENT_ID_KEY, bindMarker(CONTENT_ID_BIND))));

        this.setDelete = session.prepare(delete().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker())));
        this.setDelete.setConsistencyLevel(writeConsistency);

        this.dataRowUpdate = session.prepare(QueryBuilder.update(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .and(eq(CONTENT_ID_KEY, bindMarker(CONTENT_ID_BIND)))
                .with(set(DATA_KEY, bindMarker(DATA_BIND))));

        this.graphUpdate = session.prepare(QueryBuilder.update(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .with(set(GRAPH_KEY, bindMarker(GRAPH_BIND)))
        );

        this.equivSetSelect = session.prepare(
                select(SET_ID_KEY, CONTENT_ID_KEY, DATA_KEY, GRAPH_KEY)
                    .from(EQUIVALENT_CONTENT_TABLE)
                    .where(eq(SET_ID_KEY, bindMarker()))
        );
        this.equivSetSelect.setConsistencyLevel(readConsistency);
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
        return index -> Futures.transform(
                resultOf(selectSetsQueries(index.values()), readConsistency),
                toEquivalentsSets(index, selectedSources)
        );
    }

    private Function<Iterable<ResultSet>, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            final Map<Long, Long> index, final Set<Publisher> selectedSources) {
        return setsRows -> {
            Multimap<Long, Content> sets = deserialize(setsRows, selectedSources);
            if (sets == null) {
                return Optional.absent();
            }
            ResolvedEquivalents.Builder<Content> results = ResolvedEquivalents.builder();
            for (Entry<Long, Long> id : index.entrySet()) {
                Collection<Content> setForId = sets.get(id.getValue());
                results.putEquivalents(Id.valueOf(id.getKey()), setForId);
            }
            return Optional.of(results.build());
        };
    }

    private Multimap<Long, Content> deserialize(Iterable<ResultSet> setsRows,
            Set<Publisher> selectedSources) {
        ImmutableSetMultimap.Builder<Long, Content> sets = ImmutableSetMultimap.builder();
        ImmutableList<Row> allRows = StreamSupport.stream(setsRows.spliterator(), false)
                .flatMap(rs -> rs.all().stream())
                .collect(ImmutableCollectors.toList());

        for (Row row : allRows) {
            long setId = row.getLong(SET_ID_KEY);

            Content content = deserialize(row);

            if (contentSelected(content, selectedSources)
                    && containedInGraph(content.getId(), row)) {
                sets.put(setId, content);
            }
        }
        return sets.build();
    }

    private Content deserialize(Row row) {
        long setId = row.getLong(SET_ID_KEY);
        try {
            //This is a workaround for a 'data' column being null for some old pieces of content.
            if (row.getBytes(DATA_KEY) == null) {
                long contentId = row.getLong(CONTENT_ID_KEY);
                log.warn(
                        "'{}' column empty for row  set_id = {}, content_id = {}, in '{}' column family",
                        DATA_KEY,
                        setId,
                        contentId,
                        EQUIVALENT_CONTENT_TABLE
                );
                Content content = resolvedContentFromNonEquivalentContentStore(Id.valueOf(contentId));
                updateDataColumn(setId, content);
                return content;

            }
            ByteString bytes = ByteString.copyFrom(row.getBytes(DATA_KEY));
            ContentProtos.Content buffer = ContentProtos.Content.parseFrom(bytes);
            Content content = contentSerializer.deserialize(buffer);
            if (content instanceof Item) {
                Item item = (Item) content;
                for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
                    segmentEvent.setPublisher(item.getSource());
                }
            }
            return content;
        } catch (IOException e) {
            throw new RuntimeException(setId +":"+row.getLong(CONTENT_ID_KEY), e);
        }
    }

    private boolean contentSelected(Content content, Set<Publisher> selectedSources) {
        return content.isActivelyPublished()
            && selectedSources.contains(content.getSource());
    }

    private ListenableFuture<List<ResultSet>> resultOf(Iterable<Statement> queries, ConsistencyLevel readConsistency) {
        ImmutableList.Builder<ListenableFuture<ResultSet>> resultSets = ImmutableList.builder();
        for (Statement query : queries) {
            resultSets.add(session.executeAsync(query));
        }

        return Futures.allAsList(resultSets.build());
    }
    
    private Iterable<Statement> selectSetsQueries(Iterable<Long> keys) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> setsSelect.bind().setLong(SET_ID_BIND, k))
                .collect(ImmutableCollectors.toList());
    }

    @Override
    protected void update(EquivalenceGraph graph, Content content) {
        BatchStatement statement = new BatchStatement();
        statement.setConsistencyLevel(writeConsistency);

        statement.add(getGraphUpdateRow(graph));
        statement.add(getUpdateDataRow(graph, content));
        statement.add(index.insertStatement(
                content.getId().longValue(), graph.getId().longValue()
        ));

        session.execute(statement);
    }

    @Override
    protected void update(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent,
            EquivalenceGraphUpdate update) {
        if (graphsAndContent.isEmpty()) {
            log.warn("Empty content for " + update);
            return;
        }

        BatchStatement statement = new BatchStatement();
        statement.setConsistencyLevel(writeConsistency);

        statement.addAll(getGraphUpdateRows(graphsAndContent.keySet()));
        statement.addAll(getUpdateDataRows(graphsAndContent));
        statement.addAll(getUpdateIndexRows(graphsAndContent));
        statement.addAll(getDeleteStaleSets(update.getDeleted()));
        statement.addAll(getDeleteStaleRows(update.getCreated(), update.getUpdated().getId()));

        session.execute(statement);
    }

    private ImmutableList<Statement> getGraphUpdateRows(ImmutableSet<EquivalenceGraph> graphs) {
        return graphs.stream()
                .map(this::getGraphUpdateRow)
                .collect(ImmutableCollectors.toList());
    }

    private BoundStatement getGraphUpdateRow(EquivalenceGraph graph) {
        return graphUpdate.bind()
                .setLong(SET_ID_BIND, graph.getId().longValue())
                .setBytes(GRAPH_BIND, graphSerializer.serialize(graph));
    }

    private ImmutableList<Statement> getUpdateDataRows(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        return graphsAndContent.entries().stream()
                .map(entry -> getUpdateDataRow(entry.getKey(), entry.getValue()))
                .collect(ImmutableCollectors.toList());
    }

    private BoundStatement getUpdateDataRow(EquivalenceGraph graph, Content content) {
        return dataRowUpdate.bind()
                .setLong(SET_ID_BIND, graph.getId().longValue())
                .setLong(CONTENT_ID_BIND, content.getId().longValue())
                .setBytes(DATA_BIND, serialize(content));
    }

    private ImmutableList<Statement> getDeleteStaleSets(ImmutableSet<Id> deletedGraphs) {
        if (deletedGraphs.isEmpty()) {
            return ImmutableList.of();
        }

        return deletedGraphs.stream()
                .map(Id.toLongValue()::apply)
                .map(setDelete::bind)
                .collect(ImmutableCollectors.toList());
    }

    private ImmutableList<Statement> getUpdateIndexRows(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {

        ImmutableList.Builder<Statement> statementBuilder = ImmutableList.<Statement>builder();

        for (EquivalenceGraph graph : graphsAndContent.keySet()) {
            Set<Id> indexKeys = graphsAndContent.get(graph).stream()
                    .map(Identified::getId)
                    .collect(Collectors.toSet());

            // This is to ensure we have a mapping in the index from the graph ID to itself in case
            // the content whose ID is the graph's canonical ID has failed to resolve
            indexKeys.add(graph.getId());

            indexKeys.stream()
                    .map(indexKey -> index.insertStatement(
                            indexKey.longValue(), graph.getId().longValue()
                    ))
                    .forEach(statementBuilder::add);
        }

        return statementBuilder.build();
    }

    private List<BoundStatement> getDeleteStaleRows(ImmutableSet<EquivalenceGraph> createdGraphs,
            Id updatedGraphId) {
        return createdGraphs.stream()
                    .flatMap(graph -> graph.getEquivalenceSet().stream())
                    .map(elem -> rowDelete.bind()
                            .setLong(SET_ID_BIND, updatedGraphId.longValue())
                            .setLong(CONTENT_ID_BIND, elem.longValue()))
                    .collect(Collectors.toList());
    }

    private ByteBuffer serialize(Content content) {
        ContentProtos.Content contentBuffer = contentSerializer.serialize(content);
        ByteBuffer buffer = ByteBuffer.wrap(contentBuffer.toByteArray());
        /* Debug logging to investigate null content being written into equiv store */
        if (contentSerializer.deserialize(contentBuffer) == null ||
                buffer.array().length == 0) {
            log.warn("ByteBuffer for serialised Content {} is empty!", content.getId());
        }
        return buffer;
    }

    private Content resolvedContentFromNonEquivalentContentStore(Id contentId) throws IOException {
        Resolved<Content> deerContent = Futures.get(
                getContentResolver().resolveIds(ImmutableList.of(contentId)),
                1, TimeUnit.MINUTES,
                IOException.class
        );
        if (!deerContent.getResources().isEmpty()) {
            return Iterables.getOnlyElement(deerContent.getResources());
        }

        log.warn(
                "Content {} not found in Deer content store. Trying Owl",
                contentId.longValue()
        );

        Resolved<Content> owlContent = Futures.get(
                legacyContentResolver.resolveIds(ImmutableList.of(contentId)),
                1, TimeUnit.MINUTES,
                IOException.class
        );
        return Iterables.getOnlyElement(owlContent.getResources());
    }

    private void updateDataColumn(Long setId, Content content) {
        session.executeAsync(dataRowUpdate.bind()
                .setLong(SET_ID_BIND, setId)
                .setLong(CONTENT_ID_BIND, content.getId().longValue())
                .setBytes(DATA_BIND, serialize(content)));
    }

    @Override
    public ListenableFuture<Set<Content>> resolveEquivalentSet(Long equivalentSetId) {
        return Futures.transform(
                session.executeAsync(equivSetSelect.bind(equivalentSetId)),
                (ResultSet resultSet) -> {
                    ImmutableSet.Builder<Content> content = ImmutableSet.builder();
                    for (Row row : resultSet) {
                        Content deserialized = deserialize(row);
                        if (containedInGraph(deserialized.getId(), row)) {
                            content.add(deserialized);
                        }
                    }
                    return content.build();
                }
        );
    }

    // This is sanity checking that the resolved row actually belongs to the set graph.
    // It might not be in case of stale entries that were removed from the graph, but
    // due to a bug or missed message the row did not get removed in this store.
    // In that case the row will currently never get removed
    private boolean containedInGraph(Id contentId, Row row) {
        if (row.getBytes(GRAPH_KEY) == null) {
            return true;
        }
        EquivalenceGraph graph = graphSerializer.deserialize(row.getBytes(GRAPH_KEY));
        return graph.getEquivalenceSet().contains(contentId);
    }

    // This is effectively leaking implementation details to the abstract store which should not
    // know them, but it is required in order to find potentially stale content and update it
    @Override
    protected ListenableFuture<Set<Content>> resolveEquivalentSetIncludingStaleContent(
            Long equivalentSetId) {
        return Futures.transform(
                session.executeAsync(equivSetSelect.bind(equivalentSetId)),
                (ResultSet resultSet) -> {
                    return StreamSupport.stream(resultSet.spliterator(), false)
                            .map(this::deserialize)
                            .collect(ImmutableCollectors.toSet());
                }
        );
    }
}
