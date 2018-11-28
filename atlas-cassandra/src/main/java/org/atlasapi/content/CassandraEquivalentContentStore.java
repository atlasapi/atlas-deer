package org.atlasapi.content;

import com.codahale.metrics.MetricRegistry;
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
import com.google.common.collect.ImmutableListMultimap;
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
import com.metabroadcast.common.queue.MessageSender;
import com.metabroadcast.common.stream.MoreCollectors;
import com.metabroadcast.common.stream.MoreStreams;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.Identified;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphFilter;
import org.atlasapi.equivalence.EquivalenceGraphSerializer;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.EquivalenceGraphUpdateMessage;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.messaging.EquivalentContentUpdatedMessage;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.util.CassandraSecondaryIndex;
import org.atlasapi.util.SecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Executor;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

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

    private static final int GRAPH_SIZE_ALERTING_THRESHOLD = 150;

    private static final int DEFAULT_FETCH_SIZE = 30;

    private final LegacyContentResolver legacyContentResolver;
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;

    private final SecondaryIndex index;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentSerializer contentSerializer;
    private final EquivalenceGraphSerializer graphSerializer;

    private final PreparedStatement dataSelect;
    private final PreparedStatement graphSelect;
    private final PreparedStatement rowDelete;
    private final PreparedStatement setDelete;
    private final PreparedStatement dataRowUpdate;
    private final PreparedStatement graphUpdate;
    private final PreparedStatement equivSetSelect;

    public CassandraEquivalentContentStore(
            ContentResolver contentResolver,
            LegacyContentResolver legacyContentResolver,
            EquivalenceGraphStore graphStore,
            MessageSender<EquivalentContentUpdatedMessage> contentUpdatedMessageSender,
            MessageSender<EquivalenceGraphUpdateMessage> graphUpdatedMessageSender,
            Session session,
            ConsistencyLevel read,
            ConsistencyLevel write,
            MetricRegistry metricRegistry,
            String metricPrefix
    ) {
        super(
                contentResolver,
                graphStore,
                contentUpdatedMessageSender,
                graphUpdatedMessageSender,
                metricRegistry,
                metricPrefix
        );
        this.legacyContentResolver = checkNotNull(legacyContentResolver);
        this.contentSerializer = new ContentSerializer(new ContentSerializationVisitor());
        this.graphSerializer = new EquivalenceGraphSerializer();
        this.session = session;
        this.readConsistency = read;
        this.writeConsistency = write;
        this.index = new CassandraSecondaryIndex(session, EQUIVALENT_CONTENT_INDEX, read);

        RegularStatement dataStatement = select(
                SET_ID_KEY,
                CONTENT_ID_KEY,
                DATA_KEY
        )
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .orderBy(asc(CONTENT_ID_KEY));
        dataStatement.setFetchSize(DEFAULT_FETCH_SIZE); //this will automatically batch when there are too many rows
        dataStatement.setConsistencyLevel(read);
        this.dataSelect = session.prepare(dataStatement);

        //Despite being a static column we need to query the graph data separately to prevent fetching a potentially large equiv graph for each member of an equiv set
        RegularStatement graphStatement = select(
                SET_ID_KEY,
                GRAPH_KEY
        )
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, bindMarker(SET_ID_BIND)))
                .limit(1);
        graphStatement.setConsistencyLevel(read);

        this.graphSelect = session.prepare(graphStatement);

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

        resolveWithConsistency(result, ids, selectedSources, activeAnnotations, readConsistency);

        return result;
    }

    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIdsWithoutEquivalence(
            Iterable<Id> ids,
            Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {

        ListenableFuture<ResolvedEquivalents<Content>> equivalents = resolveIds(
                ids,
                selectedSources,
                activeAnnotations
        );
        return Futures.transform(equivalents, extractTargetContent(ids));
    }

    private Function<ResolvedEquivalents<Content>, ResolvedEquivalents<Content>> extractTargetContent(
            final Iterable<Id> ids) {
        return new Function<ResolvedEquivalents<Content>, ResolvedEquivalents<Content>>() {

            @Override
            public ResolvedEquivalents<Content> apply(ResolvedEquivalents<Content> input) {

                ResolvedEquivalents.Builder<Content> builder = ResolvedEquivalents.builder();
                for (Map.Entry<Id, Collection<Content>> entry : input.asMap().entrySet()) {
                    for (Content content : entry.getValue()) {
                        if (Iterables.contains(ids, content.getId())) {
                            builder.putEquivalents(entry.getKey(), ImmutableSet.of(content));
                        }
                    }
                }
                return builder.build();
            }
        };
    }

    private void resolveWithConsistency(final SettableFuture<ResolvedEquivalents<Content>> result,
            final Iterable<Id> ids, final Set<Publisher> selectedSources, Set<Annotation> activeAnnotations,
            final ConsistencyLevel readConsistency) {
        ListenableFuture<ImmutableMap<Long, Long>> setsToResolve =
                index.lookup(Iterables.transform(ids, Id.toLongValue()), readConsistency);

        Futures.addCallback(
                Futures.transform(
                        setsToResolve,
                        toEquivalentsSets(selectedSources, activeAnnotations)
                ),
                new FutureCallback<Optional<ResolvedEquivalents<Content>>>() {

                    @Override
                    public void onSuccess(Optional<ResolvedEquivalents<Content>> resolved) {
                        /* Because QUORUM writes are used, reads may see a set in an inconsistent 
                         * state. If a set is read in an inconsistent state then a second read is 
                         * attempted at QUORUM level; slower being better than incorrect.
                         */
                        if (resolved.isPresent()) {
                            result.set(resolved.get());
                        } else if (readConsistency != ConsistencyLevel.QUORUM) {
                            resolveWithConsistency(
                                    result,
                                    ids,
                                    selectedSources,
                                    activeAnnotations,
                                    ConsistencyLevel.QUORUM
                            );
                        } else {
                            result.setException(new IllegalStateException("Failed to resolve "
                                    + ids));
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.setException(t);
                    }
                }
        );
    }

    private AsyncFunction<Map<Long, Long>, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            final Set<Publisher> selectedSources,
            final Set<Annotation> annotations
    ) {
        return index -> Futures.transform(
                resultOf(selectSetsQueries(index.values())),
                toEquivalentsSets(index, annotations, selectedSources)
        );
    }

    private Function<List<GraphAndDataResults>, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            Map<Long, Long> index,
            Set<Annotation> activeAnnotations,
            Set<Publisher> selectedSources
    ) {
        return results -> {
            Multimap<Long, Content> sets = deserialize(
                    results,
                    activeAnnotations,
                    selectedSources,
                    index
            );

            ResolvedEquivalents.Builder<Content> resolved = ResolvedEquivalents.builder();

            index.forEach((key, value) -> resolved.putEquivalents(
                    Id.valueOf(key),
                    sets.get(value)
            ));

            return Optional.of(resolved.build());
        };
    }

    private Multimap<Long, Content> deserialize(
            List<GraphAndDataResults> results,
            Set<Annotation> activeAnnotations,
            Set<Publisher> selectedSources,
            Map<Long, Long> index
    ) {
        ImmutableListMultimap<Long, Row> setRows = results.stream()
                .flatMap(graphAndDataResults -> graphAndDataResults.dataResult.all().stream())
                .collect(MoreCollectors.toImmutableListMultiMap(
                        row -> row.getLong(SET_ID_KEY),
                        row -> row
                ));

        ImmutableListMultimap<Long, Row> graphRows = results.stream()
                .flatMap(graphAndDataResults -> graphAndDataResults.graphResult.all().stream())
                .collect(MoreCollectors.toImmutableListMultiMap(
                        row -> row.getLong(SET_ID_KEY),
                        row -> row
                ));

        try {
            setRows.asMap().forEach((id, setRowsForId) -> {
                long rowBytes = 0;
                Collection<Row> graphRowsForId = graphRows.get(id);
                for (Row row : setRowsForId) {
                    for (int i = 0; i < row.getColumnDefinitions().size(); i++) {
                        rowBytes += row.getBytesUnsafe(i).remaining();
                    }
                }
                for (Row row : graphRowsForId) {
                    for (int i = 0; i < row.getColumnDefinitions().size(); i++) {
                        rowBytes += row.getBytesUnsafe(i).remaining();
                    }
                }
                if(rowBytes > 100000) {
                    log.info("Query for {} returned {} bytes", id, rowBytes);
                }
            });
        } catch(Exception e) {
            log.warn("Byte calculation failed for {}", graphRows.keys(), e);
        }

        ImmutableMap<Long, java.util.Optional<EquivalenceGraph>> graphs = deserializeGraphs(graphRows);
        ImmutableSetMultimap<Long, Content> content = deserializeContent(
                setRows,
                activeAnnotations
        );

        return filterContentSets(selectedSources, content, graphs, index);
    }

    private ImmutableMap<Long, java.util.Optional<EquivalenceGraph>> deserializeGraphs(
            ImmutableListMultimap<Long, Row> rows
    ) {
        ImmutableMap<Long, java.util.Optional<EquivalenceGraph>> graphs =
                rows.keySet()
                .stream()
                .collect(MoreCollectors.toImmutableMap(
                        setId -> setId,
                        // The graph column is a static C* field so if the graph is set this should
                        // always succeed to get a graph from the first row of the set
                        setId -> rows.get(setId)
                                .stream()
                                .map(this::deserializeGraph)
                                .filter(java.util.Optional::isPresent)
                                .map(java.util.Optional::get)
                                .findFirst()
                ));

        graphs.values()
                .stream()
                .filter(java.util.Optional::isPresent)
                .map(java.util.Optional::get)
                .filter(graph -> graph.getEquivalenceSet().size() > GRAPH_SIZE_ALERTING_THRESHOLD)
                .forEach(graph -> log.warn(
                        "Found large graph with id: {}, size: {}",
                        graph.getId(),
                        graph.getEquivalenceSet().size()
                ));

        return graphs;
    }

    private ImmutableSetMultimap<Long, Content> deserializeContent(
            ImmutableListMultimap<Long, Row> rows,
            Set<Annotation> activeAnnotations
    ) {
        return rows.entries()
                .stream()
                .collect(MoreCollectors.toImmutableSetMultiMap(
                        Entry::getKey,
                        rowEntry -> deserializeInternal(rowEntry.getValue(), activeAnnotations)
                ));
    }

    private Multimap<Long, Content> filterContentSets(
            Set<Publisher> selectedSources,
            ImmutableSetMultimap<Long, Content> content,
            ImmutableMap<Long, java.util.Optional<EquivalenceGraph>> graphs,
            Map<Long, Long> index
    ) {
        ImmutableSetMultimap.Builder<Long, Content> filteredContentBuilder =
                ImmutableSetMultimap.builder();

        ImmutableSetMultimap<Long, Long> inverseIndex = index.entrySet()
                .stream()
                .collect(MoreCollectors.toImmutableSetMultiMap(
                        Entry::getValue,
                        Entry::getKey
                ));

        for (Long setId : content.keySet()) {
            ImmutableSet<Content> filteredContent = content.get(setId)
                    .stream()
                    .filter(EquivalenceGraphFilter
                            .builder()
                            .withGraphEntryId(java.util.Optional.of(Id.valueOf(
                                    // If we have requested the same graph multiple
                                    // times from different IDs then arbitrarily pick one
                                    inverseIndex.get(setId).iterator().next()
                            )))
                            .withGraph(graphs.get(setId))
                            .withSelectedSources(selectedSources)
                            .withSelectedGraphSources(selectedSources)
                            .withActivelyPublishedIds(
                                    content.get(setId)
                                            .stream()
                                            .filter(Content::isActivelyPublished)
                                            .map(Content::getId)
                                            .collect(MoreCollectors.toImmutableSet())
                            )
                            .build()
                    )
                    .collect(MoreCollectors.toImmutableSet());

            filteredContentBuilder.putAll(
                    setId,
                    filteredContent
            );
        }

        return filteredContentBuilder.build();
    }

    private Content deserialize(Row row) {
        return deserializeInternal(row, Annotation.all());
    }

    private Content deserializeInternal(Row row, Set<Annotation> annotations) {
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
            Content content = contentSerializer.deserialize(buffer, annotations);
            if (content instanceof Item) {
                Item item = (Item) content;
                for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
                    segmentEvent.setPublisher(item.getSource());
                }
            }
            return content;
        } catch (IOException e) {
            throw new RuntimeException(setId + ":" + row.getLong(CONTENT_ID_KEY), e);
        }
    }

    private ListenableFuture<List<GraphAndDataResults>> resultOf(Iterable<GraphAndDataSelect> queries) {
        return Futures.allAsList(MoreStreams.stream(queries)
                .map(query -> new SetSelectFuture(
                        session.executeAsync(query.graphStatement),
                        session.executeAsync(query.dataStatement)))
                .collect(MoreCollectors.toImmutableList()));
    }

    private Iterable<GraphAndDataSelect> selectSetsQueries(Iterable<Long> keys) {
        return StreamSupport.stream(keys.spliterator(), false)
                .map(k -> new GraphAndDataSelect(
                        graphSelect.bind().setLong(SET_ID_BIND, k),
                        dataSelect.bind().setLong(SET_ID_BIND, k)
                        ))
                .collect(MoreCollectors.toImmutableList());
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
                .collect(MoreCollectors.toImmutableList());
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
                .collect(MoreCollectors.toImmutableList());
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
                .collect(MoreCollectors.toImmutableList());
    }

    private ImmutableList<Statement> getUpdateIndexRows(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {

        ImmutableList.Builder<Statement> statementBuilder = ImmutableList.builder();

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
                        if (containedInGraph(deserialized.getId(), deserializeGraph(row))) {
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
    private boolean containedInGraph(Id contentId, java.util.Optional<EquivalenceGraph> graph) {
        return !graph.isPresent() || graph.get().getEquivalenceSet().contains(contentId);
    }

    private java.util.Optional<EquivalenceGraph> deserializeGraph(Row row) {
        ByteBuffer graphBytes = row.getBytes(GRAPH_KEY);
        if (graphBytes == null) {
            return java.util.Optional.empty();
        }
        return java.util.Optional.of(
                graphSerializer.deserialize(graphBytes)
        );
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
                            .collect(MoreCollectors.toImmutableSet());
                }
        );
    }

    private class GraphAndDataSelect {
        final Statement graphStatement;
        final Statement dataStatement;

        GraphAndDataSelect(Statement graphStatement, Statement dataStatement) {
            this.graphStatement = graphStatement;
            this.dataStatement = dataStatement;
        }
    }

    private class GraphAndDataResults {
        final ResultSet graphResult;
        final ResultSet dataResult;

        GraphAndDataResults(ResultSet graphResult, ResultSet dataResult) {
            this.graphResult = graphResult;
            this.dataResult = dataResult;
        }
    }

    private class SetSelectFuture implements ListenableFuture<GraphAndDataResults> {
        final ListenableFuture<List<ResultSet>> combinedFuture;

        SetSelectFuture(ListenableFuture<ResultSet> graphResult, ListenableFuture<ResultSet> setResult) {
            combinedFuture = Futures.allAsList(graphResult, setResult); //order after get is same order as given on input
        }

        @Override
        public boolean cancel(boolean mayInterruptIfRunning) {
            return combinedFuture.cancel(mayInterruptIfRunning);
        }

        @Override
        public boolean isCancelled() {
            return combinedFuture.isCancelled();
        }

        @Override
        public boolean isDone() {
            return combinedFuture.isDone();
        }

        private GraphAndDataResults toGraphAndDataResults(List<ResultSet> resultSets) {
            assert(resultSets.size() == 2);
            return new GraphAndDataResults(resultSets.get(0), resultSets.get(1));
        }

        @Override
        public GraphAndDataResults get() throws InterruptedException, ExecutionException {
            return toGraphAndDataResults(combinedFuture.get());
        }

        @Override
        public GraphAndDataResults get(long timeout, TimeUnit unit) throws InterruptedException, ExecutionException, TimeoutException {
            return toGraphAndDataResults(combinedFuture.get(timeout, unit));
        }

        @Override
        public void addListener(Runnable listener, Executor executor) {
            combinedFuture.addListener(listener, executor);
        }
    }
}
