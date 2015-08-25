package org.atlasapi.content;

import static com.datastax.driver.core.querybuilder.QueryBuilder.batch;
import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.in;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static com.datastax.driver.core.querybuilder.QueryBuilder.update;
import static com.google.common.base.Preconditions.checkNotNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.time.Instant;
import java.util.Collection;
import java.util.List;
import java.util.Map.Entry;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import com.google.common.collect.ImmutableList;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.segment.SegmentEvent;
import org.atlasapi.serialization.protobuf.ContentProtos;
import org.atlasapi.system.legacy.LegacyContentResolver;
import org.atlasapi.util.CassandraSecondaryIndex;
import org.atlasapi.util.ImmutableCollectors;
import org.atlasapi.util.SecondaryIndex;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.datastax.driver.core.ConsistencyLevel;
import com.datastax.driver.core.RegularStatement;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.Collections2;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.ImmutableSetMultimap;
import com.google.common.collect.Iterables;
import com.google.common.collect.Lists;
import com.google.common.util.concurrent.AsyncFunction;
import com.google.common.util.concurrent.FutureCallback;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.google.common.util.concurrent.SettableFuture;
import com.google.protobuf.ByteString;

public class CassandraEquivalentContentStore extends AbstractEquivalentContentStore {

    public static final String EQUIVALENT_CONTENT_INDEX = "equivalent_content_index";
    private static final String EQUIVALENT_CONTENT_TABLE = "equivalent_content";
    
    private static final String SET_ID_KEY = "set_id";
    private static final String CONTENT_ID_KEY = "content_id";
    private static final String CONTENT_KEY = "content";

    private final LegacyContentResolver legacyContentResolver;
    private final Session session;
    private final ConsistencyLevel writeConsistency;
    private final ConsistencyLevel readConsistency;

    private final SecondaryIndex index;

    private final Logger log = LoggerFactory.getLogger(getClass());
    private final ContentSerializer contentSerializer;
    
    public CassandraEquivalentContentStore(
            ContentResolver contentResolver,
            LegacyContentResolver legacyContentResolver,
            EquivalenceGraphStore graphStore,
            Session session,
            ConsistencyLevel read,
            ConsistencyLevel write
    ) {
        super(contentResolver, graphStore);
        this.legacyContentResolver = checkNotNull(legacyContentResolver);
        this.contentSerializer = new ContentSerializer(new ContentSerializationVisitor(contentResolver));
        this.session = session;
        this.readConsistency = read;
        this.writeConsistency = write;
        this.index = new CassandraSecondaryIndex(session, EQUIVALENT_CONTENT_INDEX, read);
    }



    @Override
    public ListenableFuture<ResolvedEquivalents<Content>> resolveIds(Iterable<Id> ids,
            Set<Publisher> selectedSources, Set<Annotation> activeAnnotations) {

        log.debug("Resolving IDs {}", Iterables.toString(ids));
        
        final SettableFuture<ResolvedEquivalents<Content>> result = SettableFuture.create();
  
        resolveWithConsistency(result, ids, selectedSources, readConsistency);
        
        return result;
    }

    private void resolveWithConsistency(
            final SettableFuture<ResolvedEquivalents<Content>> result,
            final Iterable<Id> ids,
            final Set<Publisher> selectedSources,
            final ConsistencyLevel readConsistency
    ) {
        ListenableFuture<Set<Long>> setsToResolve = Futures.transform(
                index.lookup(
                        Iterables.transform(
                                ids,
                                Id.toLongValue()
                        ),
                        readConsistency
                ),
                (ImmutableMap<Long, Long> input) -> {
                    return ImmutableSet.copyOf(input.values());
                }
        );
        
        Futures.addCallback(
                Futures.transform(
                        setsToResolve,
                        toEquivalentsSets(selectedSources, readConsistency)
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
                            resolveWithConsistency(result, ids, selectedSources, ConsistencyLevel.QUORUM);
                        } else {
                            result.setException(new IllegalStateException("Failed to resolve " + ids));
                        }
                    }

                    @Override
                    public void onFailure(Throwable t) {
                        result.setException(t);
                    }
                }
        );
    }

    private AsyncFunction<Set<Long>, Optional<ResolvedEquivalents<Content>>> toEquivalentsSets(
            final Set<Publisher> selectedSources,
            final ConsistencyLevel readConsistency
    ) {
        return setsToResolve -> Futures.transform(
                resultOf(
                        selectSetsQuery(setsToResolve, selectedSources),
                        readConsistency
                ),
                (List<Row> input) -> deserialize(input )

        );
    }

    private Optional<ResolvedEquivalents<Content>> deserialize(List<Row> setsRows) {
        ImmutableSetMultimap.Builder<Long, Content> sets = ImmutableSetMultimap.builder();
        if (setsRows.isEmpty()) {
            return Optional.absent();
        }
        for (Row row : setsRows) {
            long setId = row.getLong(SET_ID_KEY);
            Content content = deserialize(row);
            if (content instanceof Item) {
                Item item = (Item) content;
                for (SegmentEvent segmentEvent : item.getSegmentEvents()) {
                    segmentEvent.setPublisher(item.getSource());
                }
            }
            sets.put(setId, content);
        }
        ResolvedEquivalents.Builder<Content> equivalents =  ResolvedEquivalents.builder();
        for (Entry<Long, Collection<Content>> equivEntry : sets.build().asMap().entrySet()) {
            equivalents.putEquivalents(Id.valueOf(equivEntry.getKey()), equivEntry.getValue());
        }
        return Optional.of(
                equivalents.build()
        );
    }

    private Content deserialize(Row row) {
        long setId = row.getLong(SET_ID_KEY);
        try {
            //This is a workaround for a 'data' column being null for some old pieces of content.
            if (row.getBytes(CONTENT_KEY) == null) {
                long contentId = row.getLong(CONTENT_ID_KEY);
                log.warn(
                        "'{}' column empty for row  set_id = {}, content_id = {}, in '{}' column family",
                        CONTENT_KEY,
                        setId,
                        contentId,
                        EQUIVALENT_CONTENT_TABLE
                );
                Content content = resolvedContentFromNonEquivalentContentStore(Id.valueOf(contentId));
                updateContentColumn(setId, content);
                return content;

            }
            ByteString bytes = ByteString.copyFrom(row.getBytes(CONTENT_KEY));
            ContentProtos.Content buffer = ContentProtos.Content.parseFrom(bytes);
            return contentSerializer.deserialize(buffer);
        } catch (IOException e) {
            throw new RuntimeException(setId +":"+row.getLong(CONTENT_ID_KEY), e);
        }
    }

    //TODO more complex following of graph.
    private boolean contentSelected(
            Content content,
            Set<Publisher> selectedSources
    ) {
        return content.isActivelyPublished()
            && selectedSources.contains(content.getSource());
    }

    private ListenableFuture<List<Row>> resultOf(List<Statement> query, ConsistencyLevel readConsistency) {
        return Futures.transform(
                Futures.allAsList(
                        query.stream()
                                .map(q -> {
                                    q.setConsistencyLevel(readConsistency);
                                    return Futures.transform(
                                            session.executeAsync(q),
                                            ResultSet::all
                                    );
                                })
                                .collect(ImmutableCollectors.toList())
                ), (List<List<Row>> input) -> input.stream().flatMap( i -> i.stream()).collect(ImmutableCollectors.toList())
        );

    }

    private List<Statement> selectSetsQuery(Iterable<Long> keys, Set<Publisher> selectedSources) {
        return selectedSources.stream()
                .map(
                        pub -> {
                            System.out.println(select().all()
                                    .from(EQUIVALENT_CONTENT_TABLE)
                                    .where(in(SET_ID_KEY, ImmutableSet.copyOf(keys).toArray()))
                                    .and(eq("source", pub.key())).toString());
                            return select().all()
                                    .from(EQUIVALENT_CONTENT_TABLE)
                                    .where(in(SET_ID_KEY, ImmutableSet.copyOf(keys).toArray()))
                                    .and(eq("source", pub.key()));
                        }
                ).collect(ImmutableCollectors.toList());

    }
    
    @Override
    protected void updateEquivalences(
            ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent, EquivalenceGraphUpdate update) {
        if (graphsAndContent.isEmpty()) {
            log.warn("Empty content for " + update);
            return;
        }
        Instant start = Instant.now();
        updateContentRows(graphsAndContent);
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

    private void updateContentRows(ImmutableSetMultimap<EquivalenceGraph, Content> graphsAndContent) {
        List<Statement> updates = Lists.newArrayListWithExpectedSize(graphsAndContent.size());
        for (Entry<EquivalenceGraph, Content> graphAndContent : graphsAndContent.entries()) {
            EquivalenceGraph graph = graphAndContent.getKey();
            Content content = graphAndContent.getValue();
            updates.add(contentRowUpdateFor(graph, content));
        }
        session.execute(batch(updates.toArray(new RegularStatement[updates.size()]))
                .setConsistencyLevel(writeConsistency));
    }

    private Statement contentRowUpdateFor(EquivalenceGraph graph, Content content) {
        return update(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, graph.getId().longValue()))
                .and(eq(CONTENT_ID_KEY, content.getId().longValue()))
                .and(eq("source", content.getSource().key()))
                .with(set(CONTENT_KEY, serialize(content)));
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
        session.execute(contentRowUpdateFor(graph, content).setConsistencyLevel(writeConsistency));
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

    private void updateContentColumn(Long setId, Content content) {
        Statement statement = update((EQUIVALENT_CONTENT_TABLE))
                .where(eq(SET_ID_KEY, setId))
                .and(eq(CONTENT_ID_KEY, content.getId().longValue()))
                .with(set(CONTENT_KEY, serialize(content)));
        session.executeAsync(statement);
    }

}
