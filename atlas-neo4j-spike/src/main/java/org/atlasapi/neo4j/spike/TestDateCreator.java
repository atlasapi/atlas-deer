package org.atlasapi.neo4j.spike;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.IntStream;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentIndex;
import org.atlasapi.content.ContentResolver;
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.criteria.AttributeQuerySet;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jModule;
import org.atlasapi.util.ImmutableCollectors;

import com.metabroadcast.common.collect.OptionalMap;
import com.metabroadcast.common.query.Selection;

import com.google.common.base.Throwables;
import com.google.common.collect.FluentIterable;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class TestDateCreator {

    private static final Logger LOG = LoggerFactory.getLogger(TestDateCreator.class);

    private final Neo4jModule neo4jModule;
    private final ContentIndex contentIndex;
    private final ContentResolver contentResolver;
    private final EquivalenceGraphStore equivalenceGraphStore;
    private final int limit;
    private final int maxOffset;

    private final Set<Publisher> publishers;

    private final ExecutorService executorService;

    public TestDateCreator(Neo4jModule neo4jModule, ContentIndex contentIndex,
            ContentResolver contentResolver, EquivalenceGraphStore equivalenceGraphStore,
            int limit, int maxOffset, Iterable<Publisher> publishers) {
        this.neo4jModule = checkNotNull(neo4jModule);
        this.contentIndex = checkNotNull(contentIndex);
        this.contentResolver = checkNotNull(contentResolver);
        this.equivalenceGraphStore = checkNotNull(equivalenceGraphStore);
        this.limit = limit;
        this.maxOffset = maxOffset;

        this.publishers = ImmutableSet.copyOf(publishers);

        this.executorService = Executors.newFixedThreadPool(3);
    }

    public void createTestData() throws ExecutionException, InterruptedException {
        ImmutableList<Callable<Boolean>> tasks = IntStream.iterate(0, offset -> offset + limit)
                .limit(maxOffset / limit)
                .mapToObj(offset -> createTestDataTask(offset, limit))
                .collect(ImmutableCollectors.toList());

        List<Future<Boolean>> futures = executorService.invokeAll(tasks);

        for (Future<Boolean> future : futures) {
            future.get();
        }
    }

    private Callable<Boolean> createTestDataTask(int offset, int limit) {
        return () -> {
            try {
                LOG.info("Offset: {} - Starting", offset);
                createTestDataPage(offset, limit);
                LOG.info("Offset: {} - Finished", offset);
                return true;
            } catch (RuntimeException e) {
                LOG.error("Offset: {} - FAILED", offset, e);
                throw Throwables.propagate(e);
            }
        };
    }

    private void createTestDataPage(int offset, int limit) {
        FluentIterable<Id> ids = getCanonicalIds(offset, limit).getIds();
        LOG.info("Offset: {} - Received canonical IDs", offset);

        ImmutableMap<Id, ContentAndGraph> equivalentSets = getEquivalentSet(ids);
        LOG.info("Offset: {} - Received equivalent sets", offset);

        equivalentSets.values().stream()
                .forEach(contentAndGraph -> writeToNeo4j(
                        contentAndGraph.getGraph(),
                        contentAndGraph.getContent()
                ));
    }

    private IndexQueryResult getCanonicalIds(int offset, int limit) {
        try {
            return contentIndex.query(
                    new AttributeQuerySet(ImmutableSet.of()),
                    publishers,
                    new Selection(offset, limit),
                    Optional.<IndexQueryParams>empty()
            ).get(30_000, TimeUnit.MILLISECONDS);
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private ImmutableMap<Id, ContentAndGraph> getEquivalentSet(Iterable<Id> ids) {
        OptionalMap<Id, EquivalenceGraph> graphs;
        try {
            graphs = equivalenceGraphStore.resolveIds(ids)
                    .get(30_000, TimeUnit.MILLISECONDS);
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw Throwables.propagate(e);
        }

        return graphs.entrySet().stream()
                .filter(entry -> entry.getValue().isPresent())
                .collect(ImmutableCollectors.toMap(
                        Map.Entry::getKey,
                        entry -> new ContentAndGraph(
                                entry.getValue().get(),
                                getContent(entry.getValue().get())
                        )
                ));
    }

    private FluentIterable<Content> getContent(EquivalenceGraph graph) {
        try {
            return contentResolver.resolveIds(graph.getEquivalenceSet())
                    .get(30_000, TimeUnit.MILLISECONDS)
                    .getResources();
        } catch (ExecutionException | InterruptedException | TimeoutException e) {
            throw Throwables.propagate(e);
        }
    }

    private void writeToNeo4j(EquivalenceGraph graph, Iterable<Content> content) {
        neo4jModule.spikeContentNodeService().writeEquivalentSet(graph, content);
    }

    private class ContentAndGraph {

        private EquivalenceGraph graph;
        private FluentIterable<Content> content;

        public ContentAndGraph(EquivalenceGraph graph,
                FluentIterable<Content> content) {
            this.graph = graph;
            this.content = content;
        }

        public EquivalenceGraph getGraph() {
            return graph;
        }

        public FluentIterable<Content> getContent() {
            return content;
        }
    }
}
