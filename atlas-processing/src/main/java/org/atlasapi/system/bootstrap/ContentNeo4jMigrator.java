package org.atlasapi.system.bootstrap;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.ContentNeo4jStore;

import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Sets;
import com.google.common.util.concurrent.Futures;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static com.google.common.base.Preconditions.checkNotNull;

public class ContentNeo4jMigrator {

    private static final Logger log = LoggerFactory.getLogger(ContentNeo4jMigrator.class);

    private final ContentNeo4jStore contentNeo4jStore;
    private final ContentStore contentStore;
    private final EquivalenceGraphStore equivalenceGraphStore;

    private ContentNeo4jMigrator(
            ContentNeo4jStore contentNeo4jStore,
            ContentStore contentStore,
            EquivalenceGraphStore equivalenceGraphStore
    ) {
        this.contentNeo4jStore = checkNotNull(contentNeo4jStore);
        this.contentStore = checkNotNull(contentStore);
        this.equivalenceGraphStore = checkNotNull(equivalenceGraphStore);
    }

    public static ContentNeo4jMigrator create(
            ContentNeo4jStore contentNeo4jStore,
            ContentStore contentStore,
            EquivalenceGraphStore equivalenceGraphStore
    ) {
        return new ContentNeo4jMigrator(
                contentNeo4jStore, contentStore, equivalenceGraphStore
        );
    }

    public Result migrate(Id id, boolean migrateEntireGraph) {
        try {
            return migrateInternal(resolveContent(id), migrateEntireGraph);
        } catch (Exception e) {
            log.error("Failed to migrate content", e);
            return Result.failure(id, Throwables.getStackTraceAsString(e));
        }
    }

    public Result migrate(Content content, boolean migrateEntireGraph) {
        try {
            return migrateInternal(content, migrateEntireGraph);
        } catch (Exception e) {
            log.error("Failed to migrate content", e);
            return Result.failure(content.getId(), Throwables.getStackTraceAsString(e));
        }
    }

    private Result migrateInternal(Content content, boolean migrateEntireGraph) {
        contentNeo4jStore.writeContent(content);

        Optional<EquivalenceGraph> graphOptional = resolveEquivalenceGraph(content.getId());

        if (!graphOptional.isPresent()) {
            return Result.successWithNoGraph(content.getId());
        }

        EquivalenceGraph graph = graphOptional.get();

        if (migrateEntireGraph) {
            graph.getAdjacencyList()
                    .values()
                    .forEach(
                            adjacents -> migrateGraph(adjacents.getRef(), adjacents.getAdjacent())
                    );

            return Result.successWithFullGraph(content.getId());
        } else {
            EquivalenceGraph.Adjacents adjacents = graph.getAdjacents(content.getId());
            migrateGraph(adjacents.getRef(), adjacents.getAdjacent());

            return Result.successWithGraph(content.getId());
        }
    }

    private void migrateGraph(ResourceRef resourceRef, Sets.SetView<ResourceRef> adjacents) {
        contentNeo4jStore.writeEquivalences(
                resourceRef,
                adjacents,
                Publisher.all()
        );
    }

    private Content resolveContent(Id id) {
        Resolved<Content> resolved =
                Futures.getUnchecked(
                        contentStore.resolveIds(ImmutableList.of(id))
                );
        return resolved.getResources().first().get();
    }

    private Optional<EquivalenceGraph> resolveEquivalenceGraph(Id id) {
        return Futures.getUnchecked(
                equivalenceGraphStore.resolveIds(ImmutableList.of(id))
        )
                .get(id);
    }

    public static class Result {

        private final Id id;
        private final boolean success;
        private final GraphMigrationResult graphMigrationResult;
        private final String message;

        private Result(
                Id id,
                boolean success,
                GraphMigrationResult graphMigrationResult,
                String message
        ) {
            this.id = checkNotNull(id);
            this.success = success;
            this.graphMigrationResult = checkNotNull(graphMigrationResult);
            this.message = checkNotNull(message);
        }

        public static Result successWithNoGraph(Id id) {
            return new Result(id, true, GraphMigrationResult.NONE, "");
        }

        public static Result successWithGraph(Id id) {
            return new Result(id, true, GraphMigrationResult.ADJACENTS_ONLY, "");
        }

        public static Result successWithFullGraph(Id id) {
            return new Result(id, true, GraphMigrationResult.FULL, "");
        }

        public static Result failure(Id id, String message) {
            return new Result(id, false, GraphMigrationResult.NONE, message);
        }

        public Id getId() {
            return id;
        }

        public boolean getSuccess() {
            return success;
        }

        public GraphMigrationResult getGraphMigrationResult() {
            return graphMigrationResult;
        }

        public String getMessage() {
            return message;
        }
    }

    public enum GraphMigrationResult {
        FULL,
        ADJACENTS_ONLY,
        NONE
    }
}
