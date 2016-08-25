package org.atlasapi.system.bootstrap;

import org.atlasapi.content.Content;
import org.atlasapi.content.ContentStore;
import org.atlasapi.content.ItemRef;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.Resolved;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.equivalence.EquivalenceGraphStore;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.service.Neo4jContentStore;

import com.metabroadcast.common.collect.ImmutableOptionalMap;
import com.metabroadcast.common.collect.OptionalMap;

import com.google.api.client.repackaged.com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.util.concurrent.Futures;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;
import static org.mockito.Matchers.any;
import static org.mockito.Matchers.anySet;
import static org.mockito.Mockito.never;
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentNeo4jMigratorTest {

    @Mock private Neo4jContentStore neo4JContentStore;
    @Mock private ContentStore contentStore;
    @Mock private EquivalenceGraphStore equivalenceGraphStore;

    @Mock private Content content;

    private ContentNeo4jMigrator migrator;

    private Id id;

    @Before
    public void setUp() throws Exception {
        migrator = ContentNeo4jMigrator.create(
                neo4JContentStore, contentStore, equivalenceGraphStore
        );

        id = Id.valueOf(0L);
        when(content.getId()).thenReturn(id);
    }

    @Test
    public void migrateContentWithNoGraph() throws Exception {
        setupMocks(ImmutableOptionalMap.of());

        ContentNeo4jMigrator.Result result = migrator.migrate(id, false);

        verify(neo4JContentStore).writeContent(content);
        //noinspection unchecked
        verify(neo4JContentStore, never()).writeEquivalences(
                any(ResourceRef.class), anySet(), anySet()
        );

        assertThat(result.getId(), is(id));
        assertThat(result.getSuccess(), is(true));
        assertThat(result.getGraphMigrationResult(),
                is(ContentNeo4jMigrator.GraphMigrationResult.NONE));
        assertThat(result.getMessage(), is(""));
    }

    @Test
    public void migrateContentWithAdjacents() throws Exception {
        ResourceRef resourceRef = new ItemRef(
                id, Publisher.METABROADCAST, "", DateTime.now()
        );
        ResourceRef adjacentRef = new ItemRef(
                Id.valueOf(1L), Publisher.METABROADCAST, "", DateTime.now()
        );

        EquivalenceGraph graph = EquivalenceGraph.valueOf(ImmutableSet.of(
                EquivalenceGraph.Adjacents.valueOf(resourceRef)
                        .copyWithOutgoing(adjacentRef)
        ));

        setupMocks(ImmutableOptionalMap.of(id, graph));

        ContentNeo4jMigrator.Result result = migrator.migrate(id, false);

        verify(neo4JContentStore).writeContent(content);
        verify(neo4JContentStore).writeEquivalences(
                resourceRef, ImmutableSet.of(resourceRef, adjacentRef), Publisher.all()
        );

        assertThat(result.getId(), is(id));
        assertThat(result.getSuccess(), is(true));
        assertThat(result.getGraphMigrationResult(),
                is(ContentNeo4jMigrator.GraphMigrationResult.ADJACENTS_ONLY));
        assertThat(result.getMessage(), is(""));
    }

    @Test
    public void migrateContentWithFullGraph() throws Exception {
        ResourceRef resourceRef = new ItemRef(
                id, Publisher.METABROADCAST, "", DateTime.now()
        );
        ResourceRef adjacentRef = new ItemRef(
                Id.valueOf(1L), Publisher.METABROADCAST, "", DateTime.now()
        );

        EquivalenceGraph graph = EquivalenceGraph.valueOf(ImmutableSet.of(
                EquivalenceGraph.Adjacents.valueOf(resourceRef)
                        .copyWithOutgoing(adjacentRef),
                EquivalenceGraph.Adjacents.valueOf(adjacentRef)
        ));

        setupMocks(ImmutableOptionalMap.of(id, graph));

        ContentNeo4jMigrator.Result result = migrator.migrate(id, true);

        verify(neo4JContentStore).writeContent(content);
        verify(neo4JContentStore).writeEquivalences(
                resourceRef, ImmutableSet.of(resourceRef, adjacentRef), Publisher.all()
        );
        verify(neo4JContentStore).writeEquivalences(
                adjacentRef, ImmutableSet.of(adjacentRef), Publisher.all()
        );

        assertThat(result.getId(), is(id));
        assertThat(result.getSuccess(), is(true));
        assertThat(result.getGraphMigrationResult(),
                is(ContentNeo4jMigrator.GraphMigrationResult.FULL));
        assertThat(result.getMessage(), is(""));
    }

    @Test
    public void migrateContentWhenContentResolutionFailsReturnsFailedResult() throws Exception {
        RuntimeException exception = new RuntimeException();

        when(contentStore.resolveIds(ImmutableList.of(id))).thenThrow(exception);

        ContentNeo4jMigrator.Result result = migrator.migrate(id, false);

        verify(neo4JContentStore, never()).writeContent(content);
        //noinspection unchecked
        verify(neo4JContentStore, never()).writeEquivalences(
                any(ResourceRef.class), anySet(), anySet()
        );

        assertThat(result.getId(), is(id));
        assertThat(result.getSuccess(), is(false));
        assertThat(result.getGraphMigrationResult(),
                is(ContentNeo4jMigrator.GraphMigrationResult.NONE));
        assertThat(result.getMessage(), is(Throwables.getStackTraceAsString(exception)));
    }

    @Test
    public void migrateContentWhenGraphResolutionFailsReturnsFailedResult() throws Exception {
        when(contentStore.resolveIds(ImmutableList.of(id))).thenReturn(
                Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(content)))
        );

        RuntimeException exception = new RuntimeException();

        when(equivalenceGraphStore.resolveIds(ImmutableList.of(id))).thenThrow(exception);

        ContentNeo4jMigrator.Result result = migrator.migrate(id, false);

        verify(neo4JContentStore).writeContent(content);
        //noinspection unchecked
        verify(neo4JContentStore, never()).writeEquivalences(
                any(ResourceRef.class), anySet(), anySet()
        );

        assertThat(result.getId(), is(id));
        assertThat(result.getSuccess(), is(false));
        assertThat(result.getGraphMigrationResult(),
                is(ContentNeo4jMigrator.GraphMigrationResult.NONE));
        assertThat(result.getMessage(), is(Throwables.getStackTraceAsString(exception)));
    }

    private void setupMocks(OptionalMap<Id, EquivalenceGraph> graphMap) {
        when(contentStore.resolveIds(ImmutableList.of(id))).thenReturn(
                Futures.immediateFuture(Resolved.valueOf(ImmutableList.of(content)))
        );
        when(equivalenceGraphStore.resolveIds(ImmutableList.of(id))).thenReturn(
                Futures.immediateFuture(graphMap)
        );
    }

}
