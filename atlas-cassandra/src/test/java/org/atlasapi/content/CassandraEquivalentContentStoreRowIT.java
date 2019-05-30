package org.atlasapi.content;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.datastax.driver.core.querybuilder.QueryBuilder;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;
import org.atlasapi.annotation.Annotation;
import org.atlasapi.entity.Id;
import org.atlasapi.entity.ResourceRef;
import org.atlasapi.entity.util.WriteException;
import org.atlasapi.entity.util.WriteResult;
import org.atlasapi.equivalence.EquivalenceGraphUpdate;
import org.atlasapi.equivalence.ResolvedEquivalents;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.TestCassandraPersistenceModule;
import org.joda.time.DateTime;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.BeforeClass;
import org.junit.Test;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static com.datastax.driver.core.querybuilder.QueryBuilder.set;
import static org.atlasapi.content.CassandraEquivalentContentStore.CONTENT_ID_KEY;
import static org.atlasapi.content.CassandraEquivalentContentStore.EQUIVALENT_CONTENT_TABLE;
import static org.atlasapi.content.CassandraEquivalentContentStore.GRAPH_KEY;
import static org.atlasapi.content.CassandraEquivalentContentStore.SET_ID_KEY;
import static org.atlasapi.media.entity.Publisher.BBC;
import static org.atlasapi.media.entity.Publisher.METABROADCAST;
import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

public class CassandraEquivalentContentStoreRowIT {

    private static TestCassandraPersistenceModule persistenceModule;

    @BeforeClass
    public static void setup() throws Exception {
        persistenceModule = new TestCassandraPersistenceModule();
        persistenceModule.startAsync().awaitRunning(1, TimeUnit.MINUTES);
    }

    @After
    public void after() throws Exception {
        persistenceModule.reset();
    }

    @AfterClass
    public static void tearDown() {
        persistenceModule.tearDown();
    }

    @Test
    public void testRemovesOldRows() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(31), METABROADCAST);
        Content c4 = createAndWriteItem(Id.valueOf(41), METABROADCAST);
        Content c5 = createAndWriteItem(Id.valueOf(51), METABROADCAST);
        Content c6 = createAndWriteItem(Id.valueOf(61), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());
        persistenceModule.equivalentContentStore().updateContent(c3.getId());
        persistenceModule.equivalentContentStore().updateContent(c4.getId());
        persistenceModule.equivalentContentStore().updateContent(c5.getId());
        persistenceModule.equivalentContentStore().updateContent(c6.getId());

        makeEquivalent(c2, c4);
        makeEquivalent(c3, c5);

        resolved(c2, c2, c4);
        resolved(c3, c3, c5);

        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());

        makeEquivalent(c1, c2, c3);

        resolved(c1, c1, c2, c3, c4, c5);

        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c3.getId());
        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());

        makeEquivalent(c1, c2);

        resolved(c1, c1, c2, c4);
        resolved(c3, c3, c5);

        assertNoRowsWithIds(c1.getId(), c3.getId());
        assertNoRowsWithIds(c1.getId(), c5.getId());
        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c4.getId());
        assertNoRowsWithSetId(c5.getId());

        makeEquivalent(c2);

        resolved(c1, c1, c2);
        resolved(c4, c4);
        resolved(c3, c3, c5);

        assertNoRowsWithIds(c1.getId(), c4.getId());
        assertNoRowsWithSetId(c2.getId());
        assertNoRowsWithSetId(c5.getId());
    }

    @Test
    public void testResolveSingleContent() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());

        resolved(c1, c1);
    }

    @Test
    public void testResolveSingleContentWithNoGraph() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());

        persistenceModule.getCassandraSession().execute(
                QueryBuilder.update(EQUIVALENT_CONTENT_TABLE)
                        .where(eq(SET_ID_KEY, c1.getId().longValue()))
                        .with(set(GRAPH_KEY, null))
        );

        resolved(c1, c1);
    }

    @Test
    public void testResolveAllContentInSet() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());

        makeEquivalent(c1, c2);

        resolved(c1, c1, c2);
    }

    @Test
    public void testResolveAllContentInSetWithNoGraph() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());

        makeEquivalent(c1, c2);

        persistenceModule.equivalentContentStore().resolveEquivalentSet(c1.getId().longValue());
    }

    @Test
    public void testResolveSet() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());

        makeEquivalent(c1, c2);

        resolvedSet(c1.getId(), c1, c2);
    }

    @Test
    public void testResolveSetWithNoGraph() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());

        makeEquivalent(c1, c2);

        persistenceModule.getCassandraSession().execute(
                QueryBuilder.update(EQUIVALENT_CONTENT_TABLE)
                        .where(eq(SET_ID_KEY, c1.getId().longValue()))
                        .with(set(GRAPH_KEY, null))
        );

        resolvedSet(c1.getId(), c1, c2);
    }

    @Test
    public void testDoNotResolveContentThatIsNotInGraph() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(11), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());

        ResultSet result = persistenceModule.getCassandraSession().execute(
                QueryBuilder.select(GRAPH_KEY).from(EQUIVALENT_CONTENT_TABLE)
                        .where(eq(SET_ID_KEY, c1.getId().longValue()))
        );

        makeEquivalent(c1, c2);

        ByteBuffer oldGraph = result.iterator().next().getBytes(GRAPH_KEY);

        persistenceModule.getCassandraSession().execute(
                QueryBuilder.update(EQUIVALENT_CONTENT_TABLE)
                        .where(eq(SET_ID_KEY, c1.getId().longValue()))
                        .with(set(GRAPH_KEY, oldGraph))
        );

        resolvedSet(c1.getId(), c1);
    }

    @Test
    public void testDeletingGraphUpdatesStaleContent() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(1), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(2), METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(3), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());
        persistenceModule.equivalentContentStore().updateContent(c3.getId());

        makeEquivalent(c2, c3);

        // This is the message that should have split off c3 into its own graph, but the message
        // was not processed by the equivalent content store thus leaving c3 in the original set
        // as stale content
        persistenceModule.contentEquivalenceGraphStore().updateEquivalences(
                c2.toRef(), ImmutableSet.of(), ImmutableSet.of(METABROADCAST)
        );

        // This adds a content with a smaller ID to the set thus causing the canonical ID to change
        // and effectively causing the deletion of the old graph. c3 is no longer in the graph
        // as far as equivalence graph knows so it's not mentioned anywhere in this update
        Optional<EquivalenceGraphUpdate> updateOptional = persistenceModule
                .contentEquivalenceGraphStore()
                .updateEquivalences(
                        c1.toRef(), ImmutableSet.of(c2.toRef()), ImmutableSet.of(METABROADCAST)
                );

        persistenceModule.equivalentContentStore().updateEquivalences(updateOptional.get());

        resolvedSet(c1.getId(), c1, c2);
        resolvedSet(c3.getId(), c3);
    }

    @Test
    public void testResolvingTargetIdFromEquivalentSet() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(97), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(98), METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(99), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());
        persistenceModule.equivalentContentStore().updateContent(c3.getId());

        makeEquivalent(c1, c2, c3);

        resolvedWithoutEquivalence(c1.getId());
    }

    @Test
    public void resolveContentFilteredByGraph() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(1L), METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(2L), METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(3L), BBC);
        Content c4 = createAndWriteItem(Id.valueOf(4L), METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1.getId());
        persistenceModule.equivalentContentStore().updateContent(c2.getId());
        persistenceModule.equivalentContentStore().updateContent(c3.getId());
        persistenceModule.equivalentContentStore().updateContent(c4.getId());

        // Graph
        // c1 -> c2 -> c3 -> c4
        makeEquivalent(c1, c2);
        makeEquivalent(c2, c3);
        makeEquivalent(c3, c4);

        resolved(c1, c1, c2);
    }

    @Test
    public void testWritingAndRetrievingCustomFields() throws Exception {
        Content content = new Item(Id.valueOf(1L), METABROADCAST);
        content.addCustomField("testField", "testValue");
        content.addCustomField("testField2", "testValue2");
        writeContent(content);
        persistenceModule.equivalentContentStore().updateContent(content.getId());
        ResolvedEquivalents<Content> resolved
                = get(persistenceModule.equivalentContentStore()
                .resolveIds(ImmutableList.of(content.getId()),
                        ImmutableSet.of(METABROADCAST),
                        Annotation.all(),
                        false
                ));
        ImmutableSet<Content> resolvedContent = resolved.get(content.getId());
        assertThat(resolvedContent.size(), is(1));
        assertThat(resolvedContent.iterator().next().getCustomFields(), is(content.getCustomFields()));
    }

    private void assertNoRowsWithIds(Id setId, Id contentId) {
        Session session = persistenceModule.getCassandraSession();
        Statement rowsForIdQuery = select().all().from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, setId.longValue()))
                .and(eq(CONTENT_ID_KEY, contentId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(String.format(
                "Expected 0 rows for %s-%s, got %s",
                setId,
                contentId,
                rows.all().size()
        ), exhausted);
    }

    private void assertNoRowsWithSetId(Id setId) {
        Session session = persistenceModule.getCassandraSession();
        Statement rowsForIdQuery = select().all()
                .from(EQUIVALENT_CONTENT_TABLE)
                .where(eq(SET_ID_KEY, setId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(
                String.format("Expected 0 rows for %s, got %s", setId, rows.all().size()),
                exhausted
        );
    }

    private void resolved(Content c, Content... cs) throws Exception {
        ResolvedEquivalents<Content> resolved
                = get(persistenceModule.equivalentContentStore()
                .resolveIds(ImmutableList.of(c.getId()),
                        ImmutableSet.of(METABROADCAST),
                        Annotation.all(),
                        false
                ));
        ImmutableSet<Content> content = resolved.get(c.getId());

        assertThat(content.size(), is(cs.length));
        assertEquals(ImmutableSet.copyOf(cs), content);
    }

    private void resolvedWithoutEquivalence(Id setId) throws Exception {
        ResolvedEquivalents<Content> resolvedWithoutEquivalence = get(persistenceModule.equivalentContentStore()
                .
                        resolveIdsWithoutEquivalence(
                                ImmutableSet.of(setId),
                                ImmutableSet.of(METABROADCAST),
                                ImmutableSet.of(),
                                false
                        ));
        ResolvedEquivalents<Content> resolved = get(persistenceModule.equivalentContentStore().
                resolveIds(
                        ImmutableSet.of(setId),
                        ImmutableSet.of(METABROADCAST),
                        ImmutableSet.of(),
                        false
                ));
        assertEquals(resolved.size(), 3);
        assertEquals(resolvedWithoutEquivalence.size(), 1);
    }

    private void resolvedSet(Id setId, Content... cs) throws Exception {
        Set<Content> content = get(persistenceModule.equivalentContentStore()
                .resolveEquivalentSet(setId.longValue()));
        assertThat(content.size(), is(cs.length));
        assertEquals(ImmutableSet.copyOf(cs), content);
    }

    private <T> T get(ListenableFuture<T> resolveIds) throws Exception {
        return Futures.get(resolveIds, 10, TimeUnit.MINUTES, Exception.class);
    }

    private void makeEquivalent(Content c, Content... cs) throws WriteException {
        Set<ResourceRef> csRefs = ImmutableSet.<ResourceRef>copyOf(
                Iterables.transform(ImmutableSet.copyOf(cs), Content.toContentRef())
        );

        Optional<EquivalenceGraphUpdate> graphs
                = persistenceModule.contentEquivalenceGraphStore()
                .updateEquivalences(
                        c.toRef(),
                        csRefs,
                        ImmutableSet.of(METABROADCAST, BBC)
                );

        persistenceModule.equivalentContentStore().updateEquivalences(graphs.get());

    }

    private Content createAndWriteItem(Id id, Publisher src) throws WriteException {
        Content content = new Item(id, src);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        return writeContent(content);
    }

    private Content writeContent(Content content) throws WriteException {
        WriteResult<Content, Content> result = persistenceModule.contentStore()
                .writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        return content;
    }

}
