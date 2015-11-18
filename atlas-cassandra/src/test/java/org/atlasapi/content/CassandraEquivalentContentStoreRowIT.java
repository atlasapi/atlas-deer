package org.atlasapi.content;

import static com.datastax.driver.core.querybuilder.QueryBuilder.delete;
import static com.datastax.driver.core.querybuilder.QueryBuilder.eq;
import static com.datastax.driver.core.querybuilder.QueryBuilder.select;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.Matchers.not;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;
import static org.junit.Assert.assertTrue;

import java.nio.ByteBuffer;
import java.util.Set;
import java.util.concurrent.TimeUnit;

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

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.Statement;
import com.google.common.base.Optional;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import com.google.common.util.concurrent.Futures;
import com.google.common.util.concurrent.ListenableFuture;
import com.metabroadcast.common.time.DateTimeZones;


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
        Content c1 = createAndWriteItem(Id.valueOf(11), Publisher.METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(21), Publisher.METABROADCAST);
        Content c3 = createAndWriteItem(Id.valueOf(31), Publisher.METABROADCAST);
        Content c4 = createAndWriteItem(Id.valueOf(41), Publisher.METABROADCAST);
        Content c5 = createAndWriteItem(Id.valueOf(51), Publisher.METABROADCAST);
        Content c6 = createAndWriteItem(Id.valueOf(61), Publisher.METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1);
        persistenceModule.equivalentContentStore().updateContent(c2);
        persistenceModule.equivalentContentStore().updateContent(c3);
        persistenceModule.equivalentContentStore().updateContent(c4);
        persistenceModule.equivalentContentStore().updateContent(c5);
        persistenceModule.equivalentContentStore().updateContent(c6);
        
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
    public void testResolvesNullContentColumn() throws Exception {
        Content c1 = createAndWriteItem(Id.valueOf(1), Publisher.METABROADCAST);
        Content c2 = createAndWriteItem(Id.valueOf(2), Publisher.METABROADCAST);

        persistenceModule.equivalentContentStore().updateContent(c1);
        persistenceModule.equivalentContentStore().updateContent(c2);

        makeEquivalent(c1, c2);
        makeEquivalent(c2, c1);

        Statement delete = delete().column("data").from("equivalent_content").where(
                eq("set_id", 1)
        ).and(eq("content_id", 2));

        Session session = persistenceModule.getCassandraSession();
        session.execute(delete);

        resolved(c1, c1, c2);
        resolved(c2, c1, c2);

        assertThatDataRowIsPresent(1, 2);

    }


    private void assertNoRowsWithIds(Id setId, Id contentId) {
        Session session = persistenceModule.getCassandraSession();
        Statement rowsForIdQuery = select().all().from("equivalent_content")
                .where(eq("set_id", setId.longValue()))
                .and(eq("content_id", contentId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(String.format("Expected 0 rows for %s-%s, got %s", setId, contentId, rows.all().size()), exhausted);
    }

    private void assertNoRowsWithSetId(Id setId) {
        Session session = persistenceModule.getCassandraSession();
        Statement rowsForIdQuery = select().all().from("equivalent_content").where(eq("set_id", setId.longValue()));
        ResultSet rows = session.execute(rowsForIdQuery);
        boolean exhausted = rows.isExhausted();
        assertTrue(String.format("Expected 0 rows for %s, got %s", setId, rows.all().size()), exhausted);
    }

    private void assertThatDataRowIsPresent(long setId, long contentId) {
        Session session = persistenceModule.getCassandraSession();
        Statement rowsForIdQuery = select().all().from("equivalent_content")
                .where(eq("set_id", setId))
                .and(eq("content_id", contentId));
        ResultSet rows = session.execute(rowsForIdQuery);
        assertThat(rows.one().getBytes("data"), not(nullValue(ByteBuffer.class)));
    }

    private void resolved(Content c, Content... cs) throws Exception {
        ResolvedEquivalents<Content> resolved
            = get(persistenceModule.equivalentContentStore().resolveIds(ImmutableList.of(c.getId()), 
                    ImmutableSet.of(Publisher.METABROADCAST), Annotation.all()));
        ImmutableSet<Content> idContent = resolved.get(c.getId());
        assertEquals(ImmutableSet.copyOf(cs), idContent);
    }

    private <T> T get(ListenableFuture<T> resolveIds) throws Exception {
        return Futures.get(resolveIds, 10, TimeUnit.MINUTES, Exception.class);
    }
    

    private void makeEquivalent(Content c, Content... cs) throws WriteException {
        Set<ResourceRef> csRefs = ImmutableSet.<ResourceRef>copyOf(
                Iterables.transform(ImmutableSet.copyOf(cs), Content.toContentRef()));
       
        Optional<EquivalenceGraphUpdate> graphs
            = persistenceModule.contentEquivalenceGraphStore().updateEquivalences(c.toRef(), csRefs, 
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC));
        
        persistenceModule.equivalentContentStore().updateEquivalences(graphs.get());
        
    }

    private Content createAndWriteItem(Id id, Publisher src) throws WriteException {
        Content content = new Item(id, src);
        content.setThisOrChildLastUpdated(new DateTime(DateTimeZones.UTC));
        
        WriteResult<Content, Content> result = persistenceModule.contentStore().writeContent(content);
        assertTrue("Failed to write " + content, result.written());
        return content;
    }

}
