package org.atlasapi.neo4j.service;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;

import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.junit.runner.RunWith;
import org.mockito.InOrder;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;
import org.neo4j.driver.v1.Session;
import org.neo4j.driver.v1.Transaction;

import static org.mockito.Mockito.doThrow;
import static org.mockito.Mockito.inOrder;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentGraphServiceTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    @Mock private Neo4jSessionFactory sessionFactory;
    @Mock private EquivalenceWriter equivalenceWriter;
    @Mock private ContentWriter contentWriter;
    @Mock private Session session;
    @Mock private Transaction transaction;

    private ContentGraphService graphService;

    private ContentRef contentRefA;
    private ContentRef contentRefB;

    @Before
    public void setUp() throws Exception {
        graphService = ContentGraphService.create(sessionFactory, equivalenceWriter, contentWriter);

        contentRefA = getContentRef(new Item(), 900L, Publisher.METABROADCAST);
        contentRefB = getContentRef(new Episode(), 901L, Publisher.BBC);

        when(sessionFactory.getSession()).thenReturn(session);
        when(session.beginTransaction()).thenReturn(transaction);
    }

    @Test
    public void writeEquivalencesCallsDelegatesInTransaction() throws Exception {
        graphService.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
        );

        InOrder order = inOrder(session, transaction, contentWriter, equivalenceWriter);
        order.verify(session).beginTransaction();
        order.verify(contentWriter).writeResourceRef(contentRefA, transaction);
        order.verify(contentWriter).writeResourceRef(contentRefB, transaction);
        order.verify(equivalenceWriter).writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                transaction
        );
        order.verify(transaction).success();
        order.verify(transaction).close();
    }

    @Test
    public void writeEquivalencesFailsTransactionIfDelegateFails() throws Exception {
        doThrow(new RuntimeException())
                .when(contentWriter).writeResourceRef(contentRefA, transaction);

        exception.expect(Neo4jPersistenceException.class);
        graphService.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC)
        );

        InOrder order = inOrder(session, transaction, contentWriter, equivalenceWriter);
        order.verify(session).beginTransaction();
        order.verify(contentWriter).writeResourceRef(contentRefA, transaction);
        order.verify(transaction).failure();
        order.verify(transaction).close();
    }

    private ContentRef getContentRef(Item content, long id, Publisher source) {
        content.setId(Id.valueOf(id));
        content.setPublisher(source);
        content.setThisOrChildLastUpdated(DateTime.now());

        return content.toRef();
    }
}
