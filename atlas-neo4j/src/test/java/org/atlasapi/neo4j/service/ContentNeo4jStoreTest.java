package org.atlasapi.neo4j.service;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Clip;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;
import org.atlasapi.neo4j.service.model.Neo4jPersistenceException;
import org.atlasapi.neo4j.service.writers.BroadcastWriter;
import org.atlasapi.neo4j.service.writers.ContentWriter;
import org.atlasapi.neo4j.service.writers.EquivalenceWriter;
import org.atlasapi.neo4j.service.writers.HierarchyWriter;
import org.atlasapi.neo4j.service.writers.LocationWriter;

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
import static org.mockito.Mockito.verify;
import static org.mockito.Mockito.when;

@RunWith(MockitoJUnitRunner.class)
public class ContentNeo4jStoreTest {

    @Rule public ExpectedException exception = ExpectedException.none();

    @Mock private Neo4jSessionFactory sessionFactory;
    @Mock private EquivalenceWriter equivalenceWriter;
    @Mock private ContentWriter contentWriter;
    @Mock private BroadcastWriter broadcastWriter;
    @Mock private LocationWriter locationWriter;
    @Mock private HierarchyWriter hierarchyWriter;
    @Mock private Session session;
    @Mock private Transaction transaction;

    private ContentNeo4jStore graphService;

    private ContentRef contentRefA;
    private ContentRef contentRefB;

    @Before
    public void setUp() throws Exception {
        graphService = ContentNeo4jStore.create(
                sessionFactory,
                equivalenceWriter,
                contentWriter,
                broadcastWriter,
                locationWriter,
                hierarchyWriter
        );

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

    @Test
    public void writeContentCreatesTransaction() throws Exception {
        Item item = new Item(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(item);

        InOrder order = inOrder(session, transaction, contentWriter);
        order.verify(session).beginTransaction();
        order.verify(contentWriter).writeContent(item, transaction);
        order.verify(transaction).success();
        order.verify(transaction).close();
    }

    @Test
    public void writeContentFailsTransactionIfDelegateFails() throws Exception {
        Item item = new Item(Id.valueOf(0L), Publisher.METABROADCAST);

        doThrow(new RuntimeException()).when(contentWriter).writeContent(item, transaction);

        exception.expect(Neo4jPersistenceException.class);
        graphService.writeContent(item);

        InOrder order = inOrder(session, transaction, contentWriter);
        order.verify(session).beginTransaction();
        order.verify(contentWriter).writeContent(item, transaction);
        order.verify(transaction).failure();
        order.verify(transaction).close();
    }

    @Test
    public void writeBrand() throws Exception {
        Brand brand = new Brand(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(brand);

        verify(contentWriter).writeContent(brand, transaction);
        verify(hierarchyWriter).writeBrand(brand, transaction);
        verify(locationWriter).write(brand, transaction);
    }

    @Test
    public void writeSeries() throws Exception {
        Series series = new Series(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(series);

        verify(contentWriter).writeSeries(series, transaction);
        verify(hierarchyWriter).writeSeries(series, transaction);
        verify(locationWriter).write(series, transaction);
    }

    @Test
    public void writeEpisode() throws Exception {
        Episode episode = new Episode(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(episode);

        verify(contentWriter).writeEpisode(episode, transaction);
        verify(hierarchyWriter).writeEpisode(episode, transaction);
        verify(locationWriter).write(episode, transaction);
        verify(broadcastWriter).write(episode, transaction);
    }

    @Test
    public void writeFilm() throws Exception {
        Film film = new Film(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(film);

        verify(contentWriter).writeContent(film, transaction);
        verify(hierarchyWriter).writeNoHierarchy(film, transaction);
        verify(locationWriter).write(film, transaction);
        verify(broadcastWriter).write(film, transaction);
    }

    @Test
    public void writeSong() throws Exception {
        Song song = new Song(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(song);

        verify(contentWriter).writeContent(song, transaction);
        verify(hierarchyWriter).writeNoHierarchy(song, transaction);
        verify(locationWriter).write(song, transaction);
        verify(broadcastWriter).write(song, transaction);
    }

    @Test
    public void writeItem() throws Exception {
        Item item = new Item(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(item);

        verify(contentWriter).writeContent(item, transaction);
        verify(hierarchyWriter).writeNoHierarchy(item, transaction);
        verify(locationWriter).write(item, transaction);
        verify(broadcastWriter).write(item, transaction);
    }

    @Test
    public void writeClip() throws Exception {
        Clip clip = new Clip(Id.valueOf(0L), Publisher.METABROADCAST);

        graphService.writeContent(clip);

        verify(contentWriter).writeContent(clip, transaction);
        verify(hierarchyWriter).writeNoHierarchy(clip, transaction);
        verify(locationWriter).write(clip, transaction);
        verify(broadcastWriter).write(clip, transaction);
    }

    private ContentRef getContentRef(Item content, long id, Publisher source) {
        content.setId(Id.valueOf(id));
        content.setPublisher(source);
        content.setThisOrChildLastUpdated(DateTime.now());

        return content.toRef();
    }
}
