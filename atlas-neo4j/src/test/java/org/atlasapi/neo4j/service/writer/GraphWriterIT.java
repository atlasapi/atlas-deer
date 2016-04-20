package org.atlasapi.neo4j.service.writer;

import java.util.Map;

import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class GraphWriterIT {

    private Session session;
    private GraphWriter graphWriter;
    private ContentWriter contentWriter;

    @Before
    public void setUp() throws Exception {
        session = Neo4jSessionFactory.createWithEmbeddedDriver().getNeo4jSession();
        graphWriter = GraphWriter.create(session);
        contentWriter = ContentWriter.create(session);
    }

    @Test
    public void writeGraph() throws Exception {
        Film film = new Film(Id.valueOf(0L), Publisher.METABROADCAST);
        film.setThisOrChildLastUpdated(DateTime.now());

        Item item = new Item(Id.valueOf(1L), Publisher.BBC);
        item.setThisOrChildLastUpdated(DateTime.now());

        Long filmNodeId = contentWriter.write(film);
        Long itemNodeId = contentWriter.write(item);

        EquivalenceGraph.Adjacents filmAdjacents = new EquivalenceGraph.Adjacents(
                film.toRef(),
                DateTime.now(),
                ImmutableSet.of(film.toRef(), item.toRef()),
                ImmutableSet.of(film.toRef())
        );

        EquivalenceGraph.Adjacents itemAdjacents = new EquivalenceGraph.Adjacents(
                item.toRef(),
                DateTime.now(),
                ImmutableSet.of(item.toRef()),
                ImmutableSet.of(item.toRef())
        );

        EquivalenceGraph graph = EquivalenceGraph.valueOf(
                ImmutableSet.of(filmAdjacents, itemAdjacents)
        );

        graphWriter.writeGraph(graph);

        assertEdge(film, item, filmNodeId, itemNodeId);
        assertEdge(film, film, filmNodeId, filmNodeId);
        assertEdge(item, item, itemNodeId, itemNodeId);
        assertEdgeAbsence(item, film);
    }

    private void assertEdge(Item from, Item to, Long fromNodeId, Long toNodeId) {
        String query = "MATCH (a { id: {aId}})"
                + "-[:IS_EQUIVALENT]->"
                + "(b { id: {bId} })"
                + "RETURN id(a) AS aId, id(b) AS bId";

        Result result = session.query(query, ImmutableMap.of(
                "aId", from.getId().longValue(),
                "bId", to.getId().longValue()
        ));

        assertThat(result.iterator().hasNext(), is(true));
        Map<String, Object> resultMap = result.iterator().next();
        assertThat(resultMap.get("aId"), is(fromNodeId));
        assertThat(resultMap.get("bId"), is(toNodeId));
    }

    private void assertEdgeAbsence(Item from, Item to) {
        String query = "MATCH (a { id: {aId}})"
                + "-[:IS_EQUIVALENT]->"
                + "(b { id: {bId} })"
                + "RETURN id(a) AS aId, id(b) AS bId";

        Result result = session.query(query, ImmutableMap.of(
                "aId", from.getId().longValue(),
                "bId", to.getId().longValue()
        ));

        assertThat(result.iterator().hasNext(), is(false));
    }
}
