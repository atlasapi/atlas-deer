package org.atlasapi.neo4j.service.writers;

import java.util.List;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.codahale.metrics.Timer;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class EquivalenceWriterIT extends AbstractNeo4jIT {

    @Rule public ExpectedException exception = ExpectedException.none();

    private EquivalenceWriter equivalenceWriter;
    private ContentWriter contentWriter;

    private ContentRef contentRefA;
    private ContentRef contentRefB;
    private ContentRef contentRefC;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        equivalenceWriter = EquivalenceWriter.create(new Timer());
        contentWriter = ContentWriter.create(new Timer(), new Timer(), new Timer());

        contentRefA = getContentRef(new Item(), 900L, Publisher.METABROADCAST);
        contentRefB = getContentRef(new Episode(), 901L, Publisher.BBC);
        contentRefC = getContentRef(new Item(), 902L, Publisher.PA);

        contentWriter.writeResourceRef(contentRefA, session);
        contentWriter.writeResourceRef(contentRefB, session);
        contentWriter.writeResourceRef(contentRefC, session);
    }

    @Test
    public void writeEquivalences() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                session
        );

        StatementResult result = session.run(
                "MATCH (a { id: {idA} })-[:IS_EQUIVALENT]->(b { id: {idB} })"
                        + "RETURN a.id AS idA, b.id AS idB",
                ImmutableMap.of(
                        "idA", contentRefA.getId().longValue(),
                        "idB", contentRefB.getId().longValue()
                )
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("idA").asLong(), is(contentRefA.getId().longValue()));
        assertThat(record.get("idB").asLong(), is(contentRefB.getId().longValue()));
    }

    @Test
    public void writeEdgeFromSubjectToSubject() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(),
                ImmutableSet.of(Publisher.METABROADCAST),
                session
        );

        StatementResult result = session.run(
                "MATCH (a { id: {idA} })-[:IS_EQUIVALENT]->(a { id: {idA} })"
                        + "RETURN a.id AS idA",
                ImmutableMap.of(
                        "idA", contentRefA.getId().longValue()
                )
        );

        assertThat(result.hasNext(), is(true));
    }

    @Test
    public void failIfSubjectSourceIsNotInAssertedSources() throws Exception {
        exception.expect(IllegalArgumentException.class);

        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.BBC),
                session
        );
    }

    @Test
    public void doNotWriteEquivalencesForNotAssertedSources() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefC),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                session
        );

        StatementResult firstResult = session.run(
                "MATCH (a { id: {idA} })-[:IS_EQUIVALENT]->(c { id: {idC} })"
                        + "RETURN a.id AS idA, c.id AS idC",
                ImmutableMap.of(
                        "idA", contentRefA.getId().longValue(),
                        "idC", contentRefC.getId().longValue()
                )
        );

        assertThat(firstResult.hasNext(), is(false));
    }

    @Test
    public void onlyAssertedEquivalencesAreWritten() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB, contentRefC),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC, Publisher.PA),
                session
        );

        String query = "MATCH (source { id: {id} })-[:IS_EQUIVALENT]->(target) "
                + "RETURN target.id as id";

        List<Record> firstResultRecords = session.run(
                query,
                ImmutableMap.of(
                        "id", contentRefA.getId().longValue()
                )
        ).list();

        assertThat(firstResultRecords.size(), is(3));
        assertThat(
                firstResultRecords.stream()
                        .map(record -> record.get("id").asLong())
                        .allMatch(id -> ImmutableSet.of(
                                contentRefA.getId().longValue(),
                                contentRefB.getId().longValue(),
                                contentRefC.getId().longValue()
                        ).contains(id)),
                is(true)
        );
    }

    @Test
    public void writeEquivalencesRemovesMissingEdges() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                session
        );

        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                session
        );

        StatementResult firstResult = session.run(
                "MATCH (a { id: {idA} })-[:IS_EQUIVALENT]->(b { id: {idB} })"
                        + "RETURN a.id AS idA, b.id AS idB",
                ImmutableMap.of(
                        "idA", contentRefA.getId().longValue(),
                        "idB", contentRefB.getId().longValue()
                )
        );

        assertThat(firstResult.hasNext(), is(false));
    }

    @Test
    public void writeEquivalencesDoesNotRemoveEdgesFoNotAssertedSources() throws Exception {
        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(contentRefB),
                ImmutableSet.of(Publisher.METABROADCAST, Publisher.BBC),
                session
        );

        equivalenceWriter.writeEquivalences(
                contentRefA,
                ImmutableSet.of(),
                ImmutableSet.of(Publisher.METABROADCAST),
                session
        );

        StatementResult result = session.run(
                "MATCH (a { id: {idA} })-[:IS_EQUIVALENT]->(b { id: {idB} })"
                        + "RETURN a.id AS idA, b.id AS idB",
                ImmutableMap.of(
                        "idA", contentRefA.getId().longValue(),
                        "idB", contentRefB.getId().longValue()
                )
        );

        assertThat(result.hasNext(), is(true));
    }

    private ContentRef getContentRef(Item content, long id, Publisher source) {
        content.setId(Id.valueOf(id));
        content.setPublisher(source);
        content.setThisOrChildLastUpdated(DateTime.now());

        return content.toRef();
    }
}
