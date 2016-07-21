package org.atlasapi.neo4j.service.writers;

import org.atlasapi.content.ContentRef;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Item;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.AbstractNeo4jIT;

import com.google.common.collect.ImmutableMap;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.driver.v1.Record;
import org.neo4j.driver.v1.StatementResult;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContentWriterIT extends AbstractNeo4jIT {

    private ContentWriter contentWriter;

    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentWriter = ContentWriter.create();
    }

    @Test
    public void writeContentRef() throws Exception {
        ContentRef contentRef = getContentRef(new Item(), 0L, Publisher.METABROADCAST);

        contentWriter.writeResourceRef(contentRef, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} }) "
                        + "RETURN n.id as id, n.source AS source",
                ImmutableMap.of("id", contentRef.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("id").asLong(), is(contentRef.getId().longValue()));
        assertThat(record.get("source").asString(), is(contentRef.getSource().key()));
    }

    @Test
    public void writeExistingContentRefUpdatesFields() throws Exception {
        ContentRef contentRef = getContentRef(new Item(), 0L, Publisher.METABROADCAST);
        ContentRef updatedContentRef = getContentRef(new Episode(), 0L, Publisher.BBC);

        contentWriter.writeResourceRef(contentRef, session);
        contentWriter.writeResourceRef(updatedContentRef, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} }) "
                        + "RETURN n.id as id, n.source AS source",
                ImmutableMap.of("id", contentRef.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("id").asLong(), is(updatedContentRef.getId().longValue()));
        assertThat(record.get("source").asString(), is(updatedContentRef.getSource().key()));
    }

    private ContentRef getContentRef(Item content, long id, Publisher source) {
        content.setId(Id.valueOf(id));
        content.setPublisher(source);
        content.setThisOrChildLastUpdated(DateTime.now());

        return content.toRef();
    }
}
