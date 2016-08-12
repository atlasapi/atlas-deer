package org.atlasapi.neo4j.service.writers;

import java.util.Arrays;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Content;
import org.atlasapi.content.ContentRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Series;
import org.atlasapi.content.Song;
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
import static org.hamcrest.core.IsNull.nullValue;
import static org.junit.Assert.assertThat;

public class ContentWriterIT extends AbstractNeo4jIT {

    private ContentWriter contentWriter;

    @Override
    @Before
    public void setUp() throws Exception {
        super.setUp();
        contentWriter = ContentWriter.create();
    }

    @Test
    public void writeResourceRef() throws Exception {
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
    public void writeExistingResourceRefUpdatesFields() throws Exception {
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

    @Test
    public void writeContentRef() throws Exception {
        ContentRef contentRef = getContentRef(new Item(), 0L, Publisher.METABROADCAST);

        contentWriter.writeContentRef(contentRef, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} }) "
                        + "RETURN n.id as id, n.source AS source, n.type AS type",
                ImmutableMap.of("id", contentRef.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("id").asLong(), is(contentRef.getId().longValue()));
        assertThat(record.get("source").asString(), is(contentRef.getSource().key()));
        assertThat(record.get("type").asString(), is(contentRef.getContentType().getKey()));
    }

    @Test
    public void writeExistingContentRefUpdatesFields() throws Exception {
        ContentRef contentRef = getContentRef(new Item(), 0L, Publisher.METABROADCAST);
        ContentRef updatedContentRef = getContentRef(new Episode(), 0L, Publisher.BBC);

        contentWriter.writeContentRef(contentRef, session);
        contentWriter.writeContentRef(updatedContentRef, session);

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} }) "
                        + "RETURN n.id as id, n.source AS source, n.type AS type",
                ImmutableMap.of("id", contentRef.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("id").asLong(), is(updatedContentRef.getId().longValue()));
        assertThat(record.get("source").asString(), is(updatedContentRef.getSource().key()));
        assertThat(record.get("type").asString(), is(updatedContentRef.getContentType().getKey()));
    }

    @Test
    public void writeBrand() throws Exception {
        writeContent(getContent(new Brand(), 0L, Publisher.METABROADCAST));
    }

    @Test
    public void writeItem() throws Exception {
        writeContent(getContent(new Item(), 0L, Publisher.METABROADCAST));
    }

    @Test
    public void writeFilm() throws Exception {
        writeContent(getContent(new Film(), 0L, Publisher.METABROADCAST));
    }

    @Test
    public void writeSong() throws Exception {
        writeContent(getContent(new Song(), 0L, Publisher.METABROADCAST));
    }

    @Test
    public void writeEpisodeWithNoEpisodeNumber() throws Exception {
        Episode episode = getContent(new Episode(), 0L, Publisher.METABROADCAST);

        Record record = writeEpisode(episode);

        assertThat(record.get("episodeNumber").asObject(), is(nullValue()));
    }

    @Test
    public void writeEpisodeWithCustomEpisodeFields() throws Exception {
        Episode episode = getContent(new Episode(), 0L, Publisher.METABROADCAST);
        episode.setEpisodeNumber(5);

        Record record = writeEpisode(episode);

        assertThat(record.get("episodeNumber").asInt(), is(episode.getEpisodeNumber()));
    }

    @Test
    public void changeEpisodeToItemRemovesEpisodeFields() throws Exception {
        Episode episode = getContent(new Episode(), 0L, Publisher.METABROADCAST);
        episode.setEpisodeNumber(2);

        writeEpisode(episode);

        Record updatedRecord = writeContent(
                getContent(new Item(), 0L, Publisher.METABROADCAST)
        );

        assertThat(updatedRecord.containsKey("episodeNumber"), is(false));
    }

    @Test
    public void writeSeriesWithNoSeriesNumber() throws Exception {
        Series series = getContent(new Series(), 0L, Publisher.METABROADCAST);

        Record record = writeSeries(series);

        assertThat(record.get("seriesNumber").asObject(), is(nullValue()));
    }

    @Test
    public void writeSeriesWithCustomSeriesFields() throws Exception {
        Series series = getContent(new Series(), 0L, Publisher.METABROADCAST);
        series.withSeriesNumber(2);

        Record record = writeSeries(series);

        assertThat(record.get("seriesNumber").asInt(), is(series.getSeriesNumber()));
    }

    @Test
    public void changeSeriesToItemRemovesSeriesFields() throws Exception {
        Series series = getContent(new Series(), 0L, Publisher.METABROADCAST);
        series.withSeriesNumber(2);

        writeSeries(series);

        Record updatedRecord = writeContent(
                getContent(new Item(), 0L, Publisher.METABROADCAST)
        );

        assertThat(updatedRecord.containsKey("seriesNumber"), is(false));
    }

    private Record writeContent(Content content) {
        contentWriter.writeContent(content, session);
        return getWrittenRecord(content);
    }

    private Record writeSeries(Series series) {
        contentWriter.writeSeries(series, session);
        return getWrittenRecord(series, "seriesNumber");
    }

    private Record writeEpisode(Episode episode) {
        contentWriter.writeEpisode(episode, session);
        return getWrittenRecord(episode, "episodeNumber");
    }

    private Record getWrittenRecord(Content content, String... customFields) {
        String customFieldsReturn = "";
        if (customFields.length > 0) {
            customFieldsReturn += ", ";
            customFieldsReturn += Arrays.stream(customFields)
                    .map(field -> "n." + field + " AS " + field)
                    .collect(Collectors.joining(", "));
        }

        StatementResult result = session.run(
                "MATCH (n:Content { id: {id} }) "
                        + "RETURN n.id as id, n.source AS source, n.type AS type"
                        + customFieldsReturn,
                ImmutableMap.of("id", content.getId().longValue())
        );

        assertThat(result.hasNext(), is(true));

        Record record = result.next();
        assertThat(record.get("id").asLong(), is(content.getId().longValue()));
        assertThat(record.get("source").asString(), is(content.getSource().key()));
        assertThat(record.get("type").asString(),
                is(ContentType.fromContent(content).get().getKey()));

        return record;
    }

    private ContentRef getContentRef(Content content, long id, Publisher source) {
        return getContent(content, id, source).toRef();
    }

    private <T extends Content> T getContent(T content, long id, Publisher source) {
        content.setId(Id.valueOf(id));
        content.setPublisher(source);
        content.setThisOrChildLastUpdated(DateTime.now());

        return content;
    }
}
