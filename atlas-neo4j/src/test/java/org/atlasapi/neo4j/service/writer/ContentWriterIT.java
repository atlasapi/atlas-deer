package org.atlasapi.neo4j.service.writer;

import java.util.Map;

import org.atlasapi.content.Brand;
import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.Film;
import org.atlasapi.content.Item;
import org.atlasapi.content.Location;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.junit.Before;
import org.junit.Test;
import org.neo4j.ogm.model.Result;
import org.neo4j.ogm.session.Session;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContentWriterIT {

    private Session session;
    private ContentWriter contentWriter;

    @Before
    public void setUp() throws Exception {
        session = Neo4jSessionFactory.createWithEmbeddedDriver().getNeo4jSession();
        contentWriter = ContentWriter.create(session);
    }

    @Test
    public void writeSingleItem() throws Exception {
        Content content = new Film(Id.valueOf(0L), Publisher.METABROADCAST);

        Long nodeId = contentWriter.write(content);

        String query = "MATCH (n:Film { id: {id}, source: {source} }) "
                + "RETURN id(n) AS id";

        Result result = session.query(query, ImmutableMap.of(
                "id", content.getId().longValue(),
                "source", content.getSource().key()
        ));

        assertThat(result.iterator().hasNext(), is(true));
        assertThat(result.iterator().next().get("id"), is(nodeId));
    }

    @Test
    public void writeItemWithBroadcasts() throws Exception {
        Item item = new Film(Id.valueOf(0L), Publisher.METABROADCAST);
        Broadcast broadcast = new Broadcast(
                Id.valueOf(1L), DateTime.now(), DateTime.now().plusHours(1)
        );

        item.addBroadcast(broadcast);

        Long nodeId = contentWriter.write(item);

        String query = "MATCH (n:Film { id: {id}, source: {source} })"
                + "-[:HAS_BROADCAST]->"
                + "(b:Broadcast { "
                        + "channelId: {channelId}, "
                        + "startDateTime: {startDateTime}, "
                        + "endDateTime: {endDateTime} "
                + "}) "
                + "RETURN id(n) AS id";

        Result result = session.query(query, ImmutableMap.of(
                "id", item.getId().longValue(),
                "source", item.getSource().key(),
                "channelId", broadcast.getChannelId().longValue(),
                "startDateTime", broadcast.getTransmissionTime().toString(),
                "endDateTime", broadcast.getTransmissionEndTime().toString()
        ));

        assertThat(result.iterator().hasNext(), is(true));
        assertThat(result.iterator().next().get("id"), is(nodeId));
    }

    @Test
    public void writeItemWithLocations() throws Exception {
        Item item = new Film(Id.valueOf(0L), Publisher.METABROADCAST);

        Location location = new Location();
        location.setAvailable(true);

        Policy policy = new Policy();
        policy.setAvailabilityStart(DateTime.now());
        policy.setAvailabilityEnd(DateTime.now().plusHours(1));

        location.setPolicy(policy);

        Encoding encoding = new Encoding();
        encoding.addAvailableAt(location);

        item.addManifestedAs(encoding);

        Long nodeId = contentWriter.write(item);

        String query = "MATCH (n:Film { id: {id}, source: {source} })"
                + "-[:HAS_LOCATION]->"
                + "(b:Location { "
                + "available: {available}, "
                + "startDateTime: {startDateTime}, "
                + "endDateTime: {endDateTime} "
                + "}) "
                + "RETURN id(n) AS id";

        Result result = session.query(query, ImmutableMap.of(
                "id", item.getId().longValue(),
                "source", item.getSource().key(),
                "available", location.getAvailable(),
                "startDateTime", location.getPolicy().getAvailabilityStart().toString(),
                "endDateTime", location.getPolicy().getAvailabilityEnd().toString()
        ));

        assertThat(result.iterator().hasNext(), is(true));
        assertThat(result.iterator().next().get("id"), is(nodeId));
    }

    @Test
    public void writeHierarchy() throws Exception {
        Brand brand = new Brand();
        brand.setId(Id.valueOf(0L));
        brand.setPublisher(Publisher.METABROADCAST);

        Series series = new Series();
        series.setId(Id.valueOf(1L));
        series.setPublisher(Publisher.METABROADCAST);
        series.withSeriesNumber(4);
        series.setBrand(brand);

        Episode episode = new Episode();
        episode.setId(Id.valueOf(3L));
        episode.setPublisher(Publisher.METABROADCAST);
        episode.setEpisodeNumber(3);
        episode.setContainer(series);
        episode.setThisOrChildLastUpdated(DateTime.now());

        brand.setSeriesRefs(ImmutableList.of(series.toRef()));
        series.setItemRefs(ImmutableSet.of(episode.toRef()));

        Long brandNodeId = contentWriter.write(brand);
        Long seriesNodeId = contentWriter.write(series);
        Long episodeNodeId = contentWriter.write(episode);

        String query = "MATCH (b:Brand { id: {bId}})"
                + "-[:HAS_CHILD]->"
                + "(s:Series { id: {sId} })"
                + "-[:HAS_CHILD]->"
                + "(e:Episode { id: {eId} })"
                + "RETURN id(b) AS bId, id(s) AS sId, id(e) AS eId";

        Result result = session.query(query, ImmutableMap.of(
                "bId", brand.getId().longValue(),
                "sId", series.getId().longValue(),
                "eId", episode.getId().longValue()
        ));

        assertThat(result.iterator().hasNext(), is(true));
        Map<String, Object> resultMap = result.iterator().next();
        assertThat(resultMap.get("bId"), is(brandNodeId));
        assertThat(resultMap.get("sId"), is(seriesNodeId));
        assertThat(resultMap.get("eId"), is(episodeNodeId));
    }
}
