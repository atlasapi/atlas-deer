package org.atlasapi.neo4j.service;

import java.util.Map;
import java.util.Optional;

import org.atlasapi.content.Broadcast;
import org.atlasapi.content.Content;
import org.atlasapi.content.Encoding;
import org.atlasapi.content.Episode;
import org.atlasapi.content.IndexQueryParams;
import org.atlasapi.content.IndexQueryResult;
import org.atlasapi.content.Location;
import org.atlasapi.content.Policy;
import org.atlasapi.content.Series;
import org.atlasapi.entity.Id;
import org.atlasapi.equivalence.EquivalenceGraph;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.neo4j.Neo4jSessionFactory;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import org.neo4j.ogm.session.Session;

import static org.hamcrest.core.Is.is;
import static org.junit.Assert.assertThat;

public class ContentGraphServiceIT {

    private static Session session;
    private static ContentGraphService contentGraphService;

    private Map<String, String> requestParameters;
    private Series series;
    private Series equivalentSeries;
    private Episode episode;
    private Episode equivalentEpisode;

    @BeforeClass
    public static void init() {
        session = Neo4jSessionFactory.createWithEmbeddedDriver().getNeo4jSession();
        contentGraphService = ContentGraphService.create(session);
    }

    @Before
    public void setUp() throws Exception {
        session.purgeDatabase();

        requestParameters = ImmutableMap.of(
                "actionableFilterParameters", "location.available:true",
                "type", "episode",
                "series.id", "0L"
        );

        series = getSeries(Id.valueOf(0L), Publisher.METABROADCAST);
        equivalentSeries = getSeries(Id.valueOf(10L), Publisher.BBC);
        episode = getEpisode(Id.valueOf(1L), Publisher.METABROADCAST, 1);
        equivalentEpisode = getEpisode(Id.valueOf(20L), Publisher.BBC, 1);

        series.setItemRefs(ImmutableList.of(episode.toRef()));
        episode.setSeries(series);
        episode.setContainer(series);
    }

    @Test
    public void getEpisodeThatHasActionableLocation() throws Exception {
        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        writeSingleContent(series, episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(series, ImmutableMap.of("location.available", "true")),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void doNotGetEpisodeThatDoesNotHaveActionableLocation() throws Exception {
        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().plusHours(1),
                DateTime.now().plusHours(2)
        ));

        writeSingleContent(series, episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(series, ImmutableMap.of("location.available", "true")),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(0));
    }

    @Test
    public void getEpisodeThatHasActionableBroadcast() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        episode.addBroadcast(new Broadcast(
                Id.valueOf(2L), now.plusMinutes(10), now.plusMinutes(30)
        ));

        writeSingleContent(series, episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(
                    series,
                    ImmutableMap.of(
                            "broadcast.time.gt", now.toString(),
                            "broadcast.time.lt", now.plusHours(1).toString()
                    )),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void doNotGetEpisodeThatDoesNotHaveActionableBroadcast() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        series.setItemRefs(ImmutableList.of(episode.toRef()));
        episode.setSeries(series);

        Broadcast broadcast = new Broadcast(
                Id.valueOf(2L), now.plusMinutes(10), now.plusMinutes(30)
        );
        episode.addBroadcast(broadcast);

        writeSingleContent(series, episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(
                    series,
                    ImmutableMap.of(
                            "broadcast.time.gt", now.plusHours(1).toString(),
                            "broadcast.time.lt", now.plusHours(2).toString()
                    )),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(0));
    }

    @Test
    public void getEpisodeThatHasActionableLocationAndBroadcast() throws Exception {
        DateTime now = DateTime.now(DateTimeZone.UTC);

        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));
        episode.addBroadcast(new Broadcast(
                Id.valueOf(2L), now.plusMinutes(10), now.plusMinutes(30)
        ));

        writeSingleContent(series, episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(series, ImmutableMap.of(
                        "location.available", "true",
                        "broadcast.time.gt", now.plusHours(1).toString(),
                        "broadcast.time.lt", now.plusHours(2).toString()
                )),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void getEpisodeThatHasAnEquivalentThatIsActionable() throws Exception {
        equivalentEpisode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        writeSingleContent(series);
        writeEquivContent(episode, equivalentEpisode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(series, ImmutableMap.of("location.available", "true")),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void onlyReturnOneIdWhenMultipleEquivalentEpisodesAreActionable() throws Exception {
        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        equivalentEpisode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        writeSingleContent(series);
        writeEquivContent(episode, equivalentEpisode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(series, ImmutableMap.of("location.available", "true")),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void getEpisodeUnderEquivalentSeriesThatIsActionable() throws Exception {
        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        writeEquivContent(series, equivalentSeries);
        writeSingleContent(episode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(
                        equivalentSeries, ImmutableMap.of("location.available", "true")
                ),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(episode.getId()), is(true));
    }

    @Test
    public void getActionableEquivalentEpisodeWhenEpisodeDoesNotHaveEnabledPublisher()
            throws Exception {
        equivalentEpisode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        writeSingleContent(series);
        writeEquivContent(episode, equivalentEpisode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(
                        series, ImmutableMap.of("location.available", "true")
                ),
                ImmutableSet.of(Publisher.BBC),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(1));
        assertThat(result.getIds().contains(equivalentEpisode.getId()), is(true));
    }

    @Test
    public void orderMultipleEpisodesByEpisodeNumber() throws Exception {
        episode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusHours(1),
                DateTime.now().plusHours(1)
        ));

        Episode secondEpisode = getEpisode(Id.valueOf(30L), Publisher.METABROADCAST, 2);
        secondEpisode.addManifestedAs(getEncodingWithLocation(
                DateTime.now().minusDays(1),
                DateTime.now().plusDays(1)
        ));

        series.setItemRefs(ImmutableList.of(episode.toRef(), secondEpisode.toRef()));
        secondEpisode.setSeries(series);
        secondEpisode.setContainer(series);

        writeSingleContent(series, episode, secondEpisode);

        IndexQueryResult result = contentGraphService.query(
                getActionableEpisodeQuery(
                        series, ImmutableMap.of("location.available", "true")
                ),
                Publisher.all(),
                requestParameters
        ).get().get();

        assertThat(result.getIds().size(), is(2));
        assertThat(result.getIds().get(0), is(episode.getId()));
        assertThat(result.getIds().get(1), is(secondEpisode.getId()));
    }

    private Series getSeries(Id id, Publisher publisher) {
        Series series = new Series(id, publisher);
        series.withSeriesNumber(1);
        return series;
    }

    private Episode getEpisode(Id id, Publisher publisher, int episodeNumber) {
        Episode episode = new Episode(id, publisher);
        episode.setEpisodeNumber(episodeNumber);
        episode.setThisOrChildLastUpdated(DateTime.now());
        return episode;
    }

    private void writeSingleContent(Content... contents) {
        for (Content content : contents) {
            contentGraphService.writeEquivalentSet(
                    EquivalenceGraph.valueOf(content.toRef()),
                    ImmutableList.of(content)
            );
        }
    }

    private void writeEquivContent(Content content, Content equivContent) {
        EquivalenceGraph graph = EquivalenceGraph.valueOf(ImmutableSet.of(
                EquivalenceGraph.Adjacents.valueOf(content.toRef())
                        .copyWithEfferent(equivContent.toRef()),
                EquivalenceGraph.Adjacents.valueOf(equivContent.toRef())
                        .copyWithAfferent(content.toRef())
        ));

        contentGraphService.writeEquivalentSet(graph, ImmutableList.of(content, equivContent));
    }

    private Encoding getEncodingWithLocation(DateTime availabilityStart, DateTime availabilityEnd) {
        Location location = new Location();
        location.setAvailable(true);

        Policy policy = new Policy();
        policy.setAvailabilityStart(availabilityStart);
        policy.setAvailabilityEnd(availabilityEnd);

        location.setPolicy(policy);

        Encoding encoding = new Encoding();
        encoding.setAvailableAt(ImmutableSet.of(location));

        return encoding;
    }

    private IndexQueryParams getActionableEpisodeQuery(Series series,
            ImmutableMap<String, String> actionableParameters) {
        return new IndexQueryParams(
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Optional.empty(),
                Boolean.FALSE,
                Optional.empty(),
                Optional.of(actionableParameters),
                Optional.of(series.getId())
        );
    }
}
