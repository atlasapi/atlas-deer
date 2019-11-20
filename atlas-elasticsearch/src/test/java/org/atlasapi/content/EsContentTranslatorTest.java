package org.atlasapi.content;

import java.util.List;
import java.util.Map;

import org.atlasapi.entity.Id;
import org.atlasapi.util.SecondaryIndex;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.elasticsearch.client.Client;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.junit.Before;
import org.junit.Test;
import org.junit.runner.RunWith;
import org.mockito.Mock;
import org.mockito.runners.MockitoJUnitRunner;

import static org.hamcrest.Matchers.is;
import static org.junit.Assert.assertThat;

@RunWith(MockitoJUnitRunner.class)
public class EsContentTranslatorTest {

    private @Mock Client client;
    private @Mock SecondaryIndex secondaryIndex;
    private @Mock ContentResolver contentResolver;

    private EsContentTranslator translator;

    @Before
    public void setUp() throws Exception {
        translator = new EsContentTranslator(
                "index", client, secondaryIndex, 10L
        );
    }

    @Test
    public void testDenormaliseEpisodeBroadcastsOnSeriesWhenSeriesHasNoBroadcasts()
            throws Exception {
        Broadcast broadcastA = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Broadcast broadcastB = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC).plusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1).plusHours(1)
        );

        Episode episode = getEpisode(broadcastA, broadcastB);
        Map<String, Object> existingSeries = getExistingSeries();

        Map<String, Object> series = translator.denormalizeEpisodeOntoSeries(
                episode, existingSeries
        );

        List<Map<String, Object>> actualBroadcasts =
                (List<Map<String, Object>>) series.get(EsContent.BROADCASTS);

        assertThat(actualBroadcasts.size(), is(2));

        isExpectedBroadcast(broadcastA, actualBroadcasts.get(0));
        isExpectedBroadcast(broadcastB, actualBroadcasts.get(1));
    }

    @Test
    public void testDoNotDenormalisedNonActivelyPublishedBroadcastsFromEpisodeOnSeries()
            throws Exception {
        Broadcast broadcastA = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );
        broadcastA.setIsActivelyPublished(false);

        Broadcast broadcastB = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC).plusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1).plusHours(1)
        );

        Episode episode = getEpisode(broadcastA, broadcastB);
        Map<String, Object> existingSeries = getExistingSeries();

        Map<String, Object> series = translator.denormalizeEpisodeOntoSeries(
                episode, existingSeries
        );

        List<Map<String, Object>> actualBroadcasts =
                (List<Map<String, Object>>) series.get(EsContent.BROADCASTS);

        assertThat(actualBroadcasts.size(), is(1));

        isExpectedBroadcast(broadcastB, actualBroadcasts.get(0));
    }

    @Test
    public void testKeepExistingBroadcastsWhenDenormalisingEpisodeBroadcastsOnSeries()
            throws Exception {
        Broadcast broadcastA = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Broadcast broadcastB = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC).plusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1).plusHours(1)
        );

        Episode episode = getEpisode(broadcastB);
        Map<String, Object> existingSeries = getExistingSeries(broadcastA);

        Map<String, Object> series = translator.denormalizeEpisodeOntoSeries(
                episode, existingSeries
        );

        List<Map<String, Object>> actualBroadcasts =
                (List<Map<String, Object>>) series.get(EsContent.BROADCASTS);

        assertThat(actualBroadcasts.size(), is(2));

        isExpectedBroadcast(broadcastA, actualBroadcasts.get(0));
        isExpectedBroadcast(broadcastB, actualBroadcasts.get(1));
    }

    @Test
    public void testMergeEpisodeBroadcastsWithParentBroadcastsWhenDenormalising() throws Exception {
        Broadcast broadcastA = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );

        Broadcast broadcastB = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC).plusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1).plusHours(1)
        );

        Episode episode = getEpisode(broadcastA, broadcastB);
        Map<String, Object> existingSeries = getExistingSeries(broadcastA);

        Map<String, Object> series = translator.denormalizeEpisodeOntoSeries(
                episode, existingSeries
        );

        List<Map<String, Object>> actualBroadcasts =
                (List<Map<String, Object>>) series.get(EsContent.BROADCASTS);

        assertThat(actualBroadcasts.size(), is(2));

        isExpectedBroadcast(broadcastA, actualBroadcasts.get(0));
        isExpectedBroadcast(broadcastB, actualBroadcasts.get(1));
    }

    @Test
    public void testRemoveExistingBroadcastIfNoLongerActivelyPublishedWhenDenormalising()
            throws Exception {
        Broadcast broadcastA = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC),
                DateTime.now(DateTimeZone.UTC).plusHours(1)
        );
        broadcastA.setIsActivelyPublished(false);

        Broadcast broadcastB = getBroadcast(
                10L,
                DateTime.now(DateTimeZone.UTC).plusDays(1),
                DateTime.now(DateTimeZone.UTC).plusDays(1).plusHours(1)
        );

        Episode episode = getEpisode(broadcastA, broadcastB);
        Map<String, Object> existingSeries = getExistingSeries(broadcastA);

        Map<String, Object> series = translator.denormalizeEpisodeOntoSeries(
                episode, existingSeries
        );

        List<Map<String, Object>> actualBroadcasts =
                (List<Map<String, Object>>) series.get(EsContent.BROADCASTS);

        assertThat(actualBroadcasts.size(), is(1));

        isExpectedBroadcast(broadcastB, actualBroadcasts.get(0));
    }

    private Episode getEpisode(Broadcast... broadcasts) {
        Episode episode = new Episode();

        for (Broadcast broadcast : broadcasts) {
            episode.addBroadcast(broadcast);
        }

        return episode;
    }

    private Broadcast getBroadcast(long channelId, DateTime startTime, DateTime endTime) {
        return new Broadcast(Id.valueOf(channelId), startTime, endTime);
    }

    private Map<String, Object> getExistingSeries(Broadcast... broadcasts) {
        Map<String, Object> existingSeries = Maps.newHashMap();
        existingSeries.put(EsContent.LOCATIONS, Lists.<Map<String, Object>>newArrayList());

        List<Map<String, Object>> esBroadcasts = Lists.<Map<String, Object>>newArrayList();
        for (Broadcast broadcast : broadcasts) {
            esBroadcasts.add(toEsBroadcast(broadcast));
        }
        existingSeries.put(EsContent.BROADCASTS, esBroadcasts);

        return existingSeries;
    }

    private Map<String, Object> toEsBroadcast(Broadcast broadcast) {
        return ImmutableMap.<String, Object>builder()
                .put(EsBroadcast.CHANNEL, (int) broadcast.getChannelId().longValue())
                .put(EsBroadcast.TRANSMISSION_TIME, broadcast.getTransmissionTime().toDate())
                .put(EsBroadcast.TRANSMISSION_END_TIME, broadcast.getTransmissionEndTime().toDate())
                .build();
    }

    private void isExpectedBroadcast(Broadcast expected, Map<String, Object> actual) {
        assertThat(
                actual.get(EsBroadcast.CHANNEL),
                is(expected.getChannelId().longValue())
        );

        DateTime actualStartTime = new DateTime(actual.get(EsBroadcast.TRANSMISSION_TIME));
        assertThat(actualStartTime.isEqual(expected.getTransmissionTime()), is(true));

        DateTime actualEndTime = new DateTime(actual.get(EsBroadcast.TRANSMISSION_END_TIME));
        assertThat(actualEndTime.isEqual(expected.getTransmissionEndTime()), is(true));
    }
}