package org.atlasapi.content.v2.serialization.setters;

import java.time.OffsetDateTime;
import java.util.Map;

import org.atlasapi.content.Episode;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.serialization.RefSerialization;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.util.PagerDutyClientWithKey;

import com.github.dikhan.pagerduty.client.events.PagerDutyEventsClient;
import com.github.dikhan.pagerduty.client.events.domain.Payload;
import com.github.dikhan.pagerduty.client.events.domain.Severity;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class EpisodeSetter {
    private static final Logger log = LoggerFactory.getLogger(EpisodeSetter.class);

    private final SeriesRefSerialization seriesRefSerialization = new SeriesRefSerialization();
    private final RefSerialization refSerialization = new RefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Episode.class.isInstance(content)) {
            return;
        }

        Episode episode = (Episode) content;
        internal.setSeriesNumber(episode.getSeriesNumber());
        internal.setEpisodeNumber(episode.getEpisodeNumber());
        internal.setPartNumber(episode.getPartNumber());
        internal.setSpecial(episode.getSpecial());
        org.atlasapi.content.SeriesRef seriesRef = episode.getSeriesRef();
        SeriesRef ref = seriesRefSerialization.serialize(seriesRef);
        if (ref != null) {
            internal.setSeriesRefs(ImmutableMap.of(
                    refSerialization.serialize(seriesRef),
                    ref
            ));
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Episode episode = (Episode) content;
        Long episodeId = internal.getId();

        episode.setSeriesNumber(internal.getSeriesNumber());
        episode.setEpisodeNumber(internal.getEpisodeNumber());
        episode.setPartNumber(internal.getPartNumber());
        episode.setSpecial(internal.getSpecial());

        Map<Ref, SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null && !seriesRefs.isEmpty()) {

            if (seriesRefs.size() > 1) {
                log.error("Episode with id {} has more than one series ref", episodeId);
                try {
                    String pdIntegrationKey = System.getenv("PD_INTEGRATION_KEY"); // NPE if missing
                    boolean pdRaiseIncidents = Boolean.parseBoolean(System.getenv(
                            "PD_RAISE_INCIDENTS"));     //disables alerting

                    PagerDutyClientWithKey pagerDutyClient = new PagerDutyClientWithKey(
                            PagerDutyEventsClient.create(),
                            pdIntegrationKey,
                            pdRaiseIncidents
                    );
                    Payload payload = Payload.Builder.newBuilder()
                            .setSummary(
                                    String.format(
                                            "Episode with id %s has multiple series refs; search"
                                                    + "for 'seriesref' in Current Support Concerns",
                                            episodeId
                                    )
                            )
                            .setSource("atlas-deer")
                            .setSeverity(Severity.WARNING)
                            .setTimestamp(OffsetDateTime.now())
                            .build();
                    pagerDutyClient.trigger(payload, String.valueOf(episodeId));
                } catch (Exception e) {
                    log.warn("Triggering PagerDuty incident failed for {}", episodeId, e);
                }
            }

            Map.Entry<Ref, SeriesRef> entry = Iterables.getOnlyElement(seriesRefs.entrySet());

            Ref ref = entry.getKey();
            SeriesRef seriesRef = entry.getValue();

            org.atlasapi.content.SeriesRef deserialized =
                    seriesRefSerialization.deserialize(seriesRef);

            episode.setSeriesRef(new org.atlasapi.content.SeriesRef(
                    Id.valueOf(ref.getId()),
                    Publisher.fromKey(ref.getSource()).requireValue(),
                    deserialized.getTitle(),
                    deserialized.getSeriesNumber(),
                    deserialized.getUpdated(),
                    deserialized.getReleaseYear(),
                    deserialized.getCertificates()
            ));
        }
    }
}