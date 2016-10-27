package org.atlasapi.content.v2.serialization.setters;

import java.util.Map;

import org.atlasapi.content.Episode;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.serialization.RefSerialization;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Iterables;

public class EpisodeSetter {

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

        episode.setSeriesNumber(internal.getSeriesNumber());
        episode.setEpisodeNumber(internal.getEpisodeNumber());
        episode.setPartNumber(internal.getPartNumber());
        episode.setSpecial(internal.getSpecial());

        Map<Ref, SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null && !seriesRefs.isEmpty()) {
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