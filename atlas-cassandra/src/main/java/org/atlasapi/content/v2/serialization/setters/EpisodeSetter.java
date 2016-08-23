package org.atlasapi.content.v2.serialization.setters;

import java.util.Set;

import org.atlasapi.content.Episode;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;

import com.google.common.collect.Iterables;
import com.google.common.collect.Sets;

public class EpisodeSetter {

    private final SeriesRefSerialization seriesRef = new SeriesRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Episode.class.isInstance(content)) {
            return;
        }

        Episode episode = (Episode) content;
        internal.setSeriesNumber(episode.getSeriesNumber());
        internal.setEpisodeNumber(episode.getEpisodeNumber());
        internal.setPartNumber(episode.getPartNumber());
        internal.setSpecial(episode.getSpecial());
        SeriesRef ref = seriesRef.serialize(episode.getSeriesRef());
        if (ref != null) {
            internal.setSeriesRefs(Sets.newHashSet(ref));
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Episode episode = (Episode) content;

        episode.setSeriesNumber(internal.getSeriesNumber());
        episode.setEpisodeNumber(internal.getEpisodeNumber());
        episode.setPartNumber(internal.getPartNumber());
        episode.setSpecial(internal.getSpecial());

        Set<SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null && !seriesRefs.isEmpty()) {
            episode.setSeriesRef(seriesRef.deserialize(Iterables.getOnlyElement(seriesRefs)));
        }
    }


}