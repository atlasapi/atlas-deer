package org.atlasapi.equivalence;

import java.util.Comparator;

import org.atlasapi.content.Episode;

public class SeriesOrder implements Comparator<Episode> {

    @Override
    public int compare(Episode item1, Episode item2) {
        if (item1.getSeriesNumber() == null && item2.getSeriesNumber() == null) {
            return defaultOrder(item1, item2);
        }
        if (item1.getSeriesNumber() == null) {
            return 1;
        }
        if (item2.getSeriesNumber() == null) {
            return -1;
        }
        int seriesComparison = item1.getSeriesNumber().compareTo(item2.getSeriesNumber());
        if (seriesComparison != 0) {
            return seriesComparison;
        }
        return compareEpisodeNumbers(item1, item2);

    }

    private int compareEpisodeNumbers(Episode item1, Episode item2) {
        if (item1.getEpisodeNumber() == null && item2.getEpisodeNumber() == null) {
            return defaultOrder(item1, item2);
        }
        if (item1.getEpisodeNumber() == null) {
            return 1;
        }
        if (item2.getEpisodeNumber() == null) {
            return -1;
        }
        int episodeComparison = item1.getEpisodeNumber().compareTo(item2.getEpisodeNumber());
        if (episodeComparison != 0) {
            return episodeComparison;
        }
        return defaultOrder(item1, item2);
    }

    private int defaultOrder(Episode item1, Episode item2) {
        return item1.getCanonicalUri().compareTo(item2.getCanonicalUri());
    }
}
