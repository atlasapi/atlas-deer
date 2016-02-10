package org.atlasapi.equivalence;

import org.atlasapi.content.Episode;

import com.google.common.base.Objects;

public final class SeriesAndEpisodeNumber {

    private int seriesNumber;
    private int episodeNumber;

    public SeriesAndEpisodeNumber(Episode episode) {
        this(episode.getSeriesNumber(), episode.getEpisodeNumber());
    }

    public SeriesAndEpisodeNumber(int seriesNumber, int episodeNumber) {
        this.seriesNumber = seriesNumber;
        this.episodeNumber = episodeNumber;
    }

    @Override
    public int hashCode() {
        return Objects.hashCode(seriesNumber, episodeNumber);
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof SeriesAndEpisodeNumber) {
            SeriesAndEpisodeNumber other = (SeriesAndEpisodeNumber) obj;
            return other.seriesNumber == seriesNumber && other.episodeNumber == episodeNumber;
        }
        return false;
    }

    public int getSeriesNumber() {
        return seriesNumber;
    }

    public int getEpisodeNumber() {
        return episodeNumber;
    }
}
