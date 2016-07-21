/* Copyright 2009 British Broadcasting Corporation
   Copyright 2009 Meta Broadcast Ltd

Licensed under the Apache License, Version 2.0 (the "License"); you
may not use this file except in compliance with the License. You may
obtain a copy of the License at

http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or
implied. See the License for the specific language governing
permissions and limitations under the License. */

package org.atlasapi.content;

import javax.annotation.Nonnull;
import javax.annotation.Nullable;

import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;
import org.atlasapi.meta.annotations.FieldName;

/**
 * @author Robert Chatley (robert@metabroadcast.com)
 * @author Lee Denison (lee@metabroadcast.com)
 */
public class Episode extends Item {

    private Integer seriesNumber;
    private Integer episodeNumber;
    private Integer partNumber;
    private Boolean special = null;

    private SeriesRef seriesRef;

    public Episode(String uri, String curie, Publisher publisher) {
        super(uri, curie, publisher);
    }

    public Episode(Id id, Publisher source) {
        super(id, source);
    }

    public Episode() {
    }

    @FieldName("part_number")
    public Integer getPartNumber() {
        return this.partNumber;
    }

    @Nullable
    @FieldName("episode_number")
    public Integer getEpisodeNumber() {
        return episodeNumber;
    }

    @FieldName("series_number")
    public Integer getSeriesNumber() {
        return seriesNumber;
    }

    @FieldName("special")
    public Boolean getSpecial() {
        return special;
    }

    public void setSpecial(Boolean special) {
        this.special = special;
    }

    public void setPartNumber(Integer partNumber) {
        this.partNumber = partNumber;
    }

    public void setEpisodeNumber(Integer position) {
        this.episodeNumber = position;
    }

    public void setSeriesNumber(Integer position) {
        this.seriesNumber = position;
    }

    public void setSeriesRef(@Nullable SeriesRef seriesRef) {
        this.seriesRef = seriesRef;
    }

    public void setSeries(@Nonnull Series series) {
        setSeriesRef(series.toRef());
    }

    @FieldName("series_ref")
    public
    @Nullable
    SeriesRef getSeriesRef() {
        return seriesRef;
    }

    @Override
    public EpisodeRef toRef() {
        return new EpisodeRef(
                getId(),
                getSource(),
                SortKey.keyFrom(this),
                getThisOrChildLastUpdated()
        );
    }

    @Override
    public Episode copy() {
        return copyTo(this, new Episode());
    }

    public static Episode copyTo(Episode from, Episode to) {
        Item.copyTo(from, to);
        to.episodeNumber = from.episodeNumber;
        to.seriesNumber = from.seriesNumber;
        to.seriesRef = from.seriesRef;
        to.special = from.special;
        return to;
    }

    @Override
    public ItemSummary toSummary() {
        return new EpisodeSummary(
                toRef(),
                getTitle(),
                getDescription(),
                getImage(),
                getEpisodeNumber(),
                getYear(),
                getCertificates()
        );
    }

    public <V> V accept(ItemVisitor<V> visitor) {
        return visitor.visit(this);
    }

}
