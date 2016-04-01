package org.atlasapi.content.v2.serialization;

import org.atlasapi.content.Series;
import org.atlasapi.content.v2.model.Content;

public class SeriesSetter {

    private final BrandRefSerialization brandRef = new BrandRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Series.class.isInstance(content)) {
            return;
        }

        Series series = (Series) content;

        internal.setSeriesNumber(series.getSeriesNumber());
        internal.setTotalEpisodes(series.getTotalEpisodes());
        internal.setBrandRef(brandRef.serialize(series.getBrandRef()));
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Series series = (Series) content;

        series = series.withSeriesNumber(internal.getSeriesNumber());
        series.setTotalEpisodes(internal.getTotalEpisodes());
        series.setBrandRef(brandRef.deserialize(internal.getBrandRef()));
    }
}
