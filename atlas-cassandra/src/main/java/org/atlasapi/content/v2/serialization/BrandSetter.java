package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.v2.model.Content;

public class BrandSetter {

    private final SeriesRefSerialization seriesRef = new SeriesRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Brand.class.isInstance(content)) {
            return;
        }

        Brand brand = (Brand) content;
        internal.setSeriesRefs(brand.getSeriesRefs()
                .stream()
                .map(seriesRef::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toList()));
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Brand brand = (Brand) content;

        brand.setSeriesRefs(internal.getSeriesRefs()
                .stream()
                .map(seriesRef::deserialize)
                .collect(Collectors.toList()));
    }
}