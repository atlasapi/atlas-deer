package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.SeriesRef;

import com.google.common.collect.ImmutableList;

public class BrandSetter {

    private final SeriesRefSerialization seriesRef = new SeriesRefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Brand.class.isInstance(content)) {
            return;
        }

        Brand brand = (Brand) content;

        ImmutableList<org.atlasapi.content.SeriesRef> seriesRefs = brand.getSeriesRefs();
        if (seriesRefs != null) {
            internal.setSeriesRefs(seriesRefs
                    .stream()
                    .map(seriesRef::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Brand brand = (Brand) content;

        Set<SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null) {
            brand.setSeriesRefs(seriesRefs
                    .stream()
                    .map(seriesRef::deserialize)
                    .collect(Collectors.toList()));
        }
    }
}