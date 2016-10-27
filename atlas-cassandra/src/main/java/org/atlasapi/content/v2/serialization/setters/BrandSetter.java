package org.atlasapi.content.v2.serialization.setters;

import java.util.Map;
import java.util.stream.Collectors;

import org.atlasapi.content.Brand;
import org.atlasapi.content.v2.model.Content;
import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.content.v2.serialization.RefSerialization;
import org.atlasapi.content.v2.serialization.SeriesRefSerialization;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import com.metabroadcast.common.stream.MoreCollectors;

import com.google.common.collect.ImmutableList;

public class BrandSetter {

    private final SeriesRefSerialization seriesRefSerialization = new SeriesRefSerialization();
    private final RefSerialization refSerialization = new RefSerialization();

    public void serialize(Content internal, org.atlasapi.content.Content content) {
        if (!Brand.class.isInstance(content)) {
            return;
        }

        Brand brand = (Brand) content;

        ImmutableList<org.atlasapi.content.SeriesRef> seriesRefs = brand.getSeriesRefs();
        if (seriesRefs != null) {
            internal.setSeriesRefs(seriesRefs.stream()
                    .collect(MoreCollectors.toImmutableMap(
                            refSerialization::serialize,
                            seriesRefSerialization::serialize
                    )));
        }
    }

    public void deserialize(org.atlasapi.content.Content content, Content internal) {
        Brand brand = (Brand) content;

        Map<Ref, SeriesRef> seriesRefs = internal.getSeriesRefs();
        if (seriesRefs != null) {
            brand.setSeriesRefs(seriesRefs.entrySet().stream().map(entry -> {
                Ref ref = entry.getKey();
                SeriesRef seriesRef = entry.getValue();

                org.atlasapi.content.SeriesRef deserialized =
                        seriesRefSerialization.deserialize(seriesRef);

                return new org.atlasapi.content.SeriesRef(
                        Id.valueOf(ref.getId()),
                        Publisher.fromKey(ref.getSource()).requireValue(),
                        deserialized.getTitle(),
                        deserialized.getSeriesNumber(),
                        deserialized.getUpdated(),
                        deserialized.getReleaseYear(),
                        deserialized.getCertificates()
                );
            }).collect(Collectors.toList()));
        }
    }
}