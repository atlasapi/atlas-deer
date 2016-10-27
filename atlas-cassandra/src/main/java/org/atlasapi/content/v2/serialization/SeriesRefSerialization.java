package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.stream.Collectors;

import javax.annotation.Nullable;

import org.atlasapi.content.v2.model.udt.Ref;
import org.atlasapi.content.v2.model.udt.SeriesRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;
import static org.atlasapi.content.v2.serialization.DateTimeUtils.toInstant;

public class SeriesRefSerialization {

    private final CertificateSerialization certificate = new CertificateSerialization();

    @Nullable
    public SeriesRef serialize(@Nullable org.atlasapi.content.SeriesRef seriesRef) {
        if (seriesRef == null) {
            return null;
        }
        SeriesRef internal = new SeriesRef();

        Ref ref = new Ref();
        Id id = seriesRef.getId();
        if (id != null) {
            ref.setId(id.longValue());
        }

        Publisher source = seriesRef.getSource();
        if (source != null) {
            ref.setSource(source.key());
        }

        internal.setTitle(seriesRef.getTitle());
        internal.setUpdated(toInstant(seriesRef.getUpdated()));
        internal.setSeriesNumber(seriesRef.getSeriesNumber());
        internal.setReleaseYear(seriesRef.getReleaseYear());
        internal.setCertificates(seriesRef.getCertificates()
                .stream()
                .map(certificate::serialize)
                .filter(Objects::nonNull)
                .collect(Collectors.toSet()));

        return internal;
    }

    public org.atlasapi.content.SeriesRef deserialize(SeriesRef ref) {
        return new org.atlasapi.content.SeriesRef(
                Id.valueOf(-1),
                Publisher.METABROADCAST,
                ref.getTitle(),
                ref.getSeriesNumber(),
                toDateTime(ref.getUpdated()),
                ref.getReleaseYear(),
                ref.getCertificates()
                        .stream()
                        .map(certificate::deserialize)
                        .collect(Collectors.toList())
        );
    }

}