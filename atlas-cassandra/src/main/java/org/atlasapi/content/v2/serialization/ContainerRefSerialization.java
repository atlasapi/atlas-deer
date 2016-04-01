package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.stream.Collectors;

import org.atlasapi.content.BrandRef;
import org.atlasapi.content.ContentType;
import org.atlasapi.content.SeriesRef;
import org.atlasapi.content.v2.model.udt.ContainerRef;
import org.atlasapi.entity.Id;
import org.atlasapi.media.entity.Publisher;

import org.joda.time.DateTime;

import static org.atlasapi.content.v2.serialization.DateTimeUtils.toDateTime;

public class ContainerRefSerialization {

    private final CertificateSerialization certificate = new CertificateSerialization();

    public ContainerRef serialize(org.atlasapi.content.ContainerRef containerRef) {
        if (containerRef == null) {
            return null;
        }
        ContainerRef internalContainerRef =
                new ContainerRef();
        Id id = containerRef.getId();
        if (id != null) {
            internalContainerRef.setId(id.longValue());
        }
        Publisher source = containerRef.getSource();
        if (source != null) {
            internalContainerRef.setSource(source.key());
        }
        ContentType contentType = containerRef.getContentType();
        internalContainerRef.setType(contentType.name());
        switch (contentType) {
            case BRAND:
                break;
            case SERIES:
                SeriesRef seriesRef = (SeriesRef) containerRef;
                internalContainerRef.setTitle(seriesRef.getTitle());
                DateTime updated = seriesRef.getUpdated();
                if (updated != null) {
                    internalContainerRef.setUpdated(updated.toInstant());
                }
                internalContainerRef.setSeriesNumber(seriesRef.getSeriesNumber());
                internalContainerRef.setReleaseYear(seriesRef.getReleaseYear());
                internalContainerRef.setCertificates(seriesRef.getCertificates()
                        .stream()
                        .map(certificate::serialize)
                        .filter(Objects::nonNull)
                        .collect(Collectors.toSet()));
                break;
            default:
                throw new IllegalArgumentException(String.format(
                        "%s can't be a container",
                        containerRef
                ));
        }
        return internalContainerRef;
    }

    public org.atlasapi.content.ContainerRef deserialize(ContainerRef icr) {
        if (icr == null) {
            return null;
        }

        ContentType type = ContentType.valueOf(icr.getType());
        switch (type) {
            case SERIES:
                return new SeriesRef(
                        Id.valueOf(icr.getId()),
                        Publisher.fromKey(icr.getSource()).requireValue(),
                        icr.getTitle(),
                        icr.getSeriesNumber(),
                        toDateTime(icr.getUpdated()),
                        icr.getReleaseYear(),
                        icr.getCertificates().stream()
                                .map(certificate::deserialize)
                                .collect(Collectors.toList())
                );
            case BRAND:
                return new BrandRef(
                        Id.valueOf(icr.getId()),
                        Publisher.fromKey(icr.getSource()).requireValue()
                );
            default:
                throw new IllegalArgumentException(String.format(
                        "Illegal container ref type: %s",
                        icr.getType()
                ));
        }
    }

}