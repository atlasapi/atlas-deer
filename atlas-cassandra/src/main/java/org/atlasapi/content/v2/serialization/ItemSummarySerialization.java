package org.atlasapi.content.v2.serialization;

import java.util.Objects;
import java.util.Optional;
import java.util.stream.Collectors;

import org.atlasapi.content.Certificate;
import org.atlasapi.content.v2.model.udt.ItemSummary;

import com.google.common.collect.ImmutableSet;

public class ItemSummarySerialization {

    private final CertificateSerialization certificate = new CertificateSerialization();
    private final ItemRefSerialization itemRef = new ItemRefSerialization();

    public ItemSummary serialize(org.atlasapi.content.ItemSummary itemSummary) {
        ItemSummary internal =
                new ItemSummary();

        internal.setRef(itemRef.serialize(itemSummary.getItemRef()));
        internal.setTitle(itemSummary.getTitle());

        Optional<String> description = itemSummary.getDescription();
        if (description.isPresent()) {
            internal.setDescription(description.get());
        }

        Optional<String> image = itemSummary.getImage();
        if (image.isPresent()) {
            internal.setImage(image.get());
        }

        Optional<Integer> releaseYear = itemSummary.getReleaseYear();
        if (releaseYear.isPresent()) {
            internal.setReleaseYear(releaseYear.get());
        }

        Optional<ImmutableSet<Certificate>> certificates = itemSummary.getCertificates();
        if (certificates.isPresent()) {
            internal.setCertificate(certificates.get()
                    .stream()
                    .map(certificate::serialize)
                    .filter(Objects::nonNull)
                    .collect(Collectors.toSet()));
        }

        return internal;
    }

    public org.atlasapi.content.ItemSummary deserialize(ItemSummary is) {
        if (is == null) {
            return null;
        }
        return new org.atlasapi.content.ItemSummary(
                itemRef.deserialize(is.getRef()),
                is.getTitle(),
                is.getDescription(),
                is.getImage(),
                is.getReleaseYear(),
                is.getCertificate()
                        .stream()
                        .map(certificate::deserialize)
                        .collect(Collectors.toList())
        );
    }
}