package org.atlasapi.content.v2.serialization;

import java.util.Set;
import java.util.stream.Collectors;

import org.atlasapi.content.v2.model.udt.LocalizedTitle;

public class LocalizedTitleSerialization {

    public Set<LocalizedTitle> serialize(Set<org.atlasapi.entity.LocalizedTitle> localizedTitles) {
        return localizedTitles.stream()
                .map(localizedTitle -> serializeLocalizedTitle(localizedTitle))
                .collect(Collectors.toSet());
    }

    public LocalizedTitle serializeLocalizedTitle(
            org.atlasapi.entity.LocalizedTitle oldLocalizedTitle) {
        LocalizedTitle localizedTitle = new LocalizedTitle();
        localizedTitle.setTitle(oldLocalizedTitle.getTitle());
        localizedTitle.setType(oldLocalizedTitle.getType());
        localizedTitle.setLocale(oldLocalizedTitle.getLocale());
        return localizedTitle;
    }

    public Set<org.atlasapi.entity.LocalizedTitle> deserialize(
            Set<LocalizedTitle> localizedTitles) {
        return localizedTitles.stream()
                .map(localizedTitle -> deserializeLocalizedTitle(localizedTitle))
                .collect(Collectors.toSet());
    }

    public org.atlasapi.entity.LocalizedTitle deserializeLocalizedTitle(
            LocalizedTitle localizedTitle) {

        org.atlasapi.entity.LocalizedTitle newLocalizedTitle =
                new org.atlasapi.entity.LocalizedTitle();

        newLocalizedTitle.setLocale(localizedTitle.getLocale());
        newLocalizedTitle.setType(localizedTitle.getType());
        newLocalizedTitle.setTitle(localizedTitle.getTitle());
        return newLocalizedTitle;
    }
}
