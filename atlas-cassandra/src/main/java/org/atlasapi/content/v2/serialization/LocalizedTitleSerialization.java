package org.atlasapi.content.v2.serialization;

import java.util.Locale;

import org.atlasapi.content.v2.model.udt.LocalizedTitle;

public class LocalizedTitleSerialization {

    public LocalizedTitle serialize(org.atlasapi.content.LocalizedTitle lt) {

        if (lt == null) {
            return null;
        }
        LocalizedTitle localizedTitle = new LocalizedTitle();

        localizedTitle.setTitle(lt.getTitle());
        localizedTitle.setLocale(lt.getLanguageTag());

        return localizedTitle;
    }

    public org.atlasapi.content.LocalizedTitle deserialize(LocalizedTitle lt) {

        if (lt == null) {
            return null;
        }

        org.atlasapi.content.LocalizedTitle localizedTitle = new org.atlasapi.content.LocalizedTitle();
        localizedTitle.setTitle(lt.getTitle());
        localizedTitle.setLocale(Locale.forLanguageTag(lt.getLocale()));

        return localizedTitle;
    }
}
